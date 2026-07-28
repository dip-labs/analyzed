"""Minimal LSP stdio client for benchmarking analyzed sessions.

One LspSession = one `analyzed` process bridging stdio to the shared daemon.
All requests are timed; a stall callback fires while a request is still
outstanding past the stall threshold (used to trigger a daemon stack sample).
Timed-out requests are recorded and never retried in a loop.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from collections import deque


def uri_for(path: str) -> str:
    return "file://" + path


class Pending:
    __slots__ = ("id", "method", "t_start", "event", "result", "error", "t_done")

    def __init__(self, req_id: int, method: str):
        self.id = req_id
        self.method = method
        self.t_start = time.monotonic()
        self.event = threading.Event()
        self.result = None
        self.error = None
        self.t_done = None


class LspSession:
    def __init__(
        self,
        binary: str,
        root: str,
        env: dict,
        name: str,
        stderr_path: str,
        config: dict | None = None,
        stall_cb=None,
        stall_threshold: float = 1.0,
    ):
        self.name = name
        self.root = root
        self.config = config or {}
        self.stall_cb = stall_cb
        self.stall_threshold = stall_threshold
        self._id = 0
        self._pending: dict[int, Pending] = {}
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self.notifications = deque(maxlen=4096)
        self.request_log: list[dict] = []  # every request: method, latency, ok
        self.stall_events: list[dict] = []
        self._stalled_reported: set[int] = set()
        self._closed = False

        self._stderr_file = open(stderr_path, "wb")
        self.t_spawn = time.monotonic()
        self.proc = subprocess.Popen(
            [binary],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._stderr_file,
            cwd=root,
            env=env,
        )
        self._reader = threading.Thread(target=self._read_loop, daemon=True, name=f"lsp-read-{name}")
        self._reader.start()
        self._watchdog = threading.Thread(target=self._watch_loop, daemon=True, name=f"lsp-watch-{name}")
        self._watchdog.start()

    # ---- wire ----

    def _write_msg(self, payload: dict):
        data = json.dumps(payload).encode()
        with self._write_lock:
            self.proc.stdin.write(b"Content-Length: %d\r\n\r\n" % len(data))
            self.proc.stdin.write(data)
            self.proc.stdin.flush()

    def _read_msg(self):
        headers = {}
        line = b""
        while True:
            line = self.proc.stdout.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                break
            key, _, value = line.partition(b":")
            headers[key.strip().lower()] = value.strip()
        length = int(headers.get(b"content-length", b"0"))
        body = self.proc.stdout.read(length)
        if len(body) < length:
            return None
        return json.loads(body)

    def _read_loop(self):
        while True:
            try:
                msg = self._read_msg()
            except Exception:
                return
            if msg is None:
                return
            if "id" in msg and "method" in msg:
                self._handle_server_request(msg)
            elif "id" in msg:
                self._handle_response(msg)
            else:
                self.notifications.append((time.monotonic(), msg.get("method"), msg.get("params")))

    def _handle_response(self, msg):
        with self._lock:
            pending = self._pending.pop(msg["id"], None)
        if pending is None:
            return
        pending.t_done = time.monotonic()
        pending.result = msg.get("result")
        pending.error = msg.get("error")
        pending.event.set()

    def _handle_server_request(self, msg):
        method = msg["method"]
        result = None
        if method == "workspace/configuration":
            items = msg.get("params", {}).get("items", [])
            result = [self._config_for(item.get("section")) for item in items]
        elif method == "workspace/applyEdit":
            result = {"applied": False}
        elif method == "window/showMessageRequest":
            result = None
        # window/workDoneProgress/create, client/registerCapability -> null
        self._write_msg({"jsonrpc": "2.0", "id": msg["id"], "result": result})

    def _config_for(self, section):
        if not section:
            return self.config
        node = self.config
        for part in section.split("."):
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        return node

    def _watch_loop(self):
        while not self._closed:
            now = time.monotonic()
            with self._lock:
                items = list(self._pending.values())
            for p in items:
                if p.id in self._stalled_reported:
                    continue
                waited = now - p.t_start
                if waited >= self.stall_threshold:
                    self._stalled_reported.add(p.id)
                    event = {
                        "t": time.time(),
                        "session": self.name,
                        "method": p.method,
                        "waited_at_report_s": round(waited, 3),
                    }
                    self.stall_events.append(event)
                    if self.stall_cb:
                        try:
                            self.stall_cb(self, p, event)
                        except Exception:
                            pass
            time.sleep(0.2)

    # ---- protocol ----

    def request(self, method: str, params, timeout: float = 120.0):
        with self._lock:
            self._id += 1
            req_id = self._id
            pending = Pending(req_id, method)
            self._pending[req_id] = pending
        self._write_msg({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
        ok = pending.event.wait(timeout)
        latency = (pending.t_done or time.monotonic()) - pending.t_start
        entry = {
            "method": method,
            "latency_s": round(latency, 4),
            "timed_out": not ok,
            "error": pending.error.get("message") if isinstance(pending.error, dict) else None,
        }
        self.request_log.append(entry)
        if not ok:
            with self._lock:
                self._pending.pop(req_id, None)
            return None, entry
        return pending.result, entry

    def notify(self, method: str, params):
        self._write_msg({"jsonrpc": "2.0", "method": method, "params": params})

    def initialize(self, root: str, timeout: float = 60.0):
        params = {
            "processId": os.getpid(),
            "rootUri": uri_for(root),
            "workspaceFolders": [{"uri": uri_for(root), "name": "bench"}],
            "capabilities": {
                "workspace": {
                    "symbol": {"resolveSupport": {"properties": []}},
                    "configuration": True,
                    "didChangeWatchedFiles": {"dynamicRegistration": True},
                    "workspaceEdit": {"documentChanges": True},
                },
                "textDocument": {
                    "synchronization": {"didSave": True},
                    "definition": {"linkSupport": False},
                    "references": {},
                    "documentSymbol": {"hierarchicalDocumentSymbolSupport": True},
                    "rename": {"prepareSupport": False},
                    "publishDiagnostics": {},
                },
                "window": {"workDoneProgress": True},
            },
            "initializationOptions": self.config,
        }
        result, entry = self.request("initialize", params, timeout=timeout)
        if result is not None:
            self.notify("initialized", {})
        return result, entry

    def did_open(self, path: str, text: str, version: int = 1):
        self.notify(
            "textDocument/didOpen",
            {
                "textDocument": {
                    "uri": uri_for(path),
                    "languageId": "rust" if path.endswith(".rs") else "toml",
                    "version": version,
                    "text": text,
                }
            },
        )

    def did_change(self, path: str, text: str, version: int):
        self.notify(
            "textDocument/didChange",
            {
                "textDocument": {"uri": uri_for(path), "version": version},
                "contentChanges": [{"text": text}],
            },
        )

    def did_close(self, path: str):
        self.notify("textDocument/didClose", {"textDocument": {"uri": uri_for(path)}})

    # ---- queries ----

    def document_symbol(self, path: str, timeout: float = 120.0):
        return self.request(
            "textDocument/documentSymbol", {"textDocument": {"uri": uri_for(path)}}, timeout
        )

    def definition(self, path: str, line: int, char: int, timeout: float = 120.0):
        return self.request(
            "textDocument/definition",
            {"textDocument": {"uri": uri_for(path)}, "position": {"line": line, "character": char}},
            timeout,
        )

    def references(self, path: str, line: int, char: int, timeout: float = 120.0):
        return self.request(
            "textDocument/references",
            {
                "textDocument": {"uri": uri_for(path)},
                "position": {"line": line, "character": char},
                "context": {"includeDeclaration": True},
            },
            timeout,
        )

    def rename(self, path: str, line: int, char: int, new_name: str, timeout: float = 120.0):
        return self.request(
            "textDocument/rename",
            {
                "textDocument": {"uri": uri_for(path)},
                "position": {"line": line, "character": char},
                "newName": new_name,
            },
            timeout,
        )

    def workspace_symbol(self, query: str, timeout: float = 300.0):
        return self.request("workspace/symbol", {"query": query}, timeout)

    # ---- teardown ----

    def shutdown(self, timeout: float = 10.0) -> bool:
        """Graceful shutdown; returns True if the process exited cleanly."""
        self._closed = True
        try:
            self.request("shutdown", None, timeout=timeout)
            self.notify("exit", None)
        except Exception:
            pass
        try:
            self.proc.wait(timeout=5)
            clean = True
        except subprocess.TimeoutExpired:
            self.proc.kill()
            clean = False
        try:
            self._stderr_file.close()
        except Exception:
            pass
        return clean

    def kill(self):
        self._closed = True
        try:
            self.proc.kill()
        except Exception:
            pass
        try:
            self._stderr_file.close()
        except Exception:
            pass
