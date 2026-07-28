"""Daemon lifecycle + observability for the analyzed bench.

- Starts `analyzed daemon --foreground` under a short TMPDIR (macOS SUN_LEN:
  the default socket path under the long per-user TMPDIR exceeds sun_path).
- Samples RSS of the daemon and its children (proc-macro server etc.) plus
  `analyzed status` counters on background threads.
- On request stalls, captures a stack sample of the daemon with /usr/bin/sample
  (deduplicated, capped) so stalls can be correlated with the glue hot path.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time


class DaemonController:
    def __init__(
        self,
        binary: str,
        tmpdir: str,
        out_dir: str,
        max_samples: int = 40,
        nice_level: int = 10,
        watchdog_gib: float | None = None,
        watchdog_cb=None,
    ):
        self.binary = binary
        self.tmpdir = tmpdir
        self.out_dir = out_dir
        self.samples_dir = os.path.join(out_dir, "samples")
        self.max_samples = max_samples
        self.nice_level = nice_level
        self.watchdog_kb = watchdog_gib * 1024 * 1024 if watchdog_gib else None
        self.watchdog_cb = watchdog_cb
        self.watchdog_tripped = False
        self.env = dict(os.environ, TMPDIR=tmpdir)
        self.proc = None
        self.pid = None
        self.rss_timeline: list[dict] = []  # {t, rss_kb, children_rss_kb, nproc}
        self.status_timeline: list[dict] = []  # {t, client_sessions, workspaces, backends}
        self.sample_log: list[dict] = []
        self._sample_lock = threading.Lock()
        self._sample_running = False
        self._last_sample = 0.0
        self._stop_sampling = threading.Event()
        self._threads = []
        os.makedirs(self.samples_dir, exist_ok=True)
        os.makedirs(tmpdir, exist_ok=True)
        os.chmod(tmpdir, 0o700)

    # ---- lifecycle ----

    def _cli(self, *args, timeout=30):
        return subprocess.run(
            [self.binary, *args], env=self.env, capture_output=True, text=True, timeout=timeout
        )

    def status(self):
        try:
            out = self._cli("status")
            return json.loads(out.stdout)
        except Exception as e:
            return {"running": False, "connection_error": f"harness: {e}"}

    def fresh_start(self, log_path: str, timeout: float = 30.0):
        """Stop any leftover daemon on this TMPDIR, then start a fresh one."""
        st = self.status()
        if st.get("running"):
            self._cli("stop")
            time.sleep(1.0)
        for pid in self._find_bench_pids():
            try:
                os.kill(pid, 9)
            except OSError:
                pass
        sock_dir = os.path.join(self.tmpdir, "analyzed")
        shutil.rmtree(sock_dir, ignore_errors=True)

        self.rss_timeline = []
        self.status_timeline = []
        self.watchdog_tripped = False
        self._log_file = open(log_path, "wb")
        nice_level = self.nice_level
        self.proc = subprocess.Popen(
            [self.binary, "daemon", "--foreground"],
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=self._log_file,
            stderr=self._log_file,
            preexec_fn=(lambda: os.nice(nice_level)) if nice_level else None,
        )
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            st = self.status()
            if st.get("running"):
                self.pid = st.get("pid") or self.proc.pid
                self._start_sampling()
                return st
            if self.proc.poll() is not None:
                raise RuntimeError(f"daemon exited on startup: rc={self.proc.returncode}")
            time.sleep(0.2)
        raise RuntimeError("daemon did not come up in time")

    def stop(self, wait_s: float = 15.0) -> dict:
        """Stop the daemon, wait for exit, report leftovers."""
        self._stop_sampling.set()
        for t in self._threads:
            t.join(timeout=3)
        pre_stop_children = (
            [pid for pid, _, _ in self._descendants(self._ps_snapshot(), self.pid)]
            if self.pid
            else []
        )
        result = {"stop_ok": False, "exited": False, "orphans": []}
        try:
            self._cli("stop")
            result["stop_ok"] = True
        except Exception as e:
            result["stop_error"] = str(e)
        t0 = time.monotonic()
        while time.monotonic() - t0 < wait_s:
            if self.proc is None or self.proc.poll() is not None:
                result["exited"] = True
                break
            time.sleep(0.3)
        if not result["exited"] and self.proc is not None:
            self.proc.kill()
            result["killed"] = True
        try:
            self._log_file.close()
        except Exception:
            pass
        time.sleep(1.0)
        result["orphans"] = self.orphan_scan()
        alive = {pid for pid, _, _, _ in self._ps_snapshot()}
        result["orphaned_children"] = [p for p in pre_stop_children if p in alive]
        return result

    # ---- process observation ----

    def _ps_snapshot(self):
        """Return list of (pid, ppid, rss_kb, command) for all processes."""
        out = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,rss=,command="], capture_output=True, text=True
        )
        rows = []
        for line in out.stdout.splitlines():
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            try:
                rows.append((int(parts[0]), int(parts[1]), int(parts[2]), parts[3]))
            except ValueError:
                continue
        return rows

    def _descendants(self, rows, root_pid):
        children = {}
        for pid, ppid, rss, cmd in rows:
            children.setdefault(ppid, []).append((pid, rss, cmd))
        out = []
        stack = [root_pid]
        while stack:
            cur = stack.pop()
            for pid, rss, cmd in children.get(cur, []):
                out.append((pid, rss, cmd))
                stack.append(pid)
        return out

    def _find_bench_pids(self):
        """PIDs of analyzed processes tied to our bench TMPDIR or binary."""
        pids = []
        for pid, ppid, rss, cmd in self._ps_snapshot():
            if self.binary in cmd and pid != os.getpid():
                pids.append(pid)
        return pids

    def orphan_scan(self):
        """analyzed / proc-macro / metadata processes still alive after teardown."""
        orphans = []
        for pid, ppid, rss, cmd in self._ps_snapshot():
            if self.binary in cmd or "analyzed" in cmd.split(" ")[0]:
                orphans.append({"pid": pid, "ppid": ppid, "rss_kb": rss, "command": cmd[:200]})
        return orphans

    def _start_sampling(self):
        self._stop_sampling.clear()
        t1 = threading.Thread(target=self._rss_loop, daemon=True, name="rss-sampler")
        t2 = threading.Thread(target=self._status_loop, daemon=True, name="status-sampler")
        t1.start()
        t2.start()
        self._threads = [t1, t2]

    def _rss_loop(self):
        while not self._stop_sampling.is_set():
            rows = self._ps_snapshot()
            rss = 0
            for pid, ppid, r, cmd in rows:
                if pid == self.pid:
                    rss = r
                    break
            kids = self._descendants(rows, self.pid)
            self.rss_timeline.append(
                {
                    "t": time.time(),
                    "rss_kb": rss,
                    "children_rss_kb": sum(k[1] for k in kids),
                    "nchildren": len(kids),
                }
            )
            if self.watchdog_kb and rss > self.watchdog_kb and not self.watchdog_tripped:
                self.watchdog_tripped = True
                if self.watchdog_cb:
                    try:
                        self.watchdog_cb(rss)
                    except Exception:
                        pass
            self._stop_sampling.wait(0.5)

    def _status_loop(self):
        while not self._stop_sampling.is_set():
            st = self.status()
            self.status_timeline.append(
                {
                    "t": time.time(),
                    "client_sessions": st.get("client_sessions"),
                    "workspaces": st.get("workspaces"),
                    "backends": len(st.get("backend_sessions") or []),
                }
            )
            self._stop_sampling.wait(2.0)

    # ---- stall stack sampling ----

    def stall_sample(self, tag: str):
        """Capture one 2s stack sample of the daemon; dedupe concurrent/rapid calls."""
        with self._sample_lock:
            if self._sample_running:
                return None
            if time.monotonic() - self._last_sample < 5.0:
                return None
            if len(self.sample_log) >= self.max_samples:
                return None
            self._sample_running = True
            self._last_sample = time.monotonic()
        fname = os.path.join(
            self.samples_dir, f"sample_{len(self.sample_log):03d}_{int(time.time())}_{tag[:40]}.txt"
        )
        entry = {"t": time.time(), "tag": tag, "file": os.path.basename(fname)}
        self.sample_log.append(entry)

        def run():
            try:
                subprocess.run(
                    ["/usr/bin/sample", str(self.pid), "2", "-file", fname],
                    capture_output=True,
                    timeout=30,
                )
            except Exception:
                pass
            finally:
                with self._sample_lock:
                    self._sample_running = False

        threading.Thread(target=run, daemon=True, name="stall-sample").start()
        return fname

    def rss_now_kb(self):
        rows = self._ps_snapshot()
        for pid, ppid, r, cmd in rows:
            if pid == self.pid:
                return r
        return 0
