#!/usr/bin/env python3
"""Mass test: an agent-fleet failure shape that killed half a worker batch.

  python3 bench/masstest.py --repo /path/to/large-rust-repo [--workers 15]
      [--duration 600] [--churn-at 240] [--churn-kill 3] [--target-file REL]

N concurrent sessions at WORKTREE ROOTS of one large Rust crate (a common
headless-agent pattern), sustained semantic ops on one hot file, uncommitted
didChange edits mixed in, and a mid-run churn wave (hard SIGKILL of several
workers + replacements).

The session-aware watchdog policy runs in-loop (24 GiB cap: wait while
sessions live; 100 GiB hard cap: rotate regardless) so rotation behavior
under load is part of the test, not an external accident.

Hang taxonomy recorded per op:
  ok        answered within budget
  typed_err answered with an LSP error (acceptable: enforcer can react)
  hang      no answer within --op-timeout (the class that must reach zero)
Plus events: silent bridge deaths (exit without a client-visible error),
watchdog decisions, churn kills/replacements, daemon restarts.
"""

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness.config import RA_CONFIG_V2  # noqa: E402
from harness.daemon import DaemonController  # noqa: E402
from harness.diffs import WorktreeFactory  # noqa: E402
from harness.lsp import LspSession  # noqa: E402
from harness.replay import fn_targets  # noqa: E402

def pick_target(repo):
    """Largest .rs file under src/ — a busy, realistic query target."""
    best, size = None, 0
    for dirpath, dirnames, filenames in os.walk(os.path.join(repo, "src")):
        dirnames[:] = [d for d in dirnames if d not in ("target", ".git")]
        for fn in filenames:
            if fn.endswith(".rs"):
                full = os.path.join(dirpath, fn)
                n = os.path.getsize(full)
                if n > size:
                    best, size = os.path.relpath(full, repo), n
    return best


class MassTest:
    def __init__(self, args):
        self.args = args
        self.repo = os.path.realpath(args.repo)
        self.out_dir = args.out
        self.logs = os.path.join(self.out_dir, "logs")
        os.makedirs(self.logs, exist_ok=True)
        self.ops = []
        self.events = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.daemon = DaemonController(
            args.binary, args.tmpdir, self.out_dir, nice_level=10,
            watchdog_gib=None,  # policy handled by our own thread below
        )
        self.wt = WorktreeFactory(self.repo, args.worktrees_root, "HEAD")
        self.target_rel = args.target_file or pick_target(self.repo)
        self.workers = {}  # wid -> dict(session, thread, worktree, alive)

    def event(self, kind, detail):
        with self._lock:
            self.events.append({"t": time.time(), "kind": kind, "detail": str(detail)[:200]})
        print(f"  [{kind}] {str(detail)[:120]}", flush=True)

    def op(self, wid, kind, latency, verdict, err=None):
        with self._lock:
            self.ops.append(
                {"t": time.time(), "w": wid, "kind": kind,
                 "latency_s": round(latency, 3), "verdict": verdict,
                 **({"err": str(err)[:120]} if err else {})}
            )

    # ---- worktrees: per-worker committed diffs on the hot file ----

    def make_worktrees(self, n):
        paths = []
        base = open(os.path.join(self.repo, self.target_rel), encoding="utf-8", errors="replace").read()
        for i in range(n):
            text = base + (
                f"\n// masstest worker {i}\n"
                f"pub fn mass_probe_{i}(x: u64) -> u64 {{ x.wrapping_mul({i + 2}) }}\n"
                f"pub fn mass_call_{i}() -> u64 {{ mass_probe_{i}(7) }}\n"
            )
            path = self.wt.create(f"masstest-w{i:02d}", {self.target_rel: text}, f"masstest: worker {i}")
            paths.append(path)
        return paths

    # ---- worker ----

    def _worker(self, wid, root, deadline):
        rnd = random.Random(4000 + wid)
        name = f"w{wid:02d}"
        target = os.path.join(root, self.target_rel)
        text = open(target, encoding="utf-8", errors="replace").read()
        targets = fn_targets(text)
        # a real mid-file fn + the worker's own committed fn
        mid = targets[len(targets) // 2]
        own = next(((n, l, c) for (n, l, c) in targets if n == f"mass_probe_{wid % 50}"), targets[-1])
        sess = None
        try:
            sess = LspSession(
                self.args.binary, root, self.daemon.env, name,
                stderr_path=os.path.join(self.logs, f"{name}.log"), config=RA_CONFIG_V2,
                stall_cb=lambda s, p, e: self.daemon.stall_sample(f"{s.name}-{p.method.split('/')[-1]}"),
            )
            with self._lock:
                self.workers[wid] = {"session": sess, "root": root, "alive": True}
            t0 = time.monotonic()
            init, e = sess.initialize(root, timeout=self.args.op_timeout)
            self.op(wid, "initialize", e["latency_s"], "ok" if init else ("hang" if e["timed_out"] else "typed_err"), e.get("error"))
            if init is None:
                self.event("worker_init_failed", name)
                return
            t0 = time.monotonic()
            r, e = sess.document_symbol(target, timeout=self.args.op_timeout)
            verdict = "ok" if r is not None else ("hang" if e["timed_out"] else "typed_err")
            self.op(wid, "ready", time.monotonic() - t0, verdict, e.get("error"))
            tick = 0
            edited_text = text
            while time.monotonic() < deadline and not self._stop.is_set():
                if sess.proc.poll() is not None:
                    self.event("bridge_died", f"{name} rc={sess.proc.returncode}")
                    with self._lock:
                        self.workers[wid]["alive"] = False
                    return
                kind = rnd.choice(["docsym", "definition", "references", "edit_def", "definition", "docsym"])
                t0 = time.monotonic()
                if kind == "docsym":
                    r, e = sess.document_symbol(target, timeout=self.args.op_timeout)
                elif kind == "definition":
                    n, l, c = mid
                    r, e = sess.definition(target, l, c, timeout=self.args.op_timeout)
                elif kind == "references":
                    n, l, c = own
                    r, e = sess.references(target, l, c, timeout=self.args.op_timeout)
                else:  # uncommitted edit + query (overlay path)
                    tick += 1
                    edited_text = text + f"\n// wip tick {tick}\n"
                    sess.did_open(target, edited_text) if tick == 1 else sess.did_change(target, edited_text, version=tick + 1)
                    n, l, c = mid
                    r, e = sess.definition(target, l, c, timeout=self.args.op_timeout)
                verdict = "ok" if r is not None else ("hang" if e["timed_out"] else "typed_err")
                self.op(wid, kind, time.monotonic() - t0, verdict, e.get("error"))
                if verdict == "hang":
                    self.event("op_hang", f"{name} {kind} >{self.args.op_timeout}s")
                time.sleep(rnd.uniform(0.3, 1.5))
        except Exception as ex:
            self.event("worker_exception", f"{name}: {ex}")
        finally:
            with self._lock:
                if wid in self.workers:
                    self.workers[wid]["alive"] = False
            if sess is not None and sess.proc.poll() is None:
                sess.shutdown(timeout=5)

    # ---- watchdog policy in-loop ----

    def _watchdog_loop(self):
        cap = 24 * 1024 * 1024
        hard = 100 * 1024 * 1024
        while not self._stop.wait(60):
            rss = self.daemon.rss_now_kb()
            st = self.daemon.status()
            # FAIL-SAFE: a failed/empty status probe means UNKNOWN, not idle —
            # rotating on unknown is what reproduced the incident.
            if not st.get("running"):
                self.event("watchdog_status_unknown", st.get("connection_error", "?"))
                sessions = 999
            else:
                sessions = st.get("client_sessions")
                if sessions is None:
                    sessions = 999
            if rss > hard:
                self.event("watchdog_hard_rotate", f"rss={rss/2**20:.1f}G sessions={sessions}")
                subprocess.run([self.args.binary, "stop"], env=self.daemon.env, capture_output=True)
            elif rss > cap:
                if sessions <= 1:
                    self.event("watchdog_idle_rotate", f"rss={rss/2**20:.1f}G")
                    subprocess.run([self.args.binary, "stop"], env=self.daemon.env, capture_output=True)
                else:
                    self.event("watchdog_wait", f"rss={rss/2**20:.1f}G sessions={sessions}")

    # ---- churn ----

    def _churn(self, roots, deadline):
        time.sleep(self.args.churn_at)
        if self._stop.is_set() or time.monotonic() >= deadline:
            return
        with self._lock:
            victims = [w for w, info in self.workers.items() if info["alive"]][: self.args.churn_kill]
        for wid in victims:
            info = self.workers.get(wid)
            if info and info["session"].proc.poll() is None:
                self.event("churn_kill", f"w{wid:02d} SIGKILL")
                try:
                    os.kill(info["session"].proc.pid, signal.SIGKILL)
                except OSError:
                    pass
        time.sleep(5)
        for j, wid in enumerate(victims):
            nwid = 100 + j
            root = roots[wid % len(roots)]
            self.event("churn_spawn", f"w{nwid} on {os.path.basename(root)}")
            t = threading.Thread(target=self._worker, args=(nwid, root, deadline), daemon=True)
            t.start()

    # ---- main ----

    def run(self):
        n = self.args.workers
        print(f"masstest: {n} worktree-root sessions, {self.args.duration}s, churn at {self.args.churn_at}s", flush=True)
        roots = self.make_worktrees(n)
        self.daemon.fresh_start(os.path.join(self.logs, "daemon.log"))
        deadline = time.monotonic() + self.args.duration
        threads = [
            threading.Thread(target=self._worker, args=(i, roots[i], deadline), daemon=True)
            for i in range(n)
        ]
        wd = threading.Thread(target=self._watchdog_loop, daemon=True)
        churn = threading.Thread(target=self._churn, args=(roots, deadline), daemon=True)
        for t in threads:
            t.start()
        wd.start()
        churn.start()
        for t in threads:
            t.join(timeout=self.args.duration + self.args.op_timeout + 120)
        self._stop.set()
        time.sleep(2)

        # summary
        summary = {"workers": n, "duration_s": self.args.duration, "ops_total": len(self.ops)}
        for verdict in ("ok", "typed_err", "hang"):
            summary[verdict] = sum(1 for o in self.ops if o["verdict"] == verdict)
        by_kind = {}
        for kind in ("initialize", "ready", "docsym", "definition", "references", "edit_def"):
            lats = sorted(o["latency_s"] for o in self.ops if o["kind"] == kind and o["verdict"] == "ok")
            if lats:
                by_kind[kind] = {
                    "n": len(lats),
                    "p50_s": lats[len(lats) // 2],
                    "p95_s": lats[min(len(lats) - 1, int(len(lats) * 0.95))],
                    "max_s": lats[-1],
                }
        summary["latency"] = by_kind
        summary["events"] = {}
        for e in self.events:
            summary["events"][e["kind"]] = summary["events"].get(e["kind"], 0) + 1
        rss = [r["rss_kb"] for r in self.daemon.rss_timeline]
        summary["rss_peak_gib"] = round(max(rss) / 2**20, 1) if rss else None
        stop = self.daemon.stop()
        summary["orphans"] = len(stop.get("orphans", [])) + len(stop.get("orphaned_children", []))
        out = {
            "summary": summary, "ops": self.ops, "events": self.events,
            "rss_timeline": self.daemon.rss_timeline, "status_timeline": self.daemon.status_timeline,
        }
        with open(os.path.join(self.out_dir, "masstest.json"), "w") as f:
            json.dump(out, f, indent=1)
        print(json.dumps(summary, indent=1), flush=True)
        return summary


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--binary", default=os.path.join(os.path.dirname(here), "target/release/analyzed"))
    ap.add_argument("--workers", type=int, default=15)
    ap.add_argument("--duration", type=int, default=600)
    ap.add_argument("--churn-at", type=int, default=240)
    ap.add_argument("--churn-kill", type=int, default=3)
    ap.add_argument("--op-timeout", type=float, default=120.0)
    ap.add_argument("--tmpdir", default="/tmp/anlzb")
    ap.add_argument("--worktrees-root", default=None)
    ap.add_argument("--target-file", default=None, help="repo-relative hot file (default: largest .rs under src/)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    args.out = args.out or os.path.join(here, "results", "v2", "masstest")
    args.worktrees_root = args.worktrees_root or os.path.join(
        os.path.dirname(os.path.realpath(args.repo)), "masstest-worktrees"
    )
    MassTest(args).run()


if __name__ == "__main__":
    main()
