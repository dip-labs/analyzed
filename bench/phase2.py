#!/usr/bin/env python3
"""Phase 2 — live chaos on a LIVE repo with real worktrees and working agents.

  python3 bench/phase2.py --repo /path/to/live-repo --live [--duration 600]

Read-only discipline: sessions only read (didOpen with exact disk text, no
edits, rename only as preview and never applied). Every touched worktree's
`git status` is snapshotted before and after; any difference is reported as a
CRITICAL event. /tmp worktrees are prunable and may vanish mid-run — a session
drop is a bench EVENT (resilience metric), not a crash.

Percentiles from this phase are NOT for the verdict (noisy box) — the output
is events: races, stale overlays, watcher behavior, stalls, session drops,
plus a box timeline (load, cargo/rustc activity) for stall attribution.
"""

import argparse
import hashlib
import json
import os
import random
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness.config import RA_CONFIG_V2  # noqa: E402
from harness.daemon import DaemonController  # noqa: E402
from harness.lsp import LspSession  # noqa: E402
from harness.replay import fn_targets, rs_files, worktree_list  # noqa: E402
from harness.runner import _sym_vector  # noqa: E402


def git_state(path):
    r = subprocess.run(["git", "-C", path, "status", "--porcelain"], capture_output=True, text=True)
    return hashlib.sha256(r.stdout.encode()).hexdigest()[:16], r.stdout


class LiveChaos:
    def __init__(self, args):
        self.repo = os.path.realpath(args.repo)
        self.out_dir = args.out
        os.makedirs(os.path.join(self.out_dir, "logs"), exist_ok=True)
        self.duration = args.duration
        self.events = []
        self.ops = []
        self.box_timeline = []
        self.freshness = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.daemon = DaemonController(
            args.binary, args.tmpdir, self.out_dir,
            nice_level=10, watchdog_gib=args.watchdog_gib,
            watchdog_cb=lambda rss: self.event("watchdog", None, f"{rss/2**20:.1f} GiB — emergency stop") or self._stop.set(),
        )
        self.binary = args.binary
        wts = [(p, br) for p, br, exists in worktree_list(self.repo) if exists]
        self.worktrees = wts[: args.max_worktrees]
        self.git_snapshots = {}

    def event(self, kind, session, detail):
        with self._lock:
            self.events.append({"t": time.time(), "kind": kind, "session": session, "detail": str(detail)[:300]})

    def _spawn(self, name, root):
        return LspSession(
            self.binary, root, self.daemon.env, name,
            stderr_path=os.path.join(self.out_dir, "logs", f"{name}.log"),
            config=RA_CONFIG_V2,
            stall_cb=lambda s, p, e: (
                self.event("stall", s.name, f"{p.method} outstanding >1s"),
                self.daemon.stall_sample(f"{s.name}-{p.method.split('/')[-1]}"),
            ),
        )

    def _box_loop(self):
        while not self._stop.is_set():
            try:
                load = os.getloadavg()[0]
                ps = subprocess.run(["ps", "-axo", "pcpu=,rss=,comm="], capture_output=True, text=True).stdout
                cargo = rustc = 0
                daemon_cpu = 0.0
                for line in ps.splitlines():
                    parts = line.split(None, 2)
                    if len(parts) < 3:
                        continue
                    comm = parts[2]
                    if "cargo" in comm:
                        cargo += 1
                    elif "rustc" in comm:
                        rustc += 1
                    elif "analyzed" in comm:
                        daemon_cpu += float(parts[0])
                self.box_timeline.append(
                    {"t": time.time(), "load1": round(load, 2), "cargo": cargo, "rustc": rustc,
                     "analyzed_cpu_pct": round(daemon_cpu, 1), "rss_kb": self.daemon.rss_now_kb()}
                )
            except Exception:
                pass
            self._stop.wait(2.0)

    def _freshness_loop(self, root):
        """Watch the most recently modified .rs file; when disk changes, time
        how long the daemon takes to serve the new content (target: <2s)."""
        deadline = time.monotonic() + self.duration
        sess = None
        try:
            sess = self._spawn("freshness", root)
            sess.initialize(root)
            tracked, last_hash = None, None
            while time.monotonic() < deadline and not self._stop.is_set():
                files = [(os.path.getmtime(os.path.join(root, f)), f) for f in rs_files(root, limit=None)
                         if os.path.exists(os.path.join(root, f))]
                if not files:
                    time.sleep(5)
                    continue
                _, newest = max(files)
                path = os.path.join(root, newest)
                text = open(path, encoding="utf-8", errors="replace").read()
                h = hashlib.sha256(text.encode()).hexdigest()
                if newest != tracked:
                    tracked, last_hash = newest, h
                    time.sleep(2)
                    continue
                if h != last_hash:
                    want = [(n, l) for n, l, _ in fn_targets(text)]
                    t0 = time.monotonic()
                    seen = None
                    while time.monotonic() - t0 < 30:
                        r, e = sess.document_symbol(path, timeout=30)
                        if r is not None and _sym_vector(r) == want:
                            seen = time.monotonic() - t0
                            break
                        time.sleep(0.3)
                    self.freshness.append(
                        {"t": time.time(), "file": newest, "visible_after_s": round(seen, 3) if seen else None}
                    )
                    self.event("disk_change", "freshness",
                               f"{newest} visible_after={seen and round(seen, 2)}s")
                    last_hash = h
                time.sleep(1)
        except Exception as e:
            self.event("freshness_error", "freshness", e)
        finally:
            if sess:
                sess.kill()

    def _session_loop(self, idx):
        deadline = time.monotonic() + self.duration
        rnd = random.Random(1000 + idx)
        sess, root, name = None, None, None
        while time.monotonic() < deadline and not self._stop.is_set():
            try:
                if sess is None:
                    root, branch = rnd.choice(self.worktrees)
                    if not os.path.isdir(root):
                        self.event("worktree_gone", f"live-{idx}", root)
                        continue
                    name = f"live-{idx}"
                    if root not in self.git_snapshots:
                        self.git_snapshots[root] = git_state(root)
                    sess = self._spawn(name, root)
                    t0 = time.monotonic()
                    init, entry = sess.initialize(root, timeout=180)
                    self.ops.append({"t": time.time(), "session": name, "op": "attach", "root": root,
                                     "latency_s": entry["latency_s"], "ok": init is not None})
                    if init is None:
                        self.event("attach_failed", name, root)
                        sess.kill()
                        sess = None
                        continue
                files = rs_files(root, limit=300)
                if not files:
                    raise RuntimeError("no rs files")
                rel = rnd.choice(files)
                path = os.path.join(root, rel)
                if not os.path.exists(path):
                    self.event("file_vanished", name, rel)
                    continue
                text = open(path, encoding="utf-8", errors="replace").read()
                sess.did_open(path, text)
                r, e = sess.document_symbol(path, timeout=60)
                self.ops.append({"t": time.time(), "session": name, "op": "documentSymbol",
                                 "latency_s": e["latency_s"], "ok": r is not None,
                                 "err": str(e.get("error"))[:80] if e.get("error") else None})
                targets = fn_targets(text)
                if targets and r is not None:
                    fname, line, char = rnd.choice(targets)
                    for op, fn in (("definition", sess.definition), ("references", sess.references)):
                        rr, ee = fn(path, line, char, 60)
                        self.ops.append({"t": time.time(), "session": name, "op": op,
                                         "latency_s": ee["latency_s"], "ok": rr is not None,
                                         "empty": rr == [] or rr is None,
                                         "err": str(ee.get("error"))[:80] if ee.get("error") else None})
                        if ee.get("error") and "Invalid offset" in str(ee["error"]):
                            self.event("stale_view", name, f"{rel}: {ee['error']}")
                sess.did_close(path)
                if sess.proc.poll() is not None:
                    self.event("session_died", name, f"rc={sess.proc.returncode}")
                    sess = None
                    continue
                if rnd.random() < 0.15:  # random switching between worktrees
                    sess.shutdown()
                    self.ops.append({"t": time.time(), "session": name, "op": "detach", "root": root})
                    sess = None
                time.sleep(rnd.uniform(0.2, 1.5))
            except Exception as e:
                self.event("session_error", name or f"live-{idx}", e)
                if sess is not None:
                    sess.kill()
                    sess = None
                time.sleep(2)
        if sess is not None:
            sess.shutdown()

    def run(self, n_sessions):
        self.daemon.fresh_start(os.path.join(self.out_dir, "logs", "daemon.log"))
        box = threading.Thread(target=self._box_loop, daemon=True)
        box.start()
        keeper = self._spawn("keeper", self.repo)
        t0 = time.monotonic()
        keeper.initialize(self.repo, timeout=180)
        r, e = keeper.workspace_symbol("bench_cold_probe", timeout=900)
        cold = round(time.monotonic() - t0, 2)
        print(f"live world warm in {cold}s", flush=True)
        self.git_snapshots[self.repo] = git_state(self.repo)

        threads = [threading.Thread(target=self._session_loop, args=(i,), daemon=True) for i in range(n_sessions)]
        threads.append(threading.Thread(target=self._freshness_loop, args=(self.repo,), daemon=True))
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=self.duration + 300)
        self._stop.set()
        keeper.shutdown()

        # read-only discipline check
        integrity = []
        for root, (h, listing) in self.git_snapshots.items():
            h2, listing2 = git_state(root)
            ok = h2 == h
            integrity.append({"root": root, "unchanged": ok})
            if not ok:
                self.event("CRITICAL_git_state_changed", None, f"{root}\nBEFORE:\n{listing}\nAFTER:\n{listing2}"[:1000])
        stop = self.daemon.stop()
        return {
            "phase": 2,
            "repo": self.repo,
            "cold_warm_s": cold,
            "worktrees_available": len(self.worktrees),
            "sessions": n_sessions,
            "duration_s": self.duration,
            "ops_total": len(self.ops),
            "events": self.events,
            "ops": self.ops,
            "freshness": self.freshness,
            "box_timeline": self.box_timeline,
            "git_integrity": integrity,
            "daemon_stop": stop,
            "rss_timeline": self.daemon.rss_timeline,
            "status_timeline": self.daemon.status_timeline,
            "stall_samples": self.daemon.sample_log,
        }


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--live", action="store_true", required=True,
                    help="explicit acknowledgement that the subject is a LIVE repo")
    ap.add_argument("--binary", default=os.path.join(os.path.dirname(here), "target/release/analyzed"))
    ap.add_argument("--sessions", type=int, default=6)
    ap.add_argument("--duration", type=int, default=600)
    ap.add_argument("--max-worktrees", type=int, default=12)
    ap.add_argument("--watchdog-gib", type=float, default=24.0)
    ap.add_argument("--tmpdir", default="/tmp/anlzb")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    args.out = args.out or os.path.join(here, "results", "v2", f"phase2-{os.path.basename(os.path.realpath(args.repo))}")

    chaos = LiveChaos(args)
    print(f"phase2 live: {len(chaos.worktrees)} worktrees, {args.sessions} sessions, {args.duration}s", flush=True)
    result = chaos.run(args.sessions)
    with open(os.path.join(args.out, "live.json"), "w") as f:
        json.dump(result, f, indent=1)
    crit = [e for e in result["events"] if "CRITICAL" in e["kind"]]
    print(f"phase2 done: {result['ops_total']} ops, {len(result['events'])} events, "
          f"{len(crit)} CRITICAL, freshness={len(result['freshness'])}", flush=True)


if __name__ == "__main__":
    main()
