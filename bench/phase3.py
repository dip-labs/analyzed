#!/usr/bin/env python3
"""Phase 3 — worktree storm on a many-worktree subject.

  python3 bench/phase3.py --repo /path/to/many-worktree-repo \
      [--ladder 10,25,50,100,147] [--concurrent 25] \
      [--isolation-repos /path/repo1,/path/repo2] [--repro-symbol some_fn]

Read-only: sessions attach at EXISTING worktree roots and only read; every
touched worktree's git status is snapshotted before/after (CRITICAL event on
change). Missing/pruned worktree dirs are recorded as events and skipped.
CARGO_NET_OFFLINE=true — worktrees at old commits must not trigger network.

Sub-tests: sequential attach ladder (growth of attach/ready cost, workspaces,
RSS; distinct merge-base groups), concurrent attach, cross-repo latency
isolation (storm one repo — do other repos' p95 survive?), eviction/retention
after closing one repo's sessions, resolve-timeout repro for a
chosen symbol, full teardown (orphans, socket).
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness.config import RA_CONFIG_V2  # noqa: E402
from harness.daemon import DaemonController  # noqa: E402
from harness.lsp import LspSession  # noqa: E402
from harness.replay import fn_targets, rs_files, worktree_list  # noqa: E402
from harness.runner import pct  # noqa: E402


def git_state(path):
    r = subprocess.run(["git", "-C", path, "status", "--porcelain"], capture_output=True, text=True)
    return hashlib.sha256(r.stdout.encode()).hexdigest()[:16]


class Phase3:
    def __init__(self, args):
        self.repo = os.path.realpath(args.repo)
        self.out_dir = args.out
        self.logs = os.path.join(self.out_dir, "logs")
        os.makedirs(self.logs, exist_ok=True)
        self.binary = args.binary
        self.events = []
        self._stop_all = threading.Event()
        self.daemon = DaemonController(
            args.binary, args.tmpdir, self.out_dir,
            nice_level=10, watchdog_gib=args.watchdog_gib,
            watchdog_cb=lambda rss: (
                self.event("watchdog_tripped", f"{rss/2**20:.1f} GiB"),
                self._stop_all.set(),
            ),
        )
        self.daemon.env["CARGO_NET_OFFLINE"] = "true"
        self.args = args
        self.git_snapshots = {}

    def event(self, kind, detail):
        self.events.append({"t": time.time(), "kind": kind, "detail": str(detail)[:300]})

    def _spawn(self, name, root):
        return LspSession(
            self.binary, root, self.daemon.env, name,
            stderr_path=os.path.join(self.logs, f"{name}.log"),
            config=RA_CONFIG_V2,
            stall_cb=lambda s, p, e: self.daemon.stall_sample(f"{s.name}-{p.method.split('/')[-1]}"),
        )

    def _snapshot(self, root):
        if root not in self.git_snapshots:
            self.git_snapshots[root] = git_state(root)

    def _warm_keeper(self, tag, root):
        keeper = self._spawn(f"keeper-{tag}", root)
        t0 = time.monotonic()
        keeper.initialize(root, timeout=300)
        r, e = keeper.workspace_symbol("bench_cold_probe", timeout=900)
        return keeper, round(time.monotonic() - t0, 2)

    def merge_base_groups(self, worktrees):
        groups = {}
        for path, branch, _ in worktrees:
            ref = branch or "HEAD"
            r = subprocess.run(
                ["git", "-C", path, "merge-base", "HEAD", "origin/main"],
                capture_output=True, text=True,
            )
            if r.returncode != 0:
                r = subprocess.run(["git", "-C", self.repo, "merge-base",
                                    subprocess.run(["git", "-C", path, "rev-parse", "HEAD"],
                                                   capture_output=True, text=True).stdout.strip(),
                                    "HEAD"], capture_output=True, text=True)
            mb = r.stdout.strip() or "unknown"
            groups.setdefault(mb, []).append(path)
        return groups

    # ---- sequential attach ladder ----

    def ladder(self, marks):
        wts = [(p, b) for p, b, exists in worktree_list(self.repo) if exists and p != self.repo]
        missing = [p for p, b, exists in worktree_list(self.repo) if not exists]
        for p in missing:
            self.event("worktree_missing", p)
        out = {"test": "ladder", "available": len(wts), "missing": len(missing), "attaches": [], "marks": []}
        out["merge_base_groups"] = {
            mb[:12]: len(paths) for mb, paths in self.merge_base_groups(
                [(p, b, True) for p, b in wts]
            ).items()
        }
        self.daemon.fresh_start(os.path.join(self.logs, "daemon-ladder.log"))
        keeper, cold = self._warm_keeper("ladder", self.repo)
        self._snapshot(self.repo)
        out["cold_warm_s"] = cold
        sessions = []
        consecutive_fail = 0
        target = min(max(marks), len(wts))
        for i, (root, branch) in enumerate(wts[:target]):
            if self._stop_all.is_set():
                out["stopped"] = "watchdog"
                break
            self._snapshot(root)
            name = f"wt{i:03d}"
            sess = self._spawn(name, root)
            t0 = time.monotonic()
            init, entry = sess.initialize(root, timeout=300)
            attach_s = entry["latency_s"]
            ready_s, ready_ok = None, False
            if init is not None:
                files = rs_files(root, limit=5)
                if files:
                    t1 = time.monotonic()
                    r, e = sess.document_symbol(os.path.join(root, files[0]), timeout=300)
                    ready_s, ready_ok = round(time.monotonic() - t1, 3), r is not None
            rec = {
                "i": i + 1, "root": os.path.basename(root), "branch": branch,
                "attach_s": attach_s, "ready_s": ready_s, "ready_ok": ready_ok,
                "total_s": round(time.monotonic() - t0, 2),
                "rss_kb": self.daemon.rss_now_kb(),
                "workspaces": self.daemon.status().get("workspaces"),
            }
            out["attaches"].append(rec)
            sessions.append(sess)
            consecutive_fail = 0 if ready_ok else consecutive_fail + 1
            if consecutive_fail >= 5:
                self.event("ladder_degraded", f"5 consecutive ready failures at i={i + 1}")
                out["stopped"] = f"degraded at {i + 1}"
                break
            if (i + 1) in marks:
                st = self.daemon.status()
                out["marks"].append(
                    {"n": i + 1, "rss_kb": self.daemon.rss_now_kb(),
                     "workspaces": st.get("workspaces"),
                     "backends": len(st.get("backend_sessions") or []),
                     "status_raw": {k: st.get(k) for k in ("client_sessions", "workspaces")}}
                )
                print(f"  ladder mark {i + 1}: rss={self.daemon.rss_now_kb()/2**20:.1f}GiB "
                      f"ws={st.get('workspaces')}", flush=True)
        # eviction/retention: close ALL worktree sessions, keep keeper, watch
        for s in sessions:
            s.shutdown(timeout=5)
        evict = []
        t0 = time.monotonic()
        for mark in (30, 60, 120):
            while time.monotonic() - t0 < mark:
                time.sleep(2)
            st = self.daemon.status()
            evict.append({"after_close_s": mark, "rss_kb": self.daemon.rss_now_kb(),
                          "workspaces": st.get("workspaces")})
        out["eviction_after_worktree_close"] = evict
        keeper.shutdown()
        time.sleep(10)
        st = self.daemon.status()
        out["after_keeper_close"] = {"rss_kb": self.daemon.rss_now_kb(), "workspaces": st.get("workspaces")}
        out["daemon_stop"] = self.daemon.stop()
        out["rss_timeline"] = self.daemon.rss_timeline
        out["status_timeline"] = self.daemon.status_timeline
        return out

    # ---- concurrent attach ----

    def concurrent(self, k):
        wts = [(p, b) for p, b, exists in worktree_list(self.repo) if exists and p != self.repo][:k]
        self.daemon.fresh_start(os.path.join(self.logs, "daemon-conc.log"))
        keeper, cold = self._warm_keeper("conc", self.repo)
        out = {"test": "concurrent_attach", "k": len(wts), "cold_warm_s": cold, "attaches": []}
        lock = threading.Lock()

        def one(i, root):
            self._snapshot(root)
            sess = self._spawn(f"c{i:02d}", root)
            t0 = time.monotonic()
            init, entry = sess.initialize(root, timeout=600)
            ready_s, ready_ok = None, False
            if init is not None:
                files = rs_files(root, limit=3)
                if files:
                    t1 = time.monotonic()
                    r, e = sess.document_symbol(os.path.join(root, files[0]), timeout=600)
                    ready_s, ready_ok = round(time.monotonic() - t1, 2), r is not None
            with lock:
                out["attaches"].append(
                    {"i": i, "attach_s": entry["latency_s"], "ready_s": ready_s,
                     "ready_ok": ready_ok, "total_s": round(time.monotonic() - t0, 2)}
                )
            sess.shutdown(timeout=5)

        threads = [threading.Thread(target=one, args=(i, r), daemon=True) for i, (r, b) in enumerate(wts)]
        t0 = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=1200)
        out["wall_s"] = round(time.monotonic() - t0, 1)
        keeper.shutdown()
        out["daemon_stop"] = self.daemon.stop()
        out["rss_timeline"] = self.daemon.rss_timeline
        return out

    # ---- cross-repo latency isolation ----

    def isolation(self, other_repos, storm_s=60):
        self.daemon.fresh_start(os.path.join(self.logs, "daemon-iso.log"))
        keepers, colds = [], {}
        for tag, root in [("landing", self.repo)] + [
            (os.path.basename(r), r) for r in other_repos
        ]:
            k, cold = self._warm_keeper(f"iso-{tag}", root)
            keepers.append((tag, root, k))
            colds[tag] = cold
            self._snapshot(root)
        out = {"test": "isolation", "cold_warm_s": colds, "probes": {}}
        st = self.daemon.status()
        out["workspaces_loaded"] = st.get("workspaces")
        out["backends"] = len(st.get("backend_sessions") or [])

        def probe_targets(root):
            files = rs_files(root, limit=60)
            rel = files[len(files) // 2]
            text = open(os.path.join(root, rel), encoding="utf-8", errors="replace").read()
            ts = fn_targets(text)
            name, line, char = ts[len(ts) // 2] if ts else (None, 0, 0)
            return rel, line, char

        probes = {tag: probe_targets(root) for tag, root, k in keepers[1:]}

        def measure(tag, root, k, n=15):
            rel, line, char = probes[tag]
            lats = []
            for _ in range(n):
                r, e = k.definition(os.path.join(root, rel), line, char, timeout=60)
                lats.append(e["latency_s"])
                time.sleep(0.3)
            return {"p50_s": pct(lats, 50), "p95_s": pct(lats, 95), "max_s": pct(lats, 100)}

        for tag, root, k in keepers[1:]:
            out["probes"][f"{tag}_baseline"] = measure(tag, root, k)

        stop_storm = threading.Event()
        storm_ops = {"n": 0}

        def storm():
            tag, root, k = keepers[0]
            files = rs_files(root, limit=100)
            i = 0
            while not stop_storm.is_set():
                rel = files[i % len(files)]
                text = open(os.path.join(root, rel), encoding="utf-8", errors="replace").read()
                ts = fn_targets(text)
                if ts:
                    name, line, char = ts[i % len(ts)]
                    k.references(os.path.join(root, rel), line, char, timeout=120)
                    k.workspace_symbol(name, timeout=120)
                    storm_ops["n"] += 2
                i += 1

        storm_threads = [threading.Thread(target=storm, daemon=True) for _ in range(4)]
        for t in storm_threads:
            t.start()
        time.sleep(3)
        for tag, root, k in keepers[1:]:
            out["probes"][f"{tag}_during_storm"] = measure(tag, root, k)
        stop_storm.set()
        time.sleep(2)
        out["storm_ops"] = storm_ops["n"]
        for tag, root, k in keepers:
            k.shutdown()
        out["daemon_stop"] = self.daemon.stop()
        return out

    # ---- resolve-timeout repro ----

    def repro(self, symbol):
        self.daemon.fresh_start(os.path.join(self.logs, "daemon-repro.log"))
        out = {"test": "repro_resolve_timeout", "symbol": symbol}
        keeper, cold = self._warm_keeper("repro", self.repo)
        self._snapshot(self.repo)
        out["cold_warm_s"] = cold
        r, e = keeper.workspace_symbol(symbol, timeout=300)
        out["workspace_symbol"] = {
            "latency_s": e["latency_s"], "timed_out": e["timed_out"],
            "hits": len(r) if isinstance(r, list) else None,
            "error": str(e.get("error"))[:120] if e.get("error") else None,
        }
        loc = None
        if isinstance(r, list) and r:
            loc = (r[0].get("location") or {})
        if loc and loc.get("uri"):
            path = loc["uri"].replace("file://", "")
            line = loc.get("range", {}).get("start", {}).get("line", 0)
            char = loc.get("range", {}).get("start", {}).get("character", 0)
            for op, fn in (("definition", keeper.definition), ("references", keeper.references)):
                rr, ee = fn(path, line, char, 300)
                out[op] = {"latency_s": ee["latency_s"], "timed_out": ee["timed_out"],
                           "n": len(rr) if isinstance(rr, list) else None}
        else:
            # grep fallback like the original task's workaround, to locate it
            g = subprocess.run(["grep", "-rn", "-m1", f"fn {symbol}", os.path.join(self.repo, "src")],
                               capture_output=True, text=True).stdout.strip()
            out["grep_fallback"] = g[:200] or "not found"
            if g:
                path, line = g.split(":")[0], int(g.split(":")[1]) - 1
                rr, ee = keeper.definition(path, line, len(g.split(":")[2]) - len(g.split(":")[2].lstrip()) + 3, 300)
                out["definition"] = {"latency_s": ee["latency_s"], "timed_out": ee["timed_out"]}
        keeper.shutdown()
        out["daemon_stop"] = self.daemon.stop()
        return out

    def integrity_check(self):
        rows = []
        for root, h in self.git_snapshots.items():
            h2 = git_state(root)
            rows.append({"root": root, "unchanged": h2 == h})
            if h2 != h:
                self.event("CRITICAL_git_state_changed", root)
        return rows


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--binary", default=os.path.join(os.path.dirname(here), "target/release/analyzed"))
    ap.add_argument("--ladder", default="10,25,50,100,147")
    ap.add_argument("--concurrent", type=int, default=25)
    ap.add_argument("--isolation-repos", default=None)
    ap.add_argument("--repro-symbol", default=None)
    ap.add_argument("--tests", default="ladder,concurrent,isolation")
    ap.add_argument("--watchdog-gib", type=float, default=24.0)
    ap.add_argument("--tmpdir", default="/tmp/anlzb")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    args.out = args.out or os.path.join(here, "results", "v2", f"phase3-{os.path.basename(os.path.realpath(args.repo))}")

    p3 = Phase3(args)
    tests = args.tests.split(",")
    results = {"phase": 3, "repo": p3.repo, "t": time.time(), "config": RA_CONFIG_V2}
    if "ladder" in tests:
        print("phase3 ladder...", flush=True)
        results["ladder"] = p3.ladder([int(x) for x in args.ladder.split(",")])
        with open(os.path.join(args.out, "phase3.json"), "w") as f:
            json.dump(results, f, indent=1)
    if "concurrent" in tests and not p3._stop_all.is_set():
        print("phase3 concurrent attach...", flush=True)
        results["concurrent"] = p3.concurrent(args.concurrent)
        with open(os.path.join(args.out, "phase3.json"), "w") as f:
            json.dump(results, f, indent=1)
    if "isolation" in tests and args.isolation_repos and not p3._stop_all.is_set():
        print("phase3 isolation...", flush=True)
        results["isolation"] = p3.isolation(args.isolation_repos.split(","))
        with open(os.path.join(args.out, "phase3.json"), "w") as f:
            json.dump(results, f, indent=1)
    if "repro" in tests and args.repro_symbol:
        print("phase3 repro...", flush=True)
        results["repro"] = p3.repro(args.repro_symbol)
    results["git_integrity"] = p3.integrity_check()
    results["events"] = p3.events
    results["stall_samples"] = p3.daemon.sample_log
    with open(os.path.join(args.out, "phase3.json"), "w") as f:
        json.dump(results, f, indent=1)
    print("phase3 complete", flush=True)


if __name__ == "__main__":
    main()
