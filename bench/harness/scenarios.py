"""Scenario matrix for the analyzed bench gate.

Model: every session initializes against
ONE canonical subject root; a session's "worktree" is expressed as didOpen
overlays whose text equals its generated git worktree's file contents. The
daemon keeps one warm world; overlays are per-session private views.

Scenarios:
  disjoint       each session overlays 5 distinct files (main agent case)
  same_file      sessions hold different versions of ONE file (worst case)
  distant_branch sessions overlay a large branch diff (hundreds of files)
  manifest       overlay touches Cargo.toml (+ real-worktree-root probe)
  storm          interleaved query/edit round-robin across sessions
"""

from __future__ import annotations

import json
import os
import threading
import time

from .daemon import DaemonController
from .diffs import WorktreeFactory, bin_roots, make_variant, pick_files, read_subject
from .lsp import LspSession

GIB = 1024 * 1024  # ps rss is in KiB

RA_CONFIG = {
    "cachePriming": {"enable": True},
    "checkOnSave": False,
    "diagnostics": {"enable": True},
    "files": {"watcher": "server"},
    "procMacro": {"enable": True},
}


def pct(values, p):
    if not values:
        return None
    vs = sorted(values)
    idx = min(len(vs) - 1, max(0, round(p / 100 * len(vs) + 0.5) - 1))
    return round(vs[idx], 4)


class CellContext:
    def __init__(self):
        self.abort = threading.Event()
        self.abort_reason = None
        self.sessions = []
        self.lock = threading.Lock()

    def register(self, sess):
        with self.lock:
            self.sessions.append(sess)

    def trip(self, reason):
        if not self.abort.is_set():
            self.abort_reason = reason
            self.abort.set()
        with self.lock:
            for s in self.sessions:
                s.kill()


class Bench:
    def __init__(
        self,
        binary: str,
        subject_root: str,
        base_rev: str,
        out_dir: str,
        tmpdir: str,
        worktrees_root: str,
        cells=(1, 5, 20),
        iters: int = 10,
        distant_files: int = 250,
        cell_budget_s: float = 1500.0,
        rss_abort_gib: float = 100.0,
        make_worktrees: bool = True,
    ):
        self.binary = binary
        self.subject_root = subject_root
        self.base_rev = base_rev
        self.out_dir = out_dir
        self.tmpdir = tmpdir
        self.cells = list(cells)
        self.iters = iters
        self.distant_files = distant_files
        self.cell_budget_s = cell_budget_s
        self.rss_abort_kb = rss_abort_gib * GIB
        self.make_worktrees = make_worktrees
        self.logs_dir = os.path.join(out_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        self.daemon = DaemonController(binary, tmpdir, out_dir)
        self.wt = WorktreeFactory(subject_root, worktrees_root, base_rev)
        self._prepared_worktrees: set[str] = set()
        self.worktree_times: list[dict] = []

        # Pools may overlap across scenarios (each scenario runs on a fresh
        # daemon); within a scenario the ready/same-file targets must not
        # collide with that scenario's overlay files.
        max_sessions = max(self.cells)
        need = max(max_sessions * 4 + 1, self.distant_files + 1)
        self.pool = pick_files(subject_root, need, min_size=200, max_size=64000)
        self.disjoint_files = {i: self.pool[i * 4 : i * 4 + 4] for i in range(max_sessions)}
        self.ready_file = self.pool[max_sessions * 4]
        reserved = {self.ready_file}
        self.distant_pool = [f for f in self.pool if f not in reserved][: self.distant_files]
        # Overlay semantics only attach when the modified file IS a crate root
        # (sync drops identical-to-base cone files, so a module file never
        # pulls its crate root into the overlay). this subject's crate roots are its
        # bin targets; sessions get one each as their marker/query file.
        self.roots = bin_roots(subject_root)
        if not self.roots:
            raise RuntimeError("subject has no bin crate roots")
        # same_file worst case: N divergent versions of ONE crate root —
        # simultaneously one-path-N-versions and N overlay crate cones.
        self.same_target = self.roots[len(self.roots) // 2]

    def _session_root(self, sid: int) -> str:
        return self.roots[sid % len(self.roots)]

    # ---- session helpers ----

    def _spawn(self, name: str, root: str | None = None) -> LspSession:
        return LspSession(
            self.binary,
            root or self.subject_root,
            self.daemon.env,
            name,
            stderr_path=os.path.join(self.logs_dir, f"{name}.stderr.log"),
            config=RA_CONFIG,
            stall_cb=lambda s, p, e: self.daemon.stall_sample(
                f"{s.name}-{p.method.split('/')[-1]}"
            ),
        )

    def _abs(self, rel: str, root: str | None = None) -> str:
        return os.path.join(root or self.subject_root, rel)

    # ---- overlay plans (pure text; worktrees mirror these bytes) ----

    def _overlay_plan(self, scen: str, sid: int):
        """List of {rel, text, pos|None}; pos marks the query file.

        `disjoint` is the agent-real module-only case: analyzed 0.4.2 attaches
        no overlay crate for module files (semantic queries return empty —
        that is a finding, not a harness bug). The rooted scenarios put the
        session marker in a bin crate ROOT so the overlay crate cone attaches.
        """
        if scen == "disjoint":
            plan = []
            for j, rel in enumerate(self.disjoint_files[sid]):
                base = read_subject(self.subject_root, rel)
                if j == 0:
                    text, pos = make_variant(base, scen, sid)
                    plan.append({"rel": rel, "text": text, "pos": pos})
                else:
                    text, _ = make_variant(base, f"{scen}x{j}", sid)
                    plan.append({"rel": rel, "text": text, "pos": None})
            return plan
        if scen in ("disjoint_rooted", "storm"):
            root = self._session_root(sid)
            text, pos = make_variant(read_subject(self.subject_root, root), scen, sid)
            plan = [{"rel": root, "text": text, "pos": pos}]
            for j, rel in enumerate(self.disjoint_files[sid][:3]):
                mtext, _ = make_variant(
                    read_subject(self.subject_root, rel), f"{scen}x{j}", sid
                )
                plan.append({"rel": rel, "text": mtext, "pos": None})
            return plan
        if scen == "same_file":
            base = read_subject(self.subject_root, self.same_target)
            text, pos = make_variant(base, scen, sid)
            return [{"rel": self.same_target, "text": text, "pos": pos}]
        if scen == "distant_branch":
            root = self._session_root(sid)
            text, pos = make_variant(read_subject(self.subject_root, root), scen, sid)
            plan = [{"rel": root, "text": text, "pos": pos}]
            for j, rel in enumerate(self.distant_pool):
                base = read_subject(self.subject_root, rel)
                pad = (
                    base
                    + f"\n// bench distant-branch pad\npub fn bench_branch_pad_{j}() -> u64 {{ {j} }}\n"
                )
                plan.append({"rel": rel, "text": pad, "pos": None})
            return plan
        if scen == "manifest":
            manifest = read_subject(self.subject_root, "Cargo.toml")
            manifest += f"\n[features]\nbench_flag_{sid} = []\n"
            root = self._session_root(sid)
            text, pos = make_variant(read_subject(self.subject_root, root), scen, sid)
            module = self.disjoint_files[sid][0]
            mtext, _ = make_variant(read_subject(self.subject_root, module), f"{scen}m", sid)
            return [
                {"rel": "Cargo.toml", "text": manifest, "pos": None},
                {"rel": root, "text": text, "pos": pos},
                {"rel": module, "text": mtext, "pos": None},
            ]
        raise ValueError(scen)

    def prepare_scenario_worktrees(self, scen: str):
        """Create real git worktrees whose contents equal the overlay texts.

        Serial on purpose: git serializes worktree add on repo locks. Skipped
        for same_file (one path, many versions — inherently overlay-only).
        """
        if not self.make_worktrees or scen in ("same_file",) or scen in self._prepared_worktrees:
            return
        self._prepared_worktrees.add(scen)
        n = max(self.cells)
        if scen == "distant_branch":
            plan = self._overlay_plan(scen, 0)
            files = {p["rel"]: p["text"] for p in plan if p["pos"] is None}
            t0 = time.monotonic()
            path = self.wt.create("distant-branch-base", files, "bench: distant branch pad")
            self.worktree_times.append(
                {"name": "distant-branch-base", "files": len(files), "s": round(time.monotonic() - t0, 2)}
            )
            self._verify_worktree(path, plan[1] if plan[0]["pos"] else plan[0])
            return
        for sid in range(n):
            plan = self._overlay_plan(scen, sid)
            files = {p["rel"]: p["text"] for p in plan}
            t0 = time.monotonic()
            path = self.wt.create(f"{scen}-s{sid}", files, f"bench: {scen} session {sid}")
            self.worktree_times.append(
                {"name": f"{scen}-s{sid}", "files": len(files), "s": round(time.monotonic() - t0, 2)}
            )
            if sid == 0:
                self._verify_worktree(path, plan[0])

    def _verify_worktree(self, wt_path: str, plan_entry: dict):
        on_disk = read_subject(wt_path, plan_entry["rel"])
        if on_disk != plan_entry["text"]:
            raise RuntimeError(
                f"worktree {wt_path} content diverges from overlay plan for {plan_entry['rel']}"
            )

    # ---- per-session phase machine ----

    def _run_session(self, scen, sid, barrier, deadline, ctx: CellContext, rec: dict):
        name = f"{scen}-s{sid}"
        rec.update({"session": sid, "phases": {}, "warm": [], "violations": [], "aborted": None})
        plan = self._overlay_plan(scen, sid)
        query = next(p for p in plan if p["pos"])
        qpath = self._abs(query["rel"])
        pos = query["pos"]
        sess = None
        try:
            barrier.wait(timeout=120)
            sess = self._spawn(name)
            ctx.register(sess)
            init, entry = sess.initialize(self.subject_root)
            rec["phases"]["attach_s"] = entry["latency_s"]
            if init is None:
                rec["aborted"] = "initialize failed/timeout"
                return
            # readiness: cheap query against a BASE file before overlay install
            # (didOpen race: the protocol has no view-ready signal)
            t0 = time.monotonic()
            ready_path = self._abs(self.ready_file)
            sess.did_open(ready_path, read_subject(self.subject_root, self.ready_file))
            result, entry = sess.document_symbol(ready_path, timeout=180)
            rec["phases"]["ready_s"] = round(time.monotonic() - t0, 4)
            rec["phases"]["ready_ok"] = result is not None
            if result is None:
                rec["aborted"] = "readiness query failed"
                return
            if ctx.abort.is_set() or time.monotonic() > deadline:
                rec["aborted"] = ctx.abort_reason or "budget before overlay"
                return
            # overlay install
            t0 = time.monotonic()
            for p in plan:
                sess.did_open(self._abs(p["rel"]), p["text"])
            rec["phases"]["overlay_files"] = len(plan)
            attempts, first = 0, None
            while (
                time.monotonic() - t0 < 300
                and time.monotonic() < deadline
                and not ctx.abort.is_set()
            ):
                attempts += 1
                result, entry = sess.document_symbol(qpath, timeout=300)
                if result and _symbols_contain(result, pos["name"]):
                    first = time.monotonic() - t0
                    break
                time.sleep(0.5)
            rec["phases"]["first_result_s"] = round(first, 4) if first else None
            rec["phases"]["first_result_attempts"] = attempts
            if first is None:
                rec["aborted"] = ctx.abort_reason or "no first overlay result in budget"
                return
            # warm queries — no re-synchronization: agents don't align phases
            new_name = pos["name"] + "_rn"
            for _ in range(self.iters):
                if ctx.abort.is_set() or time.monotonic() > deadline:
                    rec["aborted"] = ctx.abort_reason or "budget during warm"
                    break
                for qtype in ("definition", "references", "rename", "wsymbol"):
                    r, e = self._one_query(sess, qtype, qpath, pos, new_name)
                    correct = self._check(qtype, r, qpath, pos, rec)
                    entry = {
                        "q": qtype,
                        "latency_s": e["latency_s"],
                        "ok": r is not None,
                        "correct": correct,
                        "timed_out": e["timed_out"],
                    }
                    if e.get("error"):
                        entry["error"] = e["error"]
                    rec["warm"].append(entry)
        except threading.BrokenBarrierError:
            rec["aborted"] = "start barrier broken"
        except Exception as e:
            rec["aborted"] = f"exception: {e}"
        finally:
            if sess is not None:
                rec["stalls"] = list(sess.stall_events)
                rec["requests"] = len(sess.request_log)
                if not ctx.abort.is_set():
                    for p in plan:
                        try:
                            sess.did_close(self._abs(p["rel"]))
                        except Exception:
                            pass
                    rec["teardown_clean"] = sess.shutdown()
                else:
                    sess.kill()
                    rec["teardown_clean"] = False

    def _one_query(self, sess, qtype, qpath, pos, new_name):
        if qtype == "definition":
            return sess.definition(qpath, pos["call"]["line"], pos["call"]["char"])
        if qtype == "references":
            return sess.references(qpath, pos["def"]["line"], pos["def"]["char"])
        if qtype == "rename":
            return sess.rename(qpath, pos["def"]["line"], pos["def"]["char"], new_name)
        # isolation probe: generic prefix matches every session's marker if leaked
        return sess.workspace_symbol(pos["name"].rsplit("_", 1)[0])

    def _check(self, qtype, result, qpath, pos, rec):
        if result is None:
            return False
        try:
            if qtype == "definition":
                locs = result if isinstance(result, list) else [result]
                ok = any(
                    l.get("uri", "").endswith(qpath)
                    and l.get("range", {}).get("start", {}).get("line") == pos["def"]["line"]
                    for l in locs
                )
                if not ok:
                    rec["violations"].append({"q": qtype, "detail": "definition missed marker"})
                return ok
            if qtype == "references":
                ok = isinstance(result, list) and len(result) >= 2
                if not ok:
                    rec["violations"].append({"q": qtype, "detail": f"{len(result or [])} refs"})
                return ok
            if qtype == "rename":
                changes = (result or {}).get("changes") or {}
                doc_changes = (result or {}).get("documentChanges") or []
                return bool(changes) or bool(doc_changes)
            # workspace/symbol on shared prefix: own must be visible, foreign must not
            names = [s.get("name", "") for s in (result or [])]
            own = any(n == pos["name"] for n in names)
            prefix = pos["name"].rsplit("_", 1)[0] + "_"
            foreign = [n for n in names if n.startswith(prefix) and n != pos["name"]]
            if foreign:
                rec["violations"].append(
                    {"q": qtype, "detail": f"foreign symbols visible: {foreign[:5]}"}
                )
            if not own:
                rec["violations"].append({"q": qtype, "detail": "own symbol missing"})
            return own and not foreign
        except Exception as e:
            rec["violations"].append({"q": qtype, "detail": f"check error: {e}"})
            return False

    # ---- cells ----

    def run_cell(self, scen: str, n: int) -> dict:
        cell = {"scenario": scen, "sessions": n, "t_start": time.time(), "cell_aborted": None}
        deadline = time.monotonic() + self.cell_budget_s
        ctx = CellContext()
        barrier = threading.Barrier(n)
        records = [dict() for _ in range(n)]
        threads = [
            threading.Thread(
                target=self._run_session,
                args=(scen, i, barrier, deadline, ctx, records[i]),
                name=f"drv-{scen}-{i}",
                daemon=True,
            )
            for i in range(n)
        ]
        cell["rss_before_kb"] = self.daemon.rss_now_kb()
        for t in threads:
            t.start()
        # monitor: RSS guard + hard deadline
        while any(t.is_alive() for t in threads):
            if self.daemon.rss_now_kb() > self.rss_abort_kb:
                ctx.trip("rss guard tripped")
            if time.monotonic() > deadline + 360:
                ctx.trip("cell hard-deadline exceeded")
            for t in threads:
                t.join(timeout=1)
        cell["cell_aborted"] = ctx.abort_reason
        cell["records"] = records
        cell["rss_peak_kb"] = max(
            (r["rss_kb"] for r in self.daemon.rss_timeline if r["t"] >= cell["t_start"]),
            default=None,
        )
        cell["wall_s"] = round(time.time() - cell["t_start"], 1)
        # settle: wait for agent sessions to unregister, then post-cell RSS
        t0 = time.monotonic()
        while time.monotonic() - t0 < 60:
            st = self.daemon.status()
            if (st.get("client_sessions") or 0) <= 1:
                break
            time.sleep(1)
        time.sleep(3)
        cell["rss_after_kb"] = self.daemon.rss_now_kb()
        st = self.daemon.status()
        cell["status_after"] = {k: st.get(k) for k in ("client_sessions", "workspaces")}
        cell["warm_summary"] = self._summarize(records)
        return cell

    def _summarize(self, records):
        out = {}
        for q in ("definition", "references", "rename", "wsymbol"):
            lats = [
                w["latency_s"] for r in records for w in r.get("warm", []) if w["q"] == q and w["ok"]
            ]
            out[q] = {
                "n": len(lats),
                "p50_s": pct(lats, 50),
                "p95_s": pct(lats, 95),
                "max_s": pct(lats, 100),
                "timeouts": sum(
                    1 for r in records for w in r.get("warm", []) if w["q"] == q and w["timed_out"]
                ),
            }
        out["attach_s"] = [r.get("phases", {}).get("attach_s") for r in records]
        out["ready_s"] = [r.get("phases", {}).get("ready_s") for r in records]
        out["first_result_s"] = [r.get("phases", {}).get("first_result_s") for r in records]
        out["aborted"] = [r.get("aborted") for r in records if r.get("aborted")]
        out["violations"] = sum(len(r.get("violations", [])) for r in records)
        out["stalls_gt1s"] = sum(len(r.get("stalls", [])) for r in records)
        return out

    # ---- storm cell (sequential round-robin, measures view-flip cost) ----

    def run_storm_cell(self, n: int) -> dict:
        scen = "storm"
        cell = {
            "scenario": scen,
            "sessions": n,
            "t_start": time.time(),
            "mode": "round-robin",
            "cell_aborted": None,
        }
        deadline = time.monotonic() + self.cell_budget_s
        cell["rss_before_kb"] = self.daemon.rss_now_kb()
        sessions, plans, setup = [], [], []
        for sid in range(n):
            rec = {"session": sid}
            plan = self._overlay_plan(scen, sid)
            sess = self._spawn(f"{scen}-s{sid}")
            init, entry = sess.initialize(self.subject_root)
            rec["attach_s"] = entry["latency_s"]
            ready_path = self._abs(self.ready_file)
            sess.did_open(ready_path, read_subject(self.subject_root, self.ready_file))
            sess.document_symbol(ready_path, timeout=180)
            t0 = time.monotonic()
            for p in plan:
                sess.did_open(self._abs(p["rel"]), p["text"])
            query = next(p for p in plan if p["pos"])
            first = None
            while time.monotonic() - t0 < 300:
                res, e = sess.document_symbol(self._abs(query["rel"]), timeout=300)
                if res and _symbols_contain(res, query["pos"]["name"]):
                    first = time.monotonic() - t0
                    break
                time.sleep(0.5)
            rec["first_result_serial_s"] = round(first, 4) if first else None
            sessions.append(sess)
            plans.append((query, plan))
            setup.append(rec)
        cell["setup"] = setup

        ops = []
        rounds = 15
        kinds = ("definition", "references", "edit_then_def", "wsymbol")
        tick = 0
        for rnd in range(rounds):
            if time.monotonic() > deadline:
                cell["cell_aborted"] = "budget during storm"
                break
            for sid, sess in enumerate(sessions):
                query, plan = plans[sid]
                qpath = self._abs(query["rel"])
                pos = query["pos"]
                kind = kinds[(rnd + sid) % len(kinds)]
                t0 = time.monotonic()
                if kind == "edit_then_def":
                    tick += 1
                    sess.did_change(qpath, query["text"] + f"\n// storm tick {tick}\n", version=tick + 1)
                    r, e = sess.definition(qpath, pos["call"]["line"], pos["call"]["char"])
                elif kind == "definition":
                    r, e = sess.definition(qpath, pos["call"]["line"], pos["call"]["char"])
                elif kind == "references":
                    r, e = sess.references(qpath, pos["def"]["line"], pos["def"]["char"])
                else:
                    r, e = sess.workspace_symbol(pos["name"].rsplit("_", 1)[0])
                ops.append(
                    {
                        "round": rnd,
                        "session": sid,
                        "kind": kind,
                        "latency_s": round(time.monotonic() - t0, 4),
                        "ok": r is not None,
                        "timed_out": e["timed_out"],
                    }
                )
        cell["ops"] = ops
        summary = {}
        for kind in kinds:
            lats = [o["latency_s"] for o in ops if o["kind"] == kind and o["ok"]]
            summary[kind] = {
                "n": len(lats),
                "p50_s": pct(lats, 50),
                "p95_s": pct(lats, 95),
                "max_s": pct(lats, 100),
            }
        summary["stalls_gt1s"] = sum(len(s.stall_events) for s in sessions)
        summary["first_result_serial_s"] = [r.get("first_result_serial_s") for r in setup]
        summary["attach_s"] = [r.get("attach_s") for r in setup]
        cell["warm_summary"] = summary
        for sid, sess in enumerate(sessions):
            for p in plans[sid][1]:
                try:
                    sess.did_close(self._abs(p["rel"]))
                except Exception:
                    pass
            sess.shutdown()
        cell["rss_after_kb"] = self.daemon.rss_now_kb()
        cell["wall_s"] = round(time.time() - cell["t_start"], 1)
        return cell

    # ---- manifest real-root probe (does a real worktree root fork the world?) ----

    def run_manifest_real_root(self) -> dict:
        rec = {"probe": "manifest_real_root"}
        manifest = (
            read_subject(self.subject_root, "Cargo.toml") + "\n[features]\nbench_realroot = []\n"
        )
        rel = self.disjoint_files[0][0]
        text, pos = make_variant(read_subject(self.subject_root, rel), "realroot", 99)
        wt_path = self.wt.create(
            "manifest-realroot", {"Cargo.toml": manifest, rel: text}, "bench: manifest change"
        )
        st = self.daemon.status()
        rec["workspaces_before"] = st.get("workspaces")
        rec["rss_before_kb"] = self.daemon.rss_now_kb()
        sess = self._spawn("manifest-realroot", root=wt_path)
        t_spawn = time.monotonic()
        init, entry = sess.initialize(wt_path, timeout=120)
        rec["attach_s"] = entry["latency_s"]
        qpath = self._abs(rel, root=wt_path)
        t0 = time.monotonic()
        first, attempts = None, 0
        while time.monotonic() - t0 < 600:
            attempts += 1
            r, e = sess.document_symbol(qpath, timeout=600)
            if r and _symbols_contain(r, pos["name"]):
                first = time.monotonic() - t0
                break
            time.sleep(1.0)
        rec["first_result_s"] = round(first, 4) if first else None
        rec["first_result_attempts"] = attempts
        r, e = sess.definition(qpath, pos["call"]["line"], pos["call"]["char"])
        rec["definition_latency_s"] = e["latency_s"]
        rec["definition_ok"] = r is not None
        st = self.daemon.status()
        rec["workspaces_after"] = st.get("workspaces")
        rec["rss_after_kb"] = self.daemon.rss_now_kb()
        rec["total_from_spawn_s"] = round(time.monotonic() - t_spawn, 2)
        sess.shutdown()
        return rec

    # ---- scenario driver ----
    #
    # Every cell gets a FRESH daemon: overlay session churn degrades daemon
    # state (probe evidence: overlay installs silently fail after a prior
    # overlay session detaches), so sequential cells on one daemon would
    # measure a poisoned world. Cold warm-up is therefore a per-cell metric.

    def _start_world(self, tag: str) -> dict:
        self.daemon.fresh_start(os.path.join(self.logs_dir, f"daemon-{tag}.log"))
        keeper = self._spawn(f"keeper-{tag}")
        t_spawn = keeper.t_spawn
        meta = {}
        init, entry = keeper.initialize(self.subject_root, timeout=120)
        meta["keeper_attach_s"] = entry["latency_s"]
        r, e = keeper.workspace_symbol("bench_cold_probe_nonexistent", timeout=600)
        meta["cold_warm_s"] = round(time.monotonic() - t_spawn, 2)
        meta["cold_warm_probe_ok"] = r is not None
        probes = []
        for _ in range(2):
            r, e = keeper.workspace_symbol("bench_cold_probe_nonexistent", timeout=120)
            probes.append(e["latency_s"])
        meta["keeper_warm_probe_s"] = probes
        meta["rss_after_warm_kb"] = self.daemon.rss_now_kb()
        meta["_keeper"] = keeper
        return meta

    def _end_world(self, keeper, retention_marks=(15,)) -> dict:
        out = {}
        try:
            keeper.shutdown()
        except Exception:
            pass
        retention = []
        t0 = time.monotonic()
        for mark in retention_marks:
            while time.monotonic() - t0 < mark:
                time.sleep(1)
            st = self.daemon.status()
            retention.append(
                {
                    "after_close_s": mark,
                    "rss_kb": self.daemon.rss_now_kb(),
                    "workspaces": st.get("workspaces"),
                    "client_sessions": st.get("client_sessions"),
                }
            )
        out["retention"] = retention
        out["daemon_stop"] = self.daemon.stop()
        out["rss_timeline"] = self.daemon.rss_timeline
        out["status_timeline"] = self.daemon.status_timeline
        return out

    def run_scenario(self, scen: str) -> dict:
        out = {"scenario": scen, "t_start": time.time(), "config": RA_CONFIG, "cells": []}
        self.prepare_scenario_worktrees(scen)
        out["worktree_times"] = [w for w in self.worktree_times if w["name"].startswith(scen[:7])]

        for idx, n in enumerate(self.cells):
            last = idx == len(self.cells) - 1
            world = self._start_world(f"{scen}-{n}")
            keeper = world.pop("_keeper")
            cell = self.run_storm_cell(n) if scen == "storm" else self.run_cell(scen, n)
            cell.update(world)
            cell.update(self._end_world(keeper, retention_marks=(10, 30, 60) if last else (15,)))
            out["cells"].append(cell)
            self._write(scen, out)  # checkpoint after every cell

        if scen == "manifest":
            world = self._start_world("manifest-realroot")
            keeper = world.pop("_keeper")
            probe = self.run_manifest_real_root()
            probe["world"] = world
            probe.update(self._end_world(keeper, retention_marks=(15,)))
            probe.pop("rss_timeline", None)
            probe.pop("status_timeline", None)
            out["real_root"] = probe
            self._write(scen, out)

        out["stall_samples"] = self.daemon.sample_log
        out["wall_s"] = round(time.time() - out["t_start"], 1)
        self._write(scen, out)
        return out

    def _write(self, scen: str, data: dict):
        with open(os.path.join(self.out_dir, f"{scen}.json"), "w") as f:
            json.dump(data, f, indent=1)


def _symbols_contain(symbols, name: str) -> bool:
    if not isinstance(symbols, list):
        return False
    for s in symbols:
        if name in (s.get("name") or ""):
            return True
        if _symbols_contain(s.get("children") or [], name):
            return True
    return False
