"""Generic v2 cell runner: N concurrent sessions over REAL-diff overlay plans.

A plan is a list of {rel, text, pos|None} where exactly one entry carries pos
= {"name": fn_name, "def": {"line", "char"}} — a real `fn` definition in the
overlay text (queries run against real symbols, not synthetic markers).

Result classes per query:
  correct    — expected location/content came back
  gap        — semantically EMPTY answer (0.4.2 overlay-cone gap class)
  violation  — wrong location, foreign version, or stale-view position error
"""

from __future__ import annotations

import os
import threading
import time

from .daemon import DaemonController
from .lsp import LspSession
from .replay import fn_targets


def pct(values, p):
    if not values:
        return None
    vs = sorted(values)
    idx = min(len(vs) - 1, max(0, round(p / 100 * len(vs) + 0.5) - 1))
    return round(vs[idx], 4)


def fn_vector(text):
    return [(name, line) for name, line, _ in fn_targets(text)]


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


class V2Runner:
    def __init__(
        self,
        binary: str,
        repo: str,
        out_dir: str,
        tmpdir: str,
        config: dict,
        iters: int = 8,
        cell_budget_s: float = 1800.0,
        watchdog_gib: float = 24.0,
        nice_level: int = 10,
    ):
        self.binary = binary
        self.repo = repo
        self.out_dir = out_dir
        self.config = config
        self.iters = iters
        self.cell_budget_s = cell_budget_s
        self.logs_dir = os.path.join(out_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)
        self._ctx = None
        self.daemon = DaemonController(
            binary,
            tmpdir,
            out_dir,
            nice_level=nice_level,
            watchdog_gib=watchdog_gib,
            watchdog_cb=self._on_watchdog,
        )

    def _on_watchdog(self, rss_kb):
        ctx = self._ctx
        if ctx is not None:
            ctx.trip(f"rss watchdog: {rss_kb / 2**20:.1f} GiB")

    def _spawn(self, name, root=None):
        return LspSession(
            self.binary,
            root or self.repo,
            self.daemon.env,
            name,
            stderr_path=os.path.join(self.logs_dir, f"{name}.stderr.log"),
            config=self.config,
            stall_cb=lambda s, p, e: self.daemon.stall_sample(
                f"{s.name}-{p.method.split('/')[-1]}"
            ),
        )

    # ---- world lifecycle (fresh daemon per cell) ----

    def start_world(self, tag, root=None):
        self.daemon.fresh_start(os.path.join(self.logs_dir, f"daemon-{tag}.log"))
        keeper = self._spawn(f"keeper-{tag}", root=root)
        t_spawn = keeper.t_spawn
        meta = {}
        init, entry = keeper.initialize(root or self.repo, timeout=180)
        meta["keeper_attach_s"] = entry["latency_s"]
        r, e = keeper.workspace_symbol("bench_cold_probe_nonexistent", timeout=900)
        meta["cold_warm_s"] = round(time.monotonic() - t_spawn, 2)
        meta["cold_warm_probe_ok"] = r is not None
        probes = []
        for _ in range(2):
            r, e = keeper.workspace_symbol("bench_cold_probe_nonexistent", timeout=180)
            probes.append(e["latency_s"])
        meta["keeper_warm_probe_s"] = probes
        meta["rss_after_warm_kb"] = self.daemon.rss_now_kb()
        meta["_keeper"] = keeper
        return meta

    def end_world(self, keeper, retention_marks=(15,)):
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

    # ---- queries + scoring ----

    def _one_query(self, sess, qtype, qpath, pos):
        d = pos["def"]
        if qtype == "definition":
            return sess.definition(qpath, d["line"], d["char"])
        if qtype == "references":
            return sess.references(qpath, d["line"], d["char"])
        if qtype == "rename":
            return sess.rename(qpath, d["line"], d["char"], pos["name"] + "_rnprev")
        return sess.workspace_symbol(pos["name"])

    def _score(self, qtype, result, entry, qpath, pos):
        """Returns 'correct' | 'gap' | 'violation' | 'error'."""
        err = entry.get("error")
        if err:
            if "Invalid offset" in str(err):
                return "violation"  # stale view: position validated against wrong text
            if "No references found" in str(err):
                return "gap"  # same no-semantics class as an empty answer
            return "error"
        if result is None:
            # JSON-RPC null (definition/rename may return null) = no-semantics
            return "error" if entry.get("timed_out") else "gap"
        if qtype == "definition":
            locs = result if isinstance(result, list) else [result]
            if not locs:
                return "gap"
            for l in locs:
                if l.get("uri", "").endswith(qpath) and l.get("range", {}).get("start", {}).get(
                    "line"
                ) == pos["def"]["line"]:
                    return "correct"
            return "violation"
        if qtype == "references":
            return "correct" if isinstance(result, list) and len(result) >= 1 else "gap"
        if qtype == "rename":
            changes = (result or {}).get("changes") or (result or {}).get("documentChanges")
            return "correct" if changes else "gap"
        names_hit = any(
            (s.get("location") or {}).get("uri", "").endswith(qpath)
            or s.get("name") == pos["name"]
            for s in (result or [])
        )
        return "correct" if names_hit else "gap"

    # ---- session driver ----

    def _run_session(self, scen, sid, plan_entry, ready_rel, barrier, deadline, ctx, rec):
        plan = plan_entry["plan"]
        rec.update(
            {
                "session": sid,
                "label": plan_entry.get("label"),
                "phases": {},
                "warm": [],
                "score": {"correct": 0, "gap": 0, "violation": 0, "error": 0},
                "aborted": None,
            }
        )
        query = next(p for p in plan if p.get("pos"))
        qpath = os.path.join(self.repo, query["rel"])
        pos = query["pos"]
        want_last = (fn_vector(query["text"]) or [None])[-1]
        try:
            base_text = open(qpath, encoding="utf-8", errors="replace").read()
        except OSError:
            base_text = ""
        base_last = (fn_vector(base_text) or [None])[-1]
        discriminable = want_last is not None and want_last != base_last
        sess = None
        try:
            barrier.wait(timeout=180)
            sess = self._spawn(f"{scen}-s{sid}")
            ctx.register(sess)
            init, entry = sess.initialize(self.repo)
            rec["phases"]["attach_s"] = entry["latency_s"]
            if init is None:
                rec["aborted"] = "initialize failed"
                return
            t0 = time.monotonic()
            ready_path = os.path.join(self.repo, ready_rel)
            sess.did_open(ready_path, open(ready_path, encoding="utf-8", errors="replace").read())
            result, entry = sess.document_symbol(ready_path, timeout=240)
            rec["phases"]["ready_s"] = round(time.monotonic() - t0, 4)
            rec["phases"]["ready_ok"] = result is not None
            if result is None:
                rec["aborted"] = "readiness query failed"
                return
            if ctx.abort.is_set() or time.monotonic() > deadline:
                rec["aborted"] = ctx.abort_reason or "budget before overlay"
                return
            # server-space base snapshot of the query file BEFORE the overlay:
            # install detection compares server vector vs THIS (representation-
            # consistent), not vs a regex parse of the text
            r0, e0 = sess.document_symbol(qpath, timeout=240)
            server_base_vec = _sym_vector(r0) if r0 is not None else None
            t0 = time.monotonic()
            for p in plan:
                sess.did_open(os.path.join(self.repo, p["rel"]), p["text"])
            rec["phases"]["overlay_files"] = len(plan)
            rec["phases"]["discriminable"] = discriminable
            first, attempts, stale_base = None, 0, 0
            while (
                time.monotonic() - t0 < 420
                and time.monotonic() < deadline
                and not ctx.abort.is_set()
            ):
                attempts += 1
                result, entry = sess.document_symbol(qpath, timeout=420)
                if result is not None:
                    got = _sym_vector(result)
                    if (
                        not discriminable
                        or server_base_vec is None
                        or got != server_base_vec
                    ):
                        first = time.monotonic() - t0
                        got_last = got[-1] if got else None
                        rec["phases"]["version_match"] = (
                            None if not discriminable else got_last == want_last
                        )
                        break
                    stale_base += 1
                    if stale_base >= 40 and time.monotonic() - t0 > 30:
                        rec["aborted"] = "overlay ignored (server kept serving base view)"
                        rec["phases"]["first_result_attempts"] = attempts
                        return
                time.sleep(0.5)
            rec["phases"]["first_result_s"] = round(first, 4) if first else None
            rec["phases"]["first_result_attempts"] = attempts
            if first is None:
                rec["aborted"] = ctx.abort_reason or "no first overlay result in budget"
                return
            for _ in range(self.iters):
                if ctx.abort.is_set() or time.monotonic() > deadline:
                    rec["aborted"] = ctx.abort_reason or "budget during warm"
                    break
                for qtype in ("definition", "references", "rename", "wsymbol"):
                    r, e = self._one_query(sess, qtype, qpath, pos)
                    verdict = self._score(qtype, r, e, qpath, pos)
                    rec["score"][verdict] += 1
                    entry_rec = {
                        "q": qtype,
                        "latency_s": e["latency_s"],
                        "verdict": verdict,
                        "timed_out": e["timed_out"],
                    }
                    if e.get("error"):
                        entry_rec["error"] = str(e["error"])[:120]
                    rec["warm"].append(entry_rec)
        except threading.BrokenBarrierError:
            rec["aborted"] = "start barrier broken"
        except Exception as e:
            rec["aborted"] = f"exception: {e}"
        finally:
            if sess is not None:
                rec["stalls"] = list(sess.stall_events)
                if not ctx.abort.is_set():
                    for p in plan:
                        try:
                            sess.did_close(os.path.join(self.repo, p["rel"]))
                        except Exception:
                            pass
                    rec["teardown_clean"] = sess.shutdown()
                else:
                    sess.kill()
                    rec["teardown_clean"] = False

    # ---- cells ----

    def run_cell(self, scen, plans, n, ready_rel):
        cell = {"scenario": scen, "sessions": n, "t_start": time.time(), "cell_aborted": None}
        deadline = time.monotonic() + self.cell_budget_s
        ctx = CellContext()
        self._ctx = ctx
        barrier = threading.Barrier(n)
        records = [dict() for _ in range(n)]
        threads = [
            threading.Thread(
                target=self._run_session,
                args=(scen, i, plans[i % len(plans)], ready_rel, barrier, deadline, ctx, records[i]),
                name=f"drv-{scen}-{i}",
                daemon=True,
            )
            for i in range(n)
        ]
        cell["plan_reuse"] = n > len(plans)
        cell["rss_before_kb"] = self.daemon.rss_now_kb()
        for t in threads:
            t.start()
        while any(t.is_alive() for t in threads):
            if time.monotonic() > deadline + 480:
                ctx.trip("cell hard-deadline exceeded")
            for t in threads:
                t.join(timeout=1)
        self._ctx = None
        cell["cell_aborted"] = ctx.abort_reason
        cell["records"] = records
        cell["rss_peak_kb"] = max(
            (r["rss_kb"] for r in self.daemon.rss_timeline if r["t"] >= cell["t_start"]),
            default=None,
        )
        cell["wall_s"] = round(time.time() - cell["t_start"], 1)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 60:
            st = self.daemon.status()
            if (st.get("client_sessions") or 0) <= 1:
                break
            time.sleep(1)
        time.sleep(3)
        cell["rss_after_kb"] = self.daemon.rss_now_kb()
        cell["warm_summary"] = self.summarize(records)
        return cell

    def run_storm_cell(self, plans, n, rounds=15):
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
        sessions, qinfo, setup = [], [], []
        for sid in range(n):
            plan_entry = plans[sid % len(plans)]
            plan = plan_entry["plan"]
            rec = {"session": sid}
            sess = self._spawn(f"{scen}-s{sid}")
            init, entry = sess.initialize(self.repo)
            rec["attach_s"] = entry["latency_s"]
            query = next(p for p in plan if p.get("pos"))
            qpath = os.path.join(self.repo, query["rel"])
            t0 = time.monotonic()
            for p in plan:
                sess.did_open(os.path.join(self.repo, p["rel"]), p["text"])
            first = None
            while time.monotonic() - t0 < 420 and time.monotonic() < deadline:
                r, e = sess.document_symbol(qpath, timeout=420)
                if r is not None:
                    first = time.monotonic() - t0
                    break
                time.sleep(0.5)
            rec["first_result_serial_s"] = round(first, 4) if first else None
            sessions.append(sess)
            qinfo.append((query, qpath))
            setup.append(rec)
        cell["setup"] = setup
        ops, tick = [], 0
        kinds = ("definition", "references", "edit_then_def", "wsymbol")
        for rnd in range(rounds):
            if time.monotonic() > deadline:
                cell["cell_aborted"] = "budget during storm"
                break
            for sid, sess in enumerate(sessions):
                query, qpath = qinfo[sid]
                pos = query["pos"]
                kind = kinds[(rnd + sid) % len(kinds)]
                t0 = time.monotonic()
                if kind == "edit_then_def":
                    tick += 1
                    sess.did_change(qpath, query["text"] + f"\n// storm tick {tick}\n", version=tick + 1)
                    r, e = sess.definition(qpath, pos["def"]["line"], pos["def"]["char"])
                else:
                    r, e = self._one_query(sess, kind if kind != "edit_then_def" else "definition", qpath, pos)
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
            sess.shutdown()
        cell["rss_after_kb"] = self.daemon.rss_now_kb()
        cell["wall_s"] = round(time.time() - cell["t_start"], 1)
        return cell

    def summarize(self, records):
        out = {}
        for q in ("definition", "references", "rename", "wsymbol"):
            lats = [
                w["latency_s"]
                for r in records
                for w in r.get("warm", [])
                if w["q"] == q and not w["timed_out"]
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
        agg = {"correct": 0, "gap": 0, "violation": 0, "error": 0}
        for r in records:
            for k in agg:
                agg[k] += r.get("score", {}).get(k, 0)
        out["score"] = agg
        out["attach_s"] = [r.get("phases", {}).get("attach_s") for r in records]
        out["ready_s"] = [r.get("phases", {}).get("ready_s") for r in records]
        out["first_result_s"] = [r.get("phases", {}).get("first_result_s") for r in records]
        out["aborted"] = [r.get("aborted") for r in records if r.get("aborted")]
        out["stalls_gt1s"] = sum(len(r.get("stalls", [])) for r in records)
        return out


def _sym_vector(symbols, acc=None):
    """documentSymbol response -> [(fn_name, line)]; Function=12, Method=6."""
    if acc is None:
        acc = []
    for s in symbols or []:
        if s.get("kind") in (6, 12):
            rng = s.get("selectionRange") or s.get("range") or {}
            acc.append((s.get("name", "").split("(")[0], rng.get("start", {}).get("line")))
        _sym_vector(s.get("children") or [], acc)
    return acc
