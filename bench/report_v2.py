#!/usr/bin/env python3
"""Generate REPORT.md for the v2 bench gate from bench/results/v2/**.

Emits phase tables + the pain-bucket decomposition inputs. The `## Verdict`
section is preserved across regenerations.
"""

import argparse
import glob
import json
import os
import re

QT = ["definition", "references", "rename", "wsymbol"]
GLUE_FRAMES = [
    "rebuild_overlay_inputs", "sync_session_overlay", "prepare_session_overlay_files",
    "apply_change", "set_crate_graph", "prime_caches", "symbol_index", "crate_symbols",
    "Cancelled", "salsa",
]


def ms(s):
    if s is None:
        return "—"
    return f"{s * 1000:.0f}ms" if s < 10 else f"{s:.1f}s"


def gib(kb):
    return f"{kb / 2**20:.2f}" if kb else "—"


def rng(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "—"
    lo, hi = min(vals), max(vals)
    return ms(lo) if hi <= lo * 1.2 else f"{ms(lo)}–{ms(hi)}"


def load_phase1(base):
    out = {}
    for d in sorted(glob.glob(os.path.join(base, "phase1-*"))):
        name = os.path.basename(d)[len("phase1-"):]
        scens = {}
        for f in glob.glob(os.path.join(d, "*.json")):
            key = os.path.basename(f)[:-5]
            if key == "meta":
                continue
            with open(f) as fh:
                scens[key] = json.load(fh)
        meta = {}
        mp = os.path.join(d, "meta.json")
        if os.path.exists(mp):
            meta = json.load(open(mp))
        out[name] = {"meta": meta, "scens": scens}
    return out


LEAD_COUNT = re.compile(r"^[\s+!:|]*(\d+) ")


def sample_attribution(samples_dirs):
    rows = []
    for sd in samples_dirs:
        for fn in sorted(glob.glob(os.path.join(sd, "*.txt"))):
            counts = {}
            with open(fn, errors="replace") as fh:
                for line in fh:
                    if "Binary Images" in line:
                        break
                    hit = next((fr for fr in GLUE_FRAMES if fr in line), None)
                    if hit is None:
                        continue
                    m = LEAD_COUNT.match(line)
                    if m:
                        n = int(m.group(1))
                        if n >= 50 and n > counts.get(hit, 0):
                            counts[hit] = n
            if counts:
                rows.append({"file": os.path.basename(fn),
                             "frames": dict(sorted(counts.items(), key=lambda kv: -kv[1])[:4])})
    return rows


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=os.path.join(here, "results", "v2"))
    ap.add_argument("--out", default=os.path.join(here, "REPORT.md"))
    args = ap.parse_args()

    old_verdict = ""
    if os.path.exists(args.out):
        old = open(args.out).read()
        if "## Verdict" in old:
            old_verdict = old[old.index("## Verdict"):]

    p1 = load_phase1(args.results)
    p2 = {}
    for f in glob.glob(os.path.join(args.results, "phase2-*", "live.json")):
        p2[os.path.basename(os.path.dirname(f))[len("phase2-"):]] = json.load(open(f))
    p3 = {}
    for f in glob.glob(os.path.join(args.results, "phase3-*", "phase3.json")):
        p3[os.path.basename(os.path.dirname(f))[len("phase3-"):]] = json.load(open(f))

    L = ["# analyzed bench gate v2 — fleet multi-worktree agent load\n"]
    any_meta = next(iter(p1.values()), {}).get("meta", {})
    L.append(f"- analyzed: `{any_meta.get('analyzed_version')}` @ `{str(any_meta.get('analyzed_rev'))[:9]}`; "
             f"rustc `{any_meta.get('rustc')}`")
    L.append(f"- session config: checkOnSave OFF, buildScripts OFF, procMacro OFF, watcher=server "
             f"(semantic completeness understated: no derive/proc-macro expansions, no OUT_DIR code)")
    L.append(f"- watchdog {any_meta.get('watchdog_gib')} GiB, daemon under nice 10\n")

    # ---- phase 1 ----
    L.append("## Phase 1 — control matrix (quiet clones, REAL diffs)\n")
    L.append("Diffs are replayed from each subject's own history: disjoint = pre-images of "
             "files real commits touched; same_file = divergent historical versions of the "
             "hottest file; distant_branch = real branch diffs vs merge-base.\n")
    L.append("| subject | scenario | N | cold | attach | ready | first result | def p50/p95 "
             "| refs p50/p95 | rename p50/p95 | ws-sym p50/p95 | correct/gap/viol/err | stalls | ab | RSS pk/ret GiB |")
    L.append("|---|---|--:|---|---|---|---|---|---|---|---|---|--:|--:|---|")
    for name, data in p1.items():
        for scen, d in sorted(data["scens"].items()):
            for cell in d.get("cells", []):
                s = cell.get("warm_summary", {})
                sc = s.get("score", {})
                score = f"{sc.get('correct',0)}/{sc.get('gap',0)}/{sc.get('violation',0)}/{sc.get('error',0)}" if sc else "—"
                if cell.get("mode") == "round-robin":
                    qc = [f"{ms((s.get(k) or {}).get('p50_s'))}/{ms((s.get(k) or {}).get('p95_s'))}"
                          for k in ("definition", "references", "edit_then_def", "wsymbol")]
                    first = rng(s.get("first_result_serial_s", []))
                    ready = "—"
                else:
                    qc = [f"{ms((s.get(k) or {}).get('p50_s'))}/{ms((s.get(k) or {}).get('p95_s'))}" for k in QT]
                    first = rng(s.get("first_result_s", []))
                    ready = rng(s.get("ready_s", []))
                ret = (cell.get("retention") or [{}])[-1].get("rss_kb")
                if cell.get("rss_peak_kb") is None and cell.get("rss_timeline"):
                    cell["rss_peak_kb"] = max(r["rss_kb"] for r in cell["rss_timeline"])
                L.append(
                    f"| {name} | {scen} | {cell.get('sessions')} | {ms(cell.get('cold_warm_s'))} "
                    f"| {rng(s.get('attach_s', []))} | {ready} | {first} "
                    f"| {qc[0]} | {qc[1]} | {qc[2]} | {qc[3]} | {score} "
                    f"| {s.get('stalls_gt1s', '—')} | {len(s.get('aborted', []))} "
                    f"| {gib(cell.get('rss_peak_kb'))}/{gib(ret)} |"
                )
    L.append("")
    for name, data in p1.items():
        rr = data["scens"].get("manifest", {}).get("real_root")
        if rr:
            L.append(f"**Manifest real-root probe ({name})**: committed change to `{rr.get('manifest')}` "
                     f"in its own worktree; attach {ms(rr.get('attach_s'))}, first symbols "
                     f"{ms(rr.get('first_result_s'))}, definition ok={rr.get('definition_ok')} "
                     f"({ms(rr.get('definition_latency_s'))}); workspaces {rr.get('workspaces_before')}→"
                     f"{rr.get('workspaces_after')}; RSS {gib(rr.get('rss_before_kb'))}→"
                     f"{gib(rr.get('rss_after_kb'))} GiB\n")

    # ---- phase 2 ----
    for name, d in p2.items():
        L.append(f"## Phase 2 — live chaos on {name} (events only, no verdict percentiles)\n")
        kinds = {}
        for e in d.get("events", []):
            kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
        L.append(f"- {d.get('sessions')} roaming sessions x {d.get('duration_s')}s over "
                 f"{d.get('worktrees_available')} live worktrees; {d.get('ops_total')} ops; world warm {ms(d.get('cold_warm_s'))}")
        L.append(f"- events: {json.dumps(kinds)}")
        ok_ops = [o for o in d.get("ops", []) if o.get("op") in ("definition", "references", "documentSymbol")]
        empt = sum(1 for o in ok_ops if o.get("empty"))
        errs = [o for o in ok_ops if o.get("err")]
        L.append(f"- read ops: {len(ok_ops)}; semantically empty: {empt}; errored: {len(errs)}")
        fresh = [f for f in d.get("freshness", []) if f.get("visible_after_s") is not None]
        if d.get("freshness"):
            L.append(f"- disk-change visibility: {len(fresh)}/{len(d.get('freshness'))} measured, "
                     f"{[f['visible_after_s'] for f in fresh][:10]} s (target <2s)")
        integ = all(r["unchanged"] for r in d.get("git_integrity", []))
        L.append(f"- read-only integrity: {'CLEAN — no live worktree git state changed' if integ else 'VIOLATED — see events'}")
        # attribution: stalls vs box activity
        stall_ts = [e["t"] for e in d.get("events", []) if e["kind"] == "stall"]
        box = d.get("box_timeline", [])
        busy = 0
        for t in stall_ts:
            near = [b for b in box if abs(b["t"] - t) <= 3]
            if any((b.get("cargo", 0) + b.get("rustc", 0)) > 0 or b.get("load1", 0) > 20 for b in near):
                busy += 1
        if stall_ts:
            L.append(f"- stall attribution: {busy}/{len(stall_ts)} stalls coincided with box "
                     f"build activity (cargo/rustc running or load1>20) → bucket 5; the rest → glue\n")
        else:
            L.append("")

    # ---- phase 3 ----
    for name, d in p3.items():
        L.append(f"## Phase 3 — worktree storm on {name}\n")
        lad = d.get("ladder", {})
        if lad:
            L.append(f"- worktrees: {lad.get('available')} alive, {lad.get('missing')} listed-but-missing (prunable churn)")
            L.append(f"- distinct merge-base groups among alive worktrees: {len(lad.get('merge_base_groups', {}))} "
                     f"(world-pool sizing input for fork item 2)")
            L.append(f"- keeper cold warm: {ms(lad.get('cold_warm_s'))}; stopped: {lad.get('stopped', 'completed')}")
            L.append("\n| ladder mark | attach (that wt) | ready | RSS GiB | workspaces |")
            L.append("|--:|---|---|---|--:|")
            att = lad.get("attaches", [])
            for m in lad.get("marks", []):
                a = att[m["n"] - 1] if m["n"] - 1 < len(att) else {}
                L.append(f"| {m['n']} | {ms(a.get('total_s'))} | {ms(a.get('ready_s'))} "
                         f"| {gib(m['rss_kb'])} | {m.get('workspaces')} |")
            if att:
                n = len(att)
                first_q, last_q = att[: max(1, n // 4)], att[-max(1, n // 4):]
                L.append(f"\n- attach growth: first-quartile median total "
                         f"{ms(sorted(a['total_s'] for a in first_q)[len(first_q)//2])} → last-quartile "
                         f"{ms(sorted(a['total_s'] for a in last_q)[len(last_q)//2])} over {n} attaches")
                fails = [a for a in att if not a.get("ready_ok")]
                L.append(f"- ready failures during ladder: {len(fails)}/{n}")
            ev = lad.get("eviction_after_worktree_close", [])
            if ev:
                L.append(f"- after closing ALL worktree sessions (keeper kept): "
                         + "; ".join(f"+{e['after_close_s']}s: {gib(e['rss_kb'])} GiB ws={e['workspaces']}" for e in ev))
            ak = lad.get("after_keeper_close")
            if ak:
                L.append(f"- after keeper close too: {gib(ak['rss_kb'])} GiB ws={ak.get('workspaces')}")
            stop = lad.get("daemon_stop", {})
            L.append(f"- teardown: stop_ok={stop.get('stop_ok')} orphans="
                     f"{len(stop.get('orphans', [])) + len(stop.get('orphaned_children', []))}\n")
        conc = d.get("concurrent", {})
        if conc:
            oks = [a for a in conc.get("attaches", []) if a.get("ready_ok")]
            tot = [a["total_s"] for a in conc.get("attaches", []) if a.get("total_s")]
            L.append(f"**Concurrent attach** k={conc.get('k')}: wall {conc.get('wall_s')}s, "
                     f"ready ok {len(oks)}/{len(conc.get('attaches', []))}, per-session total {rng(tot)}\n")
        iso = d.get("isolation", {})
        if iso:
            L.append(f"**Cross-repo isolation** (storm {name}, probe others; "
                     f"{iso.get('workspaces_loaded')} workspaces loaded, storm ops {iso.get('storm_ops')}):\n")
            L.append("| probe repo | baseline p50/p95 | during storm p50/p95 |")
            L.append("|---|---|---|")
            tags = sorted(set(k.rsplit("_", 1)[0].replace("_baseline", "").replace("_during", "")
                              for k in iso.get("probes", {})))
            for k, v in iso.get("probes", {}).items():
                if k.endswith("_baseline"):
                    tag = k[: -len("_baseline")]
                    dur = iso["probes"].get(f"{tag}_during_storm", {})
                    L.append(f"| {tag} | {ms(v.get('p50_s'))}/{ms(v.get('p95_s'))} "
                             f"| {ms(dur.get('p50_s'))}/{ms(dur.get('p95_s'))} |")
            L.append("")
        rep = d.get("repro", {})
        if rep:
            ws = rep.get("workspace_symbol", {})
            L.append(f"**Resolve-timeout repro** (`{rep.get('symbol')}`): cold warm {ms(rep.get('cold_warm_s'))}; "
                     f"workspace/symbol: {ms(ws.get('latency_s'))} timed_out={ws.get('timed_out')} "
                     f"hits={ws.get('hits')}; definition: {ms((rep.get('definition') or {}).get('latency_s'))}; "
                     f"references: {ms((rep.get('references') or {}).get('latency_s'))} "
                     f"n={(rep.get('references') or {}).get('n')}\n")
        integ = d.get("git_integrity", [])
        if integ:
            ok = all(r["unchanged"] for r in integ)
            L.append(f"- read-only integrity over {len(integ)} touched roots: "
                     f"{'CLEAN' if ok else 'VIOLATED'}\n")

    # ---- stall attribution across phases ----
    rows = sample_attribution(glob.glob(os.path.join(args.results, "*", "samples")))
    if rows:
        salsa_dom = sum(1 for r in rows if max(r["frames"], key=r["frames"].get) in ("salsa", "Cancelled", "symbol_index", "crate_symbols"))
        glue_present = sum(1 for r in rows if any(f in r["frames"] for f in ("rebuild_overlay_inputs", "sync_session_overlay", "apply_change", "set_crate_graph")))
        L.append("## Stall stack attribution (all phases)\n")
        L.append(f"- {len(rows)} samples with heavy frames; salsa cancel/recompute-dominated: {salsa_dom}; "
                 f"overlay-glue frames present: {glue_present}")
        L.append("- interpretation unchanged from v1: the glue's global input rebuild is cheap itself "
                 "but triggers the salsa invalidation/cancellation wave that IS the stall\n")

    body = "\n".join(L) + "\n"
    body += old_verdict if old_verdict else "## Verdict\n\n_(to be written)_\n"
    with open(args.out, "w") as f:
        f.write(body)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
