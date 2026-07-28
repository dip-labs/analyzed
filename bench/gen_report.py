#!/usr/bin/env python3
"""Generate bench/REPORT.md tables from bench/results/*.json.

Emits the data tables and per-target pass/fail. The verdict prose section at
the bottom is kept if the file already contains a `## Verdict` section.
"""

import argparse
import json
import os
import re

SCENARIOS = ["disjoint", "disjoint_rooted", "same_file", "distant_branch", "manifest", "storm"]
QTYPES = ["definition", "references", "rename", "wsymbol"]
GLUE_FRAMES = [
    "rebuild_overlay_inputs",
    "sync_session_overlay",
    "prepare_session_overlay_files",
    "apply_change",
    "set_crate_graph",
    "prime_caches",
    "recone_session_overlays",
    "salsa",
]


def load(out_dir):
    data = {}
    for scen in SCENARIOS:
        path = os.path.join(out_dir, f"{scen}.json")
        if os.path.exists(path):
            with open(path) as f:
                data[scen] = json.load(f)
    meta_path = os.path.join(out_dir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)
    return meta, data


def gib(kb):
    return f"{kb / 1024 / 1024:.2f}" if kb is not None else "—"


def ms(s):
    if s is None:
        return "—"
    return f"{s * 1000:.0f}ms" if s < 10 else f"{s:.1f}s"


def med(vals):
    vals = sorted(v for v in vals if v is not None)
    return vals[len(vals) // 2] if vals else None


def mx(vals):
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None


def fmt_range(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "—"
    m, M = min(vals), max(vals)
    return f"{ms(m)}–{ms(M)}" if M > m * 1.2 else ms(med(vals))


def sample_attribution(out_dir):
    samples_dir = os.path.join(out_dir, "samples")
    rows = []
    if not os.path.isdir(samples_dir):
        return rows
    for fn in sorted(os.listdir(samples_dir)):
        if not fn.endswith(".txt"):
            continue
        try:
            with open(os.path.join(samples_dir, fn), errors="replace") as f:
                text = f.read()
        except OSError:
            continue
        counts = {}
        for frame in GLUE_FRAMES:
            hits = [int(m.group(1)) for m in re.finditer(r"(\d+)\s+\S*" + frame, text)]
            if hits:
                counts[frame] = max(hits)
        total = re.search(r"(\d+) samples", text)
        rows.append(
            {
                "file": fn,
                "total": int(total.group(1)) if total else None,
                "frames": dict(sorted(counts.items(), key=lambda kv: -kv[1])[:4]),
            }
        )
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "REPORT.md"))
    args = ap.parse_args()
    meta, data = load(args.results)

    old_verdict = ""
    if os.path.exists(args.out):
        with open(args.out) as f:
            old = f.read()
        if "## Verdict" in old:
            old_verdict = old[old.index("## Verdict"):]

    L = []
    L.append("# analyzed bench gate — results\n")
    L.append(f"- analyzed: `{meta.get('analyzed_version')}` @ `{meta.get('analyzed_rev', '')[:9]}`")
    L.append(f"- subject: `{meta.get('subject_rev', '')[:9]}`")
    host = meta.get("host", {})
    L.append(
        f"- host: {host.get('cpu')} / {host.get('mem_bytes', 0) // 2**30} GiB; "
        f"cells {meta.get('cells')}; iters {meta.get('iters')}; "
        f"distant files {meta.get('distant_files')}\n"
    )

    # cold start / keeper — one fresh daemon per cell
    L.append("## Keeper world (fresh daemon per cell)\n")
    L.append("| cell | keeper attach | cold warm (spawn→first ws-symbol) | warm probe after | RSS after warm, GiB |")
    L.append("|---|---|---|---|---|")
    for scen, d in data.items():
        for cell in d.get("cells", []):
            L.append(
                f"| {scen}×{cell.get('sessions')} | {ms(cell.get('keeper_attach_s'))} "
                f"| {ms(cell.get('cold_warm_s'))} "
                f"| {fmt_range(cell.get('keeper_warm_probe_s', []))} "
                f"| {gib(cell.get('rss_after_warm_kb'))} |"
            )
    L.append("")

    # cells
    L.append("## Matrix cells\n")
    L.append(
        "Warm p50/p95 mixes install-phase interference and steady state; see storm for "
        "established-view steady state. For `storm` rows the rename column is edit→def "
        "(didChange + definition, the view-flip price) and first result is from SERIAL installs.\n"
    )
    L.append(
        "| scenario | N | attach | ready | first result | def p50/p95 | refs p50/p95 "
        "| rename p50/p95 | ws-sym p50/p95 | stalls>1s | viol | aborted | RSS peak/after GiB | wall |"
    )
    L.append("|---|--:|---|---|---|---|---|---|---|--:|--:|--:|---|--:|")
    for scen, d in data.items():
        for cell in d.get("cells", []):
            s = cell.get("warm_summary", {})
            if cell.get("mode") == "round-robin":
                qcols = []
                for k in ("definition", "references", "edit_then_def", "wsymbol"):
                    q = s.get(k, {})
                    qcols.append(f"{ms(q.get('p50_s'))}/{ms(q.get('p95_s'))}")
                attach = fmt_range(s.get("attach_s", []))
                first = fmt_range(s.get("first_result_serial_s", []))
                ready = "—"
                aborted = 1 if cell.get("cell_aborted") else 0
                viol = "—"
            else:
                qcols = []
                for k in QTYPES:
                    q = s.get(k, {})
                    to = f" ({q.get('timeouts')}TO)" if q.get("timeouts") else ""
                    qcols.append(f"{ms(q.get('p50_s'))}/{ms(q.get('p95_s'))}{to}")
                attach = fmt_range(s.get("attach_s", []))
                ready = fmt_range(s.get("ready_s", []))
                first = fmt_range(s.get("first_result_s", []))
                aborted = len(s.get("aborted", []))
                viol = s.get("violations", 0)
            L.append(
                f"| {scen} | {cell.get('sessions')} | {attach} | {ready} | {first} "
                f"| {qcols[0]} | {qcols[1]} | {qcols[2]} | {qcols[3]} "
                f"| {s.get('stalls_gt1s', '—')} | {viol} | {aborted} "
                f"| {gib(cell.get('rss_peak_kb'))}/{gib(cell.get('rss_after_kb'))} "
                f"| {cell.get('wall_s')}s |"
            )
    L.append("")

    # manifest real root
    rr = data.get("manifest", {}).get("real_root")
    if rr:
        L.append("## Manifest real-worktree-root probe\n")
        L.append(
            f"- attach {ms(rr.get('attach_s'))}; first result {ms(rr.get('first_result_s'))} "
            f"({rr.get('first_result_attempts')} probes); definition after: {ms(rr.get('definition_latency_s'))}"
        )
        L.append(
            f"- workspaces {rr.get('workspaces_before')} → {rr.get('workspaces_after')}; "
            f"RSS {gib(rr.get('rss_before_kb'))} → {gib(rr.get('rss_after_kb'))} GiB\n"
        )

    # retention — per cell (daemon restarted per cell)
    L.append("## RSS retention after ALL sessions closed (per cell)\n")
    L.append("| cell | RSS peak GiB | retention GiB (ws) at marks | stop ok | orphans |")
    L.append("|---|---|---|---|--:|")
    for scen, d in data.items():
        for cell in d.get("cells", []):
            if cell.get("rss_peak_kb") is None and cell.get("rss_timeline"):
                cell["rss_peak_kb"] = max(r["rss_kb"] for r in cell["rss_timeline"])
            marks = ", ".join(
                f"+{r['after_close_s']}s: {gib(r.get('rss_kb'))} ({r.get('workspaces', '—')})"
                for r in cell.get("retention", [])
            )
            stop = cell.get("daemon_stop", {})
            orphans = len(stop.get("orphans", [])) + len(stop.get("orphaned_children", []))
            L.append(
                f"| {scen}×{cell.get('sessions')} | {gib(cell.get('rss_peak_kb'))} | {marks or '—'} "
                f"| {stop.get('stop_ok')}/{stop.get('exited')} | {orphans} |"
            )
    L.append("")

    # stall attribution
    rows = sample_attribution(args.results)
    if rows:
        L.append("## Stall stack samples (daemon, captured while a request was >1s outstanding)\n")
        L.append("| sample | dominant glue/salsa frames (max sample count) |")
        L.append("|---|---|")
        for r in rows:
            frames = ", ".join(f"`{k}`:{v}" for k, v in r["frames"].items()) or "no glue frames matched"
            L.append(f"| {r['file']} | {frames} |")
        L.append("")

    # targets
    L.append("## Acceptance targets\n")
    L.append("| target | result |")
    L.append("|---|---|")
    worst_p95 = {}
    for scen, d in data.items():
        for cell in d.get("cells", []):
            s = cell.get("warm_summary", {})
            for k in QTYPES + ["edit_then_def"]:
                q = s.get(k, {})
                if q.get("p95_s") is not None:
                    key = (scen, cell.get("sessions"))
                    worst_p95[key] = max(worst_p95.get(key, 0), q["p95_s"])
    fails = {k: v for k, v in worst_p95.items() if v > 0.150}
    L.append(
        f"| warm p95 < 150 ms | {'FAIL in ' + str(len(fails)) + '/' + str(len(worst_p95)) + ' cells: ' + ', '.join(f'{s}×{n}={ms(v)}' for (s, n), v in sorted(fails.items(), key=lambda kv: -kv[1])[:6]) if fails else 'PASS in all cells'} |"
    )
    attaches = [
        v
        for d in data.values()
        for cell in d.get("cells", [])
        for v in (cell.get("warm_summary", {}).get("attach_s") or [])
        if v is not None
    ]
    L.append(f"| attach = milliseconds | max {ms(mx(attaches))} |")
    rss_end = {}
    for scen, d in data.items():
        for cell in d.get("cells", []):
            ret = cell.get("retention") or [{}]
            rss_end[f"{scen}×{cell.get('sessions')}"] = ret[-1].get("rss_kb")
    worst_ret = sorted(rss_end.items(), key=lambda kv: -(kv[1] or 0))[:4]
    L.append(
        "| RSS released after close | worst retained: "
        + "; ".join(f"{s}: {gib(v)} GiB" for s, v in worst_ret)
        + " |"
    )
    orphan_total = sum(
        len(cell.get("daemon_stop", {}).get("orphans", []))
        + len(cell.get("daemon_stop", {}).get("orphaned_children", []))
        for d in data.values()
        for cell in d.get("cells", [])
    )
    L.append(f"| zero orphan processes | {orphan_total} orphans across scenarios |")
    L.append("")

    body = "\n".join(L) + "\n"
    if old_verdict:
        body += old_verdict
    else:
        body += "## Verdict\n\n_(to be written from the data above)_\n"
    with open(args.out, "w") as f:
        f.write(body)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
