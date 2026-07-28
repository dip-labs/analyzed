#!/usr/bin/env python3
"""Phase 1 — control matrix on a QUIET subject clone, real diffs.

  python3 bench/phase1.py --repo /path/to/clone [--sessions 1,5,20] \
      [--scenarios a,b,c,d,e] [--branches br1,br2] [--allow-worktrees]

Scenarios (diffs are replayed from the subject's own history, not synthetic):
  a disjoint        overlays = pre-images of files real recent commits touched
  b same_file       divergent HISTORICAL versions of the hottest file
  c distant_branch  overlays = real branch diffs vs merge-base
  d manifest        Cargo.toml overlay (+ committed-manifest worktree probe)
  e storm           round-robin query/edit interleave over commit plans

The subject must be a bench-owned clone for d's real-root probe
(--allow-worktrees creates a worktree IN THE SUBJECT).
"""

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness.config import RA_CONFIG_V2  # noqa: E402
from harness.replay import (  # noqa: E402
    branch_overlay_plan,
    file_versions,
    head_text,
    hot_file,
    pick_query_target,
    rs_files,
    session_commit_plans,
)
from harness.runner import V2Runner  # noqa: E402

SCEN_NAMES = {"a": "disjoint", "b": "same_file", "c": "distant_branch", "d": "manifest", "e": "storm"}
DEFAULT_BRANCHES: list[str] = []  # pass --branches for the distant_branch scenario


def existing_branches(repo, wanted):
    out = subprocess.run(
        ["git", "-C", repo, "branch", "--list", "--format=%(refname:short)"],
        capture_output=True,
        text=True,
    ).stdout.split()
    return [b for b in wanted if b in out]


def build_plans(repo, scen, max_n, branches):
    if scen in ("disjoint", "storm"):
        plans = session_commit_plans(repo, max_n)
        for p in plans:
            p["label"] = p.pop("commit")[:9]
        return plans
    if scen == "same_file":
        hf = hot_file(repo)
        versions = file_versions(repo, hf, k=max_n)
        return [
            {"label": f"{hf}@{v['rev'][:9]}", "plan": [{"rel": hf, "text": v["text"], "pos": v["pos"]}]}
            for v in versions
        ]
    if scen == "distant_branch":
        plans = []
        for br in branches:
            b = branch_overlay_plan(repo, br)
            if any(p.get("pos") for p in b["plan"]):
                plans.append({"label": br, "plan": b["plan"], "merge_base": b["merge_base"]})
        return plans
    if scen == "manifest":
        commit_plans = session_commit_plans(repo, max_n)
        manifest = head_text(repo, "Cargo.toml") + "\n# bench manifest overlay probe\n"
        plans = []
        for cp in commit_plans:
            plans.append(
                {
                    "label": f"manifest+{cp['commit'][:9]}",
                    "plan": [{"rel": "Cargo.toml", "text": manifest, "pos": None}] + cp["plan"],
                }
            )
        return plans
    raise ValueError(scen)


def manifest_real_root_probe(runner, repo, out):
    """Worktree with a COMMITTED crate-manifest change: does the session stay
    in the shared world? new workspace? what does it cost?"""
    crates = [
        d for d in sorted(os.listdir(os.path.join(repo, "crates")))
        if os.path.isfile(os.path.join(repo, "crates", d, "Cargo.toml"))
    ] if os.path.isdir(os.path.join(repo, "crates")) else []
    manifest_rel = f"crates/{crates[0]}/Cargo.toml" if crates else "Cargo.toml"
    wt_path = os.path.join(os.path.dirname(repo), f"{os.path.basename(repo)}-manifest-probe")
    if not os.path.isdir(wt_path):
        subprocess.run(["git", "-C", repo, "worktree", "add", "--detach", wt_path, "HEAD"], check=True, capture_output=True)
        with open(os.path.join(wt_path, manifest_rel), "a") as f:
            f.write("\n[features]\nbench_realroot_probe = []\n")
        subprocess.run(["git", "-C", wt_path, "-c", "user.email=bench@analyzed", "-c", "user.name=bench",
                       "commit", "-am", "bench: manifest probe"], check=True, capture_output=True)
    world = runner.start_world("manifest-realroot")
    keeper = world.pop("_keeper")
    rec = {"probe": "manifest_real_root", "manifest": manifest_rel, "world": world}
    st = runner.daemon.status()
    rec["workspaces_before"] = st.get("workspaces")
    rec["rss_before_kb"] = runner.daemon.rss_now_kb()
    sess = runner._spawn("manifest-realroot", root=wt_path)
    t0 = time.monotonic()
    init, entry = sess.initialize(wt_path, timeout=180)
    rec["attach_s"] = entry["latency_s"]
    files = rs_files(wt_path, limit=40)
    qrel = files[len(files) // 2]
    qpath = os.path.join(wt_path, qrel)
    pos = pick_query_target(open(qpath, encoding="utf-8", errors="replace").read())
    first = None
    t1 = time.monotonic()
    while time.monotonic() - t1 < 600:
        r, e = sess.document_symbol(qpath, timeout=600)
        if r:
            first = time.monotonic() - t1
            break
        time.sleep(1)
    rec["first_result_s"] = round(first, 4) if first else None
    if pos:
        r, e = sess.definition(qpath, pos["def"]["line"], pos["def"]["char"])
        rec["definition_ok"] = bool(r) and any(
            l.get("uri", "").endswith(qrel) for l in (r if isinstance(r, list) else [r])
        )
        rec["definition_latency_s"] = e["latency_s"]
    st = runner.daemon.status()
    rec["workspaces_after"] = st.get("workspaces")
    rec["rss_after_kb"] = runner.daemon.rss_now_kb()
    rec["total_from_spawn_s"] = round(time.monotonic() - t0, 2)
    sess.shutdown()
    rec.update(runner.end_world(keeper, retention_marks=(15,)))
    rec.pop("rss_timeline", None)
    rec.pop("status_timeline", None)
    return rec


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--binary", default=os.path.join(os.path.dirname(here), "target/release/analyzed"))
    ap.add_argument("--sessions", default="1,5,20")
    ap.add_argument("--scenarios", default="a,b,c,d,e")
    ap.add_argument("--branches", default=None)
    ap.add_argument("--iters", type=int, default=8)
    ap.add_argument("--cell-budget-s", type=float, default=1800.0)
    ap.add_argument("--watchdog-gib", type=float, default=24.0)
    ap.add_argument("--tmpdir", default="/tmp/anlzb")
    ap.add_argument("--out", default=None)
    ap.add_argument("--allow-worktrees", action="store_true",
                    help="subject is a bench-owned clone: enables the manifest real-root probe")
    args = ap.parse_args()

    repo = os.path.realpath(args.repo)
    name = os.path.basename(repo)
    out_dir = args.out or os.path.join(here, "results", "v2", f"phase1-{name}")
    os.makedirs(out_dir, exist_ok=True)
    cells = [int(c) for c in args.sessions.split(",")]
    branches = existing_branches(
        repo, args.branches.split(",") if args.branches else DEFAULT_BRANCHES
    )

    meta = {
        "phase": 1,
        "t": time.time(),
        "repo": repo,
        "repo_head": subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "analyzed_version": subprocess.run([args.binary, "--version"], capture_output=True, text=True).stdout.strip(),
        "analyzed_rev": subprocess.run(["git", "-C", os.path.dirname(here), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "rustc": subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip(),
        "config": RA_CONFIG_V2,
        "sessions": cells,
        "iters": args.iters,
        "branches": branches,
        "watchdog_gib": args.watchdog_gib,
    }
    with open(os.path.join(out_dir, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    print(f"phase1 {name}: {meta['analyzed_version']} @ {meta['repo_head'][:9]}", flush=True)

    runner = V2Runner(
        binary=args.binary, repo=repo, out_dir=out_dir, tmpdir=args.tmpdir,
        config=RA_CONFIG_V2, iters=args.iters, cell_budget_s=args.cell_budget_s,
        watchdog_gib=args.watchdog_gib,
    )

    for letter in args.scenarios.split(","):
        scen = SCEN_NAMES[letter.strip()]
        plans = build_plans(repo, scen, max(cells), branches)
        if not plans:
            print(f"  {scen}: no usable plans, skipped", flush=True)
            continue
        all_rels = {p["rel"] for pl in plans for p in pl["plan"]}
        ready_rel = next(r for r in rs_files(repo) if r not in all_rels)
        out = {"scenario": scen, "t_start": time.time(), "plans": [
            {k: v for k, v in p.items() if k != "plan"} | {"files": len(p["plan"])} for p in plans
        ], "cells": []}
        for idx, n in enumerate(cells):
            last = idx == len(cells) - 1
            world = runner.start_world(f"{scen}-{n}")
            keeper = world.pop("_keeper")
            if scen == "storm":
                cell = runner.run_storm_cell(plans, n)
            else:
                cell = runner.run_cell(scen, plans, n, ready_rel)
            cell.update(world)
            cell.update(runner.end_world(keeper, retention_marks=(10, 30, 60) if last else (15,)))
            out["cells"].append(cell)
            with open(os.path.join(out_dir, f"{scen}.json"), "w") as f:
                json.dump(out, f, indent=1)
            s = cell.get("warm_summary", {})
            print(f"  {scen} N={n}: wall={cell['wall_s']}s cold={cell.get('cold_warm_s')}s "
                  f"aborted={len(s.get('aborted', []))} score={s.get('score')}", flush=True)
        if scen == "manifest" and args.allow_worktrees:
            out["real_root"] = manifest_real_root_probe(runner, repo, out_dir)
            with open(os.path.join(out_dir, f"{scen}.json"), "w") as f:
                json.dump(out, f, indent=1)
        out["stall_samples"] = runner.daemon.sample_log
        out["wall_s"] = round(time.time() - out["t_start"], 1)
        with open(os.path.join(out_dir, f"{scen}.json"), "w") as f:
            json.dump(out, f, indent=1)
    print("phase1 complete", flush=True)


if __name__ == "__main__":
    main()
