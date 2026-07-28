#!/usr/bin/env python3
"""Bench gate for analyzed under multi-worktree agent load.

Usage:
  python3 bench/run_bench.py --subject /path/to/btcr-clone [options]

The subject must be a read-only clone pinned to a fixed revision; the harness
never touches the original repository. Results land in bench/results/*.json.
"""

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness.scenarios import Bench  # noqa: E402

SCENARIOS = ["disjoint", "disjoint_rooted", "same_file", "distant_branch", "manifest", "storm"]


def main():
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser()
    ap.add_argument("--subject", required=True, help="path to the subject clone (never the original)")
    ap.add_argument("--base-rev", default="HEAD")
    ap.add_argument("--binary", default=os.path.join(repo, "target/release/analyzed"))
    ap.add_argument("--out", default=os.path.join(repo, "bench/results"))
    ap.add_argument("--tmpdir", default="/tmp/anlzb", help="short TMPDIR (macOS SUN_LEN)")
    ap.add_argument("--worktrees", default=None, help="where to create bench worktrees")
    ap.add_argument("--scenarios", default=",".join(SCENARIOS))
    ap.add_argument("--cells", default="1,5,20")
    ap.add_argument("--iters", type=int, default=10)
    ap.add_argument("--distant-files", type=int, default=250)
    ap.add_argument("--cell-budget-s", type=float, default=1500.0)
    args = ap.parse_args()

    subject = os.path.realpath(args.subject)
    assert os.path.isdir(os.path.join(subject, ".git")) or os.path.isfile(
        os.path.join(subject, ".git")
    ), "subject must be a git clone"
    worktrees = args.worktrees or os.path.join(os.path.dirname(subject), "bench-worktrees")

    version = subprocess.run(
        [args.binary, "--version"], capture_output=True, text=True
    ).stdout.strip()
    meta = {
        "t": time.time(),
        "analyzed_version": version,
        "analyzed_rev": subprocess.run(
            ["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "subject": subject,
        "subject_rev": subprocess.run(
            ["git", "-C", subject, "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "host": {
            "cpu": subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True
            ).stdout.strip(),
            "mem_bytes": int(
                subprocess.run(
                    ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True
                ).stdout.strip()
            ),
        },
        "cells": args.cells,
        "iters": args.iters,
        "distant_files": args.distant_files,
    }
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    print(f"bench: {version} on {meta['subject_rev'][:9]}", flush=True)

    bench = Bench(
        binary=args.binary,
        subject_root=subject,
        base_rev=args.base_rev,
        out_dir=args.out,
        tmpdir=args.tmpdir,
        worktrees_root=worktrees,
        cells=[int(c) for c in args.cells.split(",")],
        iters=args.iters,
        distant_files=args.distant_files,
        cell_budget_s=args.cell_budget_s,
    )
    for scen in args.scenarios.split(","):
        scen = scen.strip()
        print(f"=== scenario {scen} ===", flush=True)
        t0 = time.time()
        out = bench.run_scenario(scen)
        print(
            f"    done in {out['wall_s']}s; cold_warm={out.get('cold_warm_s')}s; "
            f"cells={[c.get('sessions') for c in out['cells']]}",
            flush=True,
        )
    print("bench complete", flush=True)


if __name__ == "__main__":
    main()
