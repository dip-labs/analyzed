"""Real-diff sampling for the v2 bench: agent diffs are replayed from the
subject's own history instead of synthesized.

- commit replay: a session's overlay = the PRE-image (commit^) of the .rs
  files a real commit touched — realistic diff shape (which files change
  together, how much) and guaranteed text != HEAD for files the commit changed.
- branch diffs: overlay = branch blob contents for files differing from the
  merge-base with HEAD (distant-branch scenario, real branches).
- hot-file versions: last K distinct historical versions of the most-edited
  file (same-file scenario: real divergent versions of one path).

Query targets are real `fn` definitions parsed from the overlay text; the
LAST fn in the file discriminates versions (its line shifts between versions).
All git access is read-only (git show / log / diff).
"""

from __future__ import annotations

import os
import re
import subprocess

FN_RE = re.compile(
    r"^(\s*)(?:pub(?:\([^)]*\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?"
    r"(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?fn\s+(\w+)",
    re.M,
)


def _git(repo, *args, text=True):
    r = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=text)
    if r.returncode != 0:
        raise RuntimeError(f"git {' '.join(args[:3])}...: {r.stderr if text else r.stderr.decode()!r}"[:300])
    return r.stdout


def _show(repo, rev, path):
    return _git(repo, "show", f"{rev}:{path}")


def head_text(repo, path):
    with open(os.path.join(repo, path), encoding="utf-8", errors="replace") as f:
        return f.read()


def sample_commit_diffs(repo, n_commits=50, min_rs=1, max_rs=8):
    """[(sha, [rs files changed])] for the last n commits, newest first."""
    out = _git(repo, "log", f"-{n_commits}", "--no-merges", "--name-only", "--pretty=%x00%H")
    commits = []
    for block in out.split("\x00"):
        if not block.strip():
            continue
        lines = block.strip().splitlines()
        sha, files = lines[0], [l for l in lines[1:] if l.endswith(".rs") and "/tests/" not in l]
        if min_rs <= len(files):
            commits.append((sha, files[:max_rs]))
    return commits


def fn_targets(text):
    """All fn defs as (name, line, char-of-name); line/char 0-based."""
    out = []
    for m in FN_RE.finditer(text):
        line = text[: m.start(2)].count("\n")
        char = m.start(2) - (text.rindex("\n", 0, m.start(2)) + 1 if "\n" in text[: m.start(2)] else 0)
        out.append((m.group(2), line, char))
    return out


def pick_query_target(text):
    """Last fn def in the file — its line discriminates file versions."""
    targets = fn_targets(text)
    if not targets:
        return None
    name, line, char = targets[-1]
    return {"name": name, "def": {"line": line, "char": char}}


def commit_overlay_plan(repo, sha, files, max_files=5):
    """Overlay = pre-image (sha^) of files the commit touched; skip files
    whose pre-image equals current HEAD text or which didn't exist yet."""
    plan = []
    for path in files:
        try:
            pre = _show(repo, f"{sha}^", path)
        except RuntimeError:
            continue  # file added by this commit
        try:
            cur = head_text(repo, path)
        except OSError:
            continue  # file deleted since
        if pre == cur:
            continue
        pos = pick_query_target(pre)
        plan.append({"rel": path, "text": pre, "pos": None if pos is None else pos})
        if len(plan) >= max_files:
            break
    # exactly one query file: the first with a target
    seen = False
    for p in plan:
        if p["pos"] is not None:
            if seen:
                p["pos"] = None
            else:
                seen = True
    return plan if seen else []


def session_commit_plans(repo, n_sessions, n_commits=50):
    """One plan per session from distinct recent commits; skips empty plans."""
    plans = []
    for sha, files in sample_commit_diffs(repo, n_commits):
        plan = commit_overlay_plan(repo, sha, files)
        if plan:
            plans.append({"commit": sha, "plan": plan})
        if len(plans) >= n_sessions:
            break
    return plans


def branch_overlay_plan(repo, branch, max_files=400):
    """Overlay = branch blobs for files differing from merge-base with HEAD."""
    base = _git(repo, "merge-base", "HEAD", branch).strip()
    files = [
        l
        for l in _git(repo, "diff", "--name-only", f"{base}", branch).splitlines()
        if l.endswith(".rs")
    ]
    plan = []
    for path in files[:max_files]:
        try:
            text = _show(repo, branch, path)
        except RuntimeError:
            continue  # deleted on branch
        try:
            cur = head_text(repo, path)
        except OSError:
            cur = None  # added on branch: still an overlay (new file) — skip:
            continue  # 0.4.2 has no base file for it, overlay would be ignored
        if text == cur:
            continue
        pos = pick_query_target(text)
        plan.append({"rel": path, "text": text, "pos": pos})
    seen = False
    for p in plan:
        if p["pos"] is not None:
            if seen:
                p["pos"] = None
            else:
                seen = True
    return {"branch": branch, "merge_base": base, "files_total": len(files), "plan": plan}


def hot_file(repo, n_commits=50):
    """Most frequently changed .rs file in recent history."""
    out = _git(repo, "log", f"-{n_commits}", "--no-merges", "--name-only", "--pretty=")
    counts = {}
    for l in out.splitlines():
        if l.endswith(".rs") and "/tests/" not in l:
            counts[l] = counts.get(l, 0) + 1
    live = [(c, f) for f, c in counts.items() if os.path.exists(os.path.join(repo, f))]
    if not live:
        raise RuntimeError("no hot .rs file found")
    return sorted(live, reverse=True)[0][1]


def file_versions(repo, path, k=20):
    """Last k DISTINCT historical versions of path (newest first), each with a
    query target; versions equal to current HEAD text are skipped."""
    shas = _git(repo, "log", f"-{k * 3}", "--pretty=%H", "--", path).split()
    cur = head_text(repo, path)
    seen, versions = set(), []
    for sha in shas:
        try:
            text = _show(repo, sha, path)
        except RuntimeError:
            continue
        h = hash(text)
        if h in seen or text == cur:
            continue
        seen.add(h)
        pos = pick_query_target(text)
        if pos is None:
            continue
        versions.append({"rev": sha, "text": text, "pos": pos})
        if len(versions) >= k:
            break
    return versions


def worktree_list(repo):
    """[(path, branch_or_none, exists)] from git worktree list --porcelain."""
    out = _git(repo, "worktree", "list", "--porcelain")
    items, cur = [], {}
    for line in out.splitlines() + [""]:
        if not line:
            if cur:
                items.append(
                    (
                        cur.get("worktree"),
                        cur.get("branch", "").replace("refs/heads/", "") or None,
                        os.path.isdir(cur.get("worktree", "")),
                    )
                )
            cur = {}
        elif " " in line:
            k, v = line.split(" ", 1)
            cur[k] = v
        else:
            cur[line] = True
    return items


def rs_files(root, limit=None):
    """Real .rs files under a worktree (skipping target/.git), sorted."""
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ("target", ".git", "node_modules")]
        for fn in sorted(filenames):
            if fn.endswith(".rs"):
                out.append(os.path.relpath(os.path.join(dirpath, fn), root))
    out.sort()
    return out[:limit] if limit else out
