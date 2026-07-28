"""Synthetic agent diffs against the subject clone.

Deterministic: file selection is sorted-by-path, markers are derived from
(scenario, session). Real git worktrees are created for the scenarios where a
worktree is meaningful on disk (disjoint / distant-branch / manifest); overlay
texts are read back from the worktree so the harness measures the same bytes an
agent would hold in its editor buffer.
"""

from __future__ import annotations

import os
import subprocess


def marker_fn(scen: str, sid: int) -> str:
    return f"bench_uni_{scen}_{sid}"


def make_variant(base_text: str, scen: str, sid: int, tick: int = 0):
    """Append session-unique marker functions; return (text, positions).

    Positions (0-based line/char, UTF-16 == ASCII here):
      def:  name position on the `pub fn bench_uni_...` line
      call: position of the callee name inside bench_call_...
    """
    name = marker_fn(scen, sid)
    if not base_text.endswith("\n"):
        base_text += "\n"
    block = (
        f"\n// analyzed-bench overlay marker tick={tick}\n"
        f"pub fn {name}(x: u64) -> u64 {{ x + {sid + 1} }}\n"
        f"pub fn bench_call_{scen}_{sid}() -> u64 {{ {name}(1) + 1 }}\n"
    )
    text = base_text + block
    def_line = text[: text.rindex(f"pub fn {name}(")].count("\n")
    def_char = f"pub fn ".__len__()
    call_line = def_line + 1
    call_char = f"pub fn bench_call_{scen}_{sid}() -> u64 {{ ".__len__()
    return text, {
        "name": name,
        "def": {"line": def_line, "char": def_char},
        "call": {"line": call_line, "char": call_char},
    }


def pick_files(subject_root: str, count: int, min_size=1000, max_size=24000, subdir="src"):
    """Deterministic pick of MODULE files: only files under subdirectories of
    src (top-level src/*.rs here are bin crate roots or orphan files not
    referenced by any manifest target), skipping mod.rs."""
    picked = []
    base = os.path.join(subject_root, subdir)
    for dirpath, dirnames, filenames in sorted(os.walk(base)):
        dirnames.sort()
        if os.path.normpath(dirpath) == os.path.normpath(base):
            continue  # top-level src/*.rs: crate roots / orphans, not modules
        if os.path.relpath(dirpath, base).split(os.sep)[0] == "bin":
            continue  # src/bin/*.rs are autobin crate roots
        for fn in sorted(filenames):
            if not fn.endswith(".rs") or fn in ("main.rs", "lib.rs", "mod.rs"):
                continue
            full = os.path.join(dirpath, fn)
            try:
                size = os.path.getsize(full)
            except OSError:
                continue
            if min_size <= size <= max_size:
                picked.append(os.path.relpath(full, subject_root))
    if len(picked) < count:
        raise RuntimeError(f"only {len(picked)} candidate files, need {count}")
    return picked[:count]


def bin_roots(subject_root: str, min_size=200, max_size=200000):
    """Crate-root files of the package's bin targets: [[bin]] paths from
    Cargo.toml plus src/bin/*.rs autobins. Sorted, size-filtered."""
    roots = set()
    manifest = read_subject(subject_root, "Cargo.toml")
    for line in manifest.splitlines():
        line = line.strip()
        if line.startswith("path = \"src/") and line.endswith(".rs\""):
            roots.add(line[len("path = \""):-1])
    autobin_dir = os.path.join(subject_root, "src", "bin")
    if os.path.isdir(autobin_dir):
        for fn in os.listdir(autobin_dir):
            if fn.endswith(".rs"):
                roots.add(f"src/bin/{fn}")
    out = []
    for rel in sorted(roots):
        full = os.path.join(subject_root, rel)
        try:
            size = os.path.getsize(full)
        except OSError:
            continue
        if min_size <= size <= max_size:
            out.append(rel)
    return out


def read_subject(subject_root: str, rel: str) -> str:
    with open(os.path.join(subject_root, rel), "r", encoding="utf-8", errors="replace") as f:
        return f.read()


class WorktreeFactory:
    def __init__(self, subject_root: str, worktrees_root: str, base_rev: str):
        self.subject_root = subject_root
        self.worktrees_root = worktrees_root
        self.base_rev = base_rev
        os.makedirs(worktrees_root, exist_ok=True)

    def _git(self, *args, cwd=None):
        r = subprocess.run(
            ["git", *args], cwd=cwd or self.subject_root, capture_output=True, text=True
        )
        if r.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)}: {r.stderr.strip()}")
        return r.stdout

    def create(self, name: str, files: dict[str, str], commit_msg: str) -> str:
        """Worktree at base_rev with `files` (rel -> text) committed on a bench branch."""
        path = os.path.join(self.worktrees_root, name)
        if os.path.isdir(path):
            return path
        branch = f"bench/{name}"
        self._git("worktree", "add", "--detach", path, self.base_rev)
        self._git("checkout", "-B", branch, cwd=path)
        for rel, text in files.items():
            full = os.path.join(path, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write(text)
        self._git("add", "-A", cwd=path)
        self._git("-c", "user.email=bench@analyzed", "-c", "user.name=bench",
                  "commit", "-m", commit_msg, cwd=path)
        return path

    def cleanup(self):
        try:
            out = self._git("worktree", "list", "--porcelain")
        except RuntimeError:
            return
        for line in out.splitlines():
            if line.startswith("worktree ") and self.worktrees_root in line:
                path = line.split(" ", 1)[1]
                subprocess.run(
                    ["git", "worktree", "remove", "--force", path],
                    cwd=self.subject_root, capture_output=True,
                )
