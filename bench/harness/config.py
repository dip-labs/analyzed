"""Session config for the v2 bench phases.

Flycheck/checkOnSave, cargo build scripts and proc macros are DISABLED: the
daemon must never spawn cargo / proc-macro-srv inside foreign (live) worktrees.
This UNDERSTATES semantic completeness (derive/proc-macro expansions and
OUT_DIR-generated code are absent) — recorded in the report.
"""

RA_CONFIG_V2 = {
    "cachePriming": {"enable": True},
    "checkOnSave": False,
    "cargo": {"buildScripts": {"enable": False}},
    "procMacro": {"enable": False},
    "diagnostics": {"enable": True},
    "files": {"watcher": "server"},
}
