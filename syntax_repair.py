#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automatic Python syntax repair tool.
- Scans current directory recursively for .py files.
- Detects SyntaxError via ast.parse.
- Applies heuristic repairs (colons, brackets, quotes, EOF newline).
- Writes .bak backups and only overwrites on successful re-parse.
- Pure, tiny functions (≤10 lines), 79 cols, mypy-friendly typing.
- Run: python py_syntax_repair_tool.py --help
- Self-test: python py_syntax_repair_tool.py --self-test
"""
from __future__ import annotations

import argparse
import ast
import itertools
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional, Sequence, Tuple

# ---------------------------- Data structures ----------------------------- #

@dataclass(frozen=True)
class RepairResult:
    ok: bool
    code: str
    applied: Tuple[str, ...]

# ------------------------------ Utilities -------------------------------- #

def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="surrogatepass")


def write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8")


def backup_path(p: Path) -> Path:
    return p.with_suffix(p.suffix + ".bak")


def copy_backup(src: Path) -> Path:
    dst = backup_path(src)
    write_text(dst, read_text(src))
    return dst


def ast_ok(code: str) -> bool:
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False

# --------------------------- Small string utils --------------------------- #

def trailing_newline(s: str) -> str:
    return s if s.endswith("\n") else s + "\n"


def ends_with_ws(s: str) -> bool:
    return bool(s) and s[-1].isspace()


def rstrip_nl(s: str) -> str:
    return s[:-1] if s.endswith("\n") else s


def lines(s: str) -> List[str]:
    return s.splitlines()


def join_lines(xs: Iterable[str]) -> str:
    return "\n".join(xs) + "\n"

# ----------------------------- Heuristics -------------------------------- #

KEYWORDS_WITH_COLON = (
    "if", "elif", "else", "for", "while", "try", "except", "finally",
    "def", "class", "with", "match", "case",
)

OPENERS = "([{"
CLOSERS = ")] }"
PAIRS = {"(": ")", "[": "]", "{": "}"}

TRIPLE = ("'''", '"""')
SINGLE = ("'", '"')


def add_missing_colons(code: str) -> Tuple[str, bool]:
    out: List[str] = []
    changed = False
    for ln in lines(code):
        s = ln.rstrip()
        head = s.split("#", 1)[0].rstrip()
        pat = r"^(\s*)(%s)\b.*[^:]$" % "|".join(KEYWORDS_WITH_COLON)
        if re.match(pat, head):
            ln = ln + ":"
            changed = True
        out.append(ln)
    return join_lines(out), changed


def balance_brackets(code: str) -> Tuple[str, bool]:
    stk: List[str] = []
    added: List[str] = []
    for ch in code:
        if ch in OPENERS:
            stk.append(ch)
        elif ch in CLOSERS and stk:
            if PAIRS.get(stk[-1]) == ch:
                stk.pop()
    while stk:
        added.append(PAIRS[stk.pop()])
    return code + "".join(added), bool(added)


def close_triple_quotes(code: str) -> Tuple[str, bool]:
    changed = False
    for q in TRIPLE:
        n = code.count(q)
        if n % 2 == 1:
            code += q
            changed = True
    return code, changed


def close_single_quotes(code: str) -> Tuple[str, bool]:
    changed = False
    def _fix(line: str) -> Tuple[str, bool]:
        nonlocal changed
        head = line.split("#", 1)[0]
        for q in SINGLE:
            if head.count(q) % 2 == 1:
                line += q
                changed = True
        return line, changed
    out = [
        _fix(ln)[0] for ln in lines(code)
    ]
    return join_lines(out), changed


def ensure_newline_eof(code: str) -> Tuple[str, bool]:
    s = trailing_newline(code)
    return s, s != code


def strip_trailing_ws(code: str) -> Tuple[str, bool]:
    out = [rstrip_nl(ln).rstrip() for ln in lines(code)]
    s = join_lines(out)
    return s, s != code

# ------------------------- Repair pipeline logic -------------------------- #

Fixer = Callable[[str], Tuple[str, bool]]


def fixers() -> Tuple[Fixer, ...]:
    return (
        add_missing_colons,
        close_triple_quotes,
        close_single_quotes,
        balance_brackets,
        ensure_newline_eof,
        strip_trailing_ws,
    )


def apply_fixes_once(code: str) -> RepairResult:
    applied: List[str] = []
    for fx in fixers():
        code2, changed = fx(code)
        if changed:
            applied.append(fx.__name__)
            code = code2
    ok = ast_ok(code)
    return RepairResult(ok, code, tuple(applied))


def repair_code(code: str, limit: int) -> RepairResult:
    step = 0
    applied_all: List[str] = []
    cur = code
    while step < limit:
        res = apply_fixes_once(cur)
        applied_all.extend(res.applied)
        if res.ok:
            return RepairResult(True, res.code, tuple(applied_all))
        if not res.applied:
            break
        cur = res.code
        step += 1
    return RepairResult(False, cur, tuple(applied_all))

# ------------------------------ File passes ------------------------------- #


def needs_repair(p: Path) -> bool:
    try:
        return not ast_ok(read_text(p))
    except Exception:
        return True


def repair_file(p: Path, limit: int, dry: bool) -> Tuple[bool, Tuple[str, ...]]:
    src = read_text(p)
    res = repair_code(src, limit)
    if res.ok and not dry:
        copy_backup(p)
        write_text(p, res.code)
    return res.ok, res.applied

# ------------------------------ Discovery -------------------------------- #


def is_py(p: Path) -> bool:
    return p.suffix == ".py"


def ignore_dirs() -> Tuple[str, ...]:
    return (".git", "__pycache__", ".venv", "venv", ".mypy_cache")


def walk_py(root: Path) -> Iterator[Path]:
    bad = set(ignore_dirs())
    for dp, dns, fns in os.walk(root):
        dns[:] = [d for d in dns if d not in bad]
        for f in fns:
            p = Path(dp) / f
            if is_py(p):
                yield p

# ------------------------------ Reporting -------------------------------- #


def log(msg: str, *, quiet: bool) -> None:
    if not quiet:
        print(msg)


def fmt_result(p: Path, ok: bool, applied: Sequence[str]) -> str:
    tag = "FIXED" if ok else "FAILED"
    apps = ",".join(applied) if applied else "-"
    return f"{tag:6} {p} [{apps}]"

# ------------------------------ CLI pieces -------------------------------- #


def add_dir_arg(pa: argparse.ArgumentParser) -> None:
    pa.add_argument("dir", nargs="?", default=".", help="root directory")


def add_limit_arg(pa: argparse.ArgumentParser) -> None:
    pa.add_argument("--limit", type=int, default=5, help="max passes")


def add_dry_arg(pa: argparse.ArgumentParser) -> None:
    pa.add_argument("--dry-run", action="store_true", help="no writes")


def add_quiet_arg(pa: argparse.ArgumentParser) -> None:
    pa.add_argument("-q", "--quiet", action="store_true", help="silence")


def add_selftest_arg(pa: argparse.ArgumentParser) -> None:
    pa.add_argument("--self-test", action="store_true", help="run tests")


def build_parser() -> argparse.ArgumentParser:
    pa = argparse.ArgumentParser(prog="py-syntax-repair")
    for adder in (
        add_dir_arg,
        add_limit_arg,
        add_dry_arg,
        add_quiet_arg,
        add_selftest_arg,
    ):
        adder(pa)
    return pa

# ------------------------------- Self tests ------------------------------- #

@dataclass(frozen=True)
class Case:
    name: str
    bad: str
    good: str


def cases() -> Tuple[Case, ...]:
    return (
        Case("colon_if", "if True\n    pass", "if True:\n    pass\n"),
        Case("paren", "x = (1+2\n", "x = (1+2)\n"),
        Case("triple", "s = '''abc\n", "s = '''abc'''\n"),
        Case("single", "s = 'a\n", "s = 'a'\n"),
        Case("eofnl", "x=1", "x=1\n"),
    )


def run_case(c: Case) -> Tuple[bool, str]:
    res = repair_code(c.bad, 5)
    ok = res.ok and res.code == c.good
    return ok, c.name


def run_tests() -> int:
    fails: List[str] = []
    for c in cases():
        ok, name = run_case(c)
        if not ok:
            fails.append(name)
    if fails:
        print("TEST FAIL:", ", ".join(fails))
        return 1
    print("All tests passed.")
    return 0

# --------------------------------- Main ---------------------------------- #


def main(argv: Optional[Sequence[str]] = None) -> int:
    pa = build_parser()
    ns = pa.parse_args(argv)
    if ns.self_test:
        return run_tests()
    root = Path(ns.dir).resolve()
    any_fail = False
    for p in walk_py(root):
        if needs_repair(p):
            ok, apps = repair_file(p, ns.limit, ns.dry_run)
            any_fail |= not ok
            log(fmt_result(p, ok, apps), quiet=ns.quiet)
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
