#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Codegen for a tiny, pure Python syntax repair toolkit.
- Generates files inside ./py-syntax-repair/
- All functions ≤10 lines, 79 cols, mypy-friendly typing.
- Usage: python generate_py_syntax_repair_tool.py
"""
from __future__ import annotations
from pathlib import Path
from typing import Iterable

# ------------------------------- Utilities -------------------------------- #
ROOT = Path("py-syntax-repair")


def _w(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _p(name: str) -> Path:
    return ROOT / name


def _emit(name: str, content: str) -> None:
    _w(_p(name), content)
    print(f"✓ Wrote {ROOT / name}")


def _join(lines: Iterable[str]) -> str:
    return "\n".join(lines) + "\n"


# ----------------------------- File contents ------------------------------ #
tool_py = _join(
    [
        "#!/usr/bin/env python3",
        "# -*- coding: utf-8 -*-",
        '"""',
        "Tiny Python syntax repair tool.",
        "- Recursively fixes .py files.",
        "- Heuristic fixes: colons, brackets, quotes, EOF newline.",
        "- Safe: writes .bak, only overwrites if ast.parse succeeds.",
        "- ≤10 lines per func, 79 cols, typed.",
        '"""',
        "from __future__ import annotations",
        "",
        "import argparse",
        "import ast",
        "import os",
        "import re",
        "import sys",
        "import tokenize",
        "from dataclasses import dataclass",
        "from io import BytesIO",
        "from pathlib import Path",
        "from typing import Callable, Iterable, Iterator, List, Optional, Sequence, Tuple",
        "",
        "# ---------------------------- Data structs ---------------------------- #",
        "",
        "@dataclass(frozen=True)",
        "class RepairResult:",
        "    ok: bool",
        "    code: str",
        "    applied: Tuple[str, ...]",
        "",
        "@dataclass(frozen=True)",
        "class Case:",
        "    name: str",
        "    bad: str",
        "    good: str",
        "",
        "Fixer = Callable[[str], Tuple[str, bool]]",
        "",
        "# ------------------------------- IO ---------------------------------- #",
        "",
        "def read_text(p: Path) -> str:",
        "    return p.read_text(encoding='utf-8', errors='surrogatepass')",
        "",
        "def write_text(p: Path, s: str) -> None:",
        "    p.write_text(s, encoding='utf-8')",
        "",
        "def backup_path(p: Path) -> Path:",
        "    return p.with_suffix(p.suffix + '.bak')",
        "",
        "def copy_backup(src: Path) -> Path:",
        "    dst = backup_path(src)",
        "    write_text(dst, read_text(src))",
        "    return dst",
        "",
        "# --------------------------- Token helpers --------------------------- #",
        "",
        "def get_indent(line: str) -> str:",
        "    return line[:len(line) - len(line.lstrip())]",
        "",
        "def strip_comment(line: str) -> str:",
        "    return line.split('#', 1)[0]",
        "",
        "# ----------------------------- Heuristics ----------------------------- #",
        "",
        "KEYWORDS_NEED_COLON = {",
        "    'if', 'elif', 'else', 'for', 'while', 'try', 'except',",
        "    'finally', 'def', 'class', 'with', 'match', 'case',",
        "}",
        "",
        "PAIRS = {'(': ')', '[': ']', '{': '}'}",
        "OPENERS = set(PAIRS.keys())",
        "CLOSERS = set(PAIRS.values())",
        "",
        "def ast_ok(code: str) -> bool:",
        "    try: ast.parse(code); return True",
        "    except SyntaxError: return False",
        "",
        "def add_missing_colons(code: str) -> Tuple[str, bool]:",
        "    lines = code.splitlines()",
        "    out: List[str] = []",
        "    changed = False",
        "    for i, line in enumerate(lines):",
        "        raw = line",
        "        indent = get_indent(line)",
        "        line = strip_comment(line).rstrip()",
        "        if not line or line.endswith(':') or line.endswith('\\'):",
        "            out.append(raw); continue",
        "        try:",
        "            tokens = list(tokenize.tokenize(BytesIO(line.encode()).readline))",
        "        except tokenize.TokenError:",
        "            out.append(raw); continue",
        "        if len(tokens) > 1 and tokens[-2].string in KEYWORDS_NEED_COLON:",
        "            if not any(t.string == ':' for t in tokens[1:]):",
        "                raw = raw.rstrip() + ':' + raw[len(raw.rstrip()):]",
        "                changed = True",
        "        out.append(raw)",
        "    return '\n'.join(out) + '\n', changed",
        "",
        "def balance_brackets(code: str) -> Tuple[str, bool]:",
        "    stack: List[Tuple[str, int]] = []",
        "    for i, ch in enumerate(code):",
        "        if ch in OPENERS: stack.append((ch, i))",
        "        elif ch in CLOSERS and stack:",
        "            if PAIRS[stack[-1][0]] == ch: stack.pop()",
        "    added = ''.join(PAIRS[op] for op, _ in reversed(stack))",
        "    return code + added, bool(added)",
        "",
        "def close_quotes(code: str) -> Tuple[str, bool]:",
        "    lines = code.splitlines()",
        "    out: List[str] = []",
        "    changed = False",
        "    for line in lines:",
        "        head = strip_comment(line)",
        "        for q in \"'\\\"\":",
        "            if head.count(q) % 2 == 1:",
        "                line += q",
        "                changed = True",
        "        out.append(line)",
        "    return '\n'.join(out) + '\n', changed",
        "",
        "def ensure_eof_newline(code: str) -> Tuple[str, bool]:",
        "    if code.endswith('\n'): return code, False",
        "    if not code.strip(): return code + '\n', True",
        "    try: ast.parse(code + '\n'); return code + '\n', True",
        "    except: return code, False",
        "",
        "def strip_trailing_ws(code: str) -> Tuple[str, bool]:",
        "    lines = [l.rstrip() for l in code.splitlines()]",
        "    s = '\n'.join(lines) + '\n'",
        "    return s, s != code",
        "",
        "def fixers() -> Tuple[Fixer, ...]:",
        "    return (",
        "        add_missing_colons,",
        "        close_quotes,",
        "        balance_brackets,",
        "        ensure_eof_newline,",
        "        strip_trailing_ws,",
        "    )",
        "",
        "def apply_fixes_once(code: str) -> RepairResult:",
        "    applied: List[str] = []",
        "    for f in fixers():",
        "        code2, ch = f(code)",
        "        if ch: applied.append(f.__name__); code = code2",
        "    return RepairResult(ast_ok(code), code, tuple(applied))",
        "",
        "def repair_code(code: str, limit: int = 5) -> RepairResult:",
        "    cur = code",
        "    all_applied: List[str] = []",
        "    for _ in range(limit):",
        "        res = apply_fixes_once(cur)",
        "        all_applied.extend(res.applied)",
        "        if res.ok or not res.applied: break",
        "        cur = res.code",
        "    return RepairResult(res.ok, cur, tuple(all_applied))",
        "",
        "# ------------------------------ Files -------------------------------- #",
        "",
        "def is_py(p: Path) -> bool:",
        "    return p.suffix == '.py'",
        "",
        "def walk_py(root: Path) -> Iterator[Path]:",
        "    skip = {'.git', '__pycache__', '.venv', 'venv', '.mypy_cache'}",
        "    for dirpath, dirnames, filenames in os.walk(root):",
        "        dirnames[:] = [d for d in dirnames if d not in skip]",
        "        for f in filenames:",
        "            p = Path(dirpath) / f",
        "            if is_py(p): yield p",
        "",
        "def needs_repair(p: Path) -> bool:",
        "    try: return not ast_ok(read_text(p))",
        "    except: return True",
        "",
        "def repair_file(p: Path, limit: int, dry: bool) -> Tuple[bool, Tuple[str, ...]]:",
        "    src = read_text(p)",
        "    res = repair_code(src, limit)",
        "    if res.ok and not dry:",
        "        copy_backup(p)",
        "        write_text(p, res.code)",
        "    return res.ok, res.applied",
        "",
        "# ------------------------------- Report ------------------------------- #",
        "",
        "def fmt_result(p: Path, ok: bool, applied: Sequence[str]) -> str:",
        "    tag = 'FIXED' if ok else 'FAILED'",
        "    apps = ','.join(applied) or '-'",
        "    return f'{tag:6} {p} [{apps}]'",
        "",
        "def log(msg: str, quiet: bool) -> None:",
        "    if not quiet: print(msg)",
        "",
        "# --------------------------------- CLI -------------------------------- #",
        "",
        "def build_parser() -> argparse.ArgumentParser:",
        "    p = argparse.ArgumentParser(prog='py-syntax-repair')",
        "    p.add_argument('dir', nargs='?', default='.', help='root dir')",
        "    p.add_argument('--limit', type=int, default=5, help='max passes')",
        "    p.add_argument('--dry-run', action='store_true', help='no write')",
        "    p.add_argument('-q', '--quiet', action='store_true', help='quiet')",
        "    p.add_argument('--self-test', action='store_true', help='run tests')",
        "    return p",
        "",
        "# ------------------------------- Selftest ----------------------------- #",
        "",
        "def cases() -> Tuple[Case, ...]:",
        "    return (",
        "        Case('colon', 'if x == 1\\n    print(x)', 'if x == 1:\\n    print(x)\\n'),",
        "        Case('paren', 'x = (1 + 2\\n', 'x = (1 + 2)\\n'),",
        "        Case('triple', \"x = '''hello\\n\", \"x = '''hello'''\\n\"),",
        "        Case('single', \"x = 'hi\\n\", \"x = 'hi'\\n\"),",
        "        Case('eof', 'x=1', 'x=1\\n'),",
        "    )",
        "",
        "def run_tests() -> int:",
        "    fail = []",
        "    for c in cases():",
        "        res = repair_code(c.bad, 5)",
        "        if not (res.ok and res.code == c.good):",
        "            fail.append(c.name)",
        "    if fail:",
        "        print('FAIL:', ', '.join(fail))",
        "        return 1",
        "    print('All tests passed.')",
        "    return 0",
        "",
        "# --------------------------------- Main ------------------------------- #",
        "",
        "def main(argv: Optional[Sequence[str]] = None) -> int:",
        "    ns = build_parser().parse_args(argv)",
        "    if ns.self_test: return run_tests()",
        "    root = Path(ns.dir).resolve()",
        "    any_fail = False",
        "    for p in walk_py(root):",
        "        if needs_repair(p):",
        "            ok, apps = repair_file(p, ns.limit, ns.dry_run)",
        "            any_fail |= not ok",
        "            log(fmt_result(p, ok, apps), ns.quiet)",
        "    return 1 if any_fail else 0",
        "",
        "if __name__ == '__main__':",
        "    sys.exit(main())",
    ]
)

tests_py = _join(
    [
        "#!/usr/bin/env python3",
        "# -*- coding: utf-8 -*-",
        '"""Tests for py_syntax_repair_tool.py"""',
        "from __future__ import annotations",
        "import unittest",
        "from py_syntax_repair_tool import repair_code, Case",
        "",
        "class TestRepair(unittest.TestCase):",
        "    def assertRepairs(self, bad: str, good: str) -> None:",
        "        res = repair_code(bad, 5)",
        "        self.assertTrue(res.ok, f'Failed to fix: {res.applied}')",
        "        self.assertEqual(res.code, good)",
        "",
        "    def test_colon(self) -> None:",
        "        self.assertRepairs('if x == 1\\n    print(x)', 'if x == 1:\\n    print(x)\\n')",
        "",
        "    def test_paren(self) -> None:",
        "        self.assertRepairs('x = (1 + 2\\n', 'x = (1 + 2)\\n')",
        "",
        "    def test_triple(self) -> None:",
        "        self.assertRepairs(\"x = '''hello\\n\", \"x = '''hello'''\\n\")",
        "",
        "    def test_single(self) -> None:",
        "        self.assertRepairs(\"x = 'hi\\n\", \"x = 'hi'\\n\")",
        "",
        "    def test_eof(self) -> None:",
        "        self.assertRepairs('x=1', 'x=1\\n')",
        "",
        "if __name__ == '__main__':",
        "    unittest.main(verbosity=2)",
    ]
)

run_sh = _join(
    [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        'cd "$(dirname "$0")/py-syntax-repair"',
        'python3 py_syntax_repair_tool.py "${1:-.}"',
    ]
)

run_tests_sh = _join(
    [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        'cd "$(dirname "$0")/py-syntax-repair"',
        "python3 -m unittest discover -v",
    ]
)

run_bat = _join(
    [
        "@echo off",
        "cd /d %~dp0\\py-syntax-repair",
        "python py_syntax_repair_tool.py %*",
    ]
)

run_tests_bat = _join(
    [
        "@echo off",
        "cd /d %~dp0\\py-syntax-repair",
        "python -m unittest discover -v",
    ]
)

# --------------------------------- Main ---------------------------------- #
def main() -> None:
    _emit("py_syntax_repair_tool.py", tool_py)
    _emit("test_py_syntax_repair_tool.py", tests_py)
    _emit("run_tool.sh", run_sh)
    _emit("run_tests.sh", run_tests_sh)
    _emit("run_tool.bat", run_bat)
    _emit("run_tests.bat", run_tests_bat)
    # Make scripts executable
    for sh in ("run_tool.sh", "run_tests.sh"):
        (_p(sh)).chmod(0o755)


if __name__ == "__main__":
    main()
