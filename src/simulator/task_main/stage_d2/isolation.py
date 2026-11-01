from __future__ import annotations

import ast
import hashlib
from pathlib import Path

from ..isolation import FORBIDDEN_SYMBOLS, dependency_audit


def stage_d2_dependency_audit():
    base = dependency_audit()
    directory = Path(__file__).resolve().parent
    violations = list(base["violations"])
    hashes = dict(base["source_sha256"])
    for path in sorted(directory.glob("*.py")):
        relative = f"src/simulator/task_main/stage_d2/{path.name}"
        source = path.read_bytes()
        hashes[relative] = hashlib.sha256(source).hexdigest()
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            symbol = (node.id if isinstance(node, ast.Name) else
                      node.attr if isinstance(node, ast.Attribute) else None)
            if symbol in FORBIDDEN_SYMBOLS:
                violations.append(f"{relative}:{node.lineno}: forbidden reference {symbol}")
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module and node.module.startswith("src.simulator.") and not node.module.startswith("src.simulator.task_main"):
                violations.append(f"{relative}:{node.lineno}: forbidden import {node.module}")
    return {"status": "PASS" if not violations else "FAIL", "violations": violations,
            "source_sha256": hashes, "base_audit": base}
