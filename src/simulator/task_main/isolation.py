from __future__ import annotations

import ast
import hashlib
import importlib.abc
from importlib.util import resolve_name
from pathlib import Path
import sys

PACKAGE = "src.simulator.task_main"
SAFE_SHARED_MODULE = "src.simulator.trace"
FORBIDDEN_SYMBOLS = frozenset({
    "PrefillServiceModel", "service_ms", "load_ms", "RemotePressure",
    "ReactiveNodeGate", "NodeGate", "TokenBucket", "TRIGGER_TICK",
    "SERVICE_START", "SERVICE_DONE", "cost_v_ref", "cost_t_ref_ms",
})


def install_legacy_import_guard() -> None:
    """Fail immediately on forbidden legacy imports, including already loaded ones."""
    allowed = (PACKAGE, SAFE_SHARED_MODULE, "src.simulator.config")

    def forbidden(name: str) -> bool:
        return name.startswith("src.simulator.") and not any(
            name == item or name.startswith(item + ".") for item in allowed
        )

    loaded = [name for name in sys.modules if forbidden(name)]
    if loaded:
        raise RuntimeError(f"legacy modules already loaded: {sorted(loaded)}")

    class RejectLegacy(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if forbidden(fullname):
                raise RuntimeError(f"legacy import attempted: {fullname}")
            return None

    sys.meta_path.insert(0, RejectLegacy())


def dependency_audit() -> dict:
    """Audit executable imports/references, not whether old config switches are off."""
    package_dir = Path(__file__).resolve().parent
    files = sorted(package_dir.glob("*.py"))
    files.append(package_dir.parent / "trace.py")
    edges: list[tuple[str, str]] = []
    violations: list[str] = []
    hashes: dict[str, str] = {}
    for path in files:
        source = path.read_bytes()
        relative = ("trace.py" if path.parent != package_dir else f"task_main/{path.name}")
        hashes[f"src/simulator/{relative}"] = hashlib.sha256(source).hexdigest()
        tree = ast.parse(source, filename=str(path))
        package = PACKAGE if path.parent == package_dir else "src.simulator"
        for node in ast.walk(tree):
            imports = []
            if isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imports = [resolve_name("." * node.level + (node.module or ""), package)
                           if node.level else (node.module or "")]
            for module in imports:
                edges.append((relative, module))
                allowed = (module == PACKAGE or module.startswith(PACKAGE + ".")
                           or module == SAFE_SHARED_MODULE
                           or module.split(".")[0] in sys.stdlib_module_names
                           or module == "yaml")
                if not allowed:
                    violations.append(f"{relative}: forbidden import {module}")
            symbol = (node.id if isinstance(node, ast.Name) else
                      node.attr if isinstance(node, ast.Attribute) else None)
            if symbol in FORBIDDEN_SYMBOLS:
                violations.append(f"{relative}:{node.lineno}: forbidden reference {symbol}")
    return {
        "status": "PASS" if not violations else "FAIL",
        "checked_modules": sorted(hashes), "source_sha256": hashes,
        "import_edges": sorted(edges), "violations": violations,
        "safe_shared_primitive": SAFE_SHARED_MODULE,
        "parent_package_note": (
            "Python loads src.simulator.__init__, which exports the legacy config "
            "class and trace types. TaskMain never instantiates that config or "
            "imports the legacy engine, Pod, service, routing, or policy modules."
        ),
    }
