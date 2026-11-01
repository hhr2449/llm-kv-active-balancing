"""Stage 0 dependency identity includes its versioned extension modules."""
import ast
import hashlib
from importlib.util import resolve_name
from pathlib import Path
import sys

from ..isolation import dependency_audit


def stage0_dependency_audit():
    base=dependency_audit();hashes={};violations=list(base['violations'])
    for path in sorted(Path(__file__).parent.glob('*.py')):
        data=path.read_bytes();hashes['src/simulator/task_main/stage0/'+path.name]=hashlib.sha256(data).hexdigest()
        for node in ast.walk(ast.parse(data)):
            modules=[]
            if isinstance(node,ast.Import):modules=[a.name for a in node.names]
            elif isinstance(node,ast.ImportFrom):
                modules=[resolve_name('.'*node.level+(node.module or ''),'src.simulator.task_main.stage0') if node.level else node.module]
            for module in modules:
                if not (module.startswith('src.simulator.task_main') or module=='yaml' or module.split('.')[0] in sys.stdlib_module_names):
                    violations.append(f'{path.name}:{node.lineno}: {module}')
    return dict(status='FAIL' if violations else 'PASS',violations=violations,
                source_sha256={**base['source_sha256'],**hashes})
