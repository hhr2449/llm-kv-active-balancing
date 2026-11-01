import json
from pathlib import Path
import subprocess
import sys

import pytest

from src.simulator.task_main.isolation import dependency_audit

ROOT = Path(__file__).resolve().parents[2]


def test_ast_dependency_allowlist_is_clean():
    audit = dependency_audit()
    assert audit["status"] == "PASS" and audit["violations"] == []
    assert "src/simulator/task_main/engine.py" in audit["source_sha256"]


def test_execution_with_legacy_imports_blocked_at_import_time():
    code = '''
import sys
from src.simulator.task_main.isolation import install_legacy_import_guard
install_legacy_import_guard()
from src.simulator.task_main.config import TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.trace import TraceRequest
from src.simulator.task_main.transfer import IndependentTransfers
from src.simulator.task_main.cache import TaskMainCache
req = TraceRequest.from_record(0, {"timestamp": 0, "input_length": 512,
                                  "output_length": 0, "hash_ids": [1]})
assert TaskMainEngine(TaskMainConfig()).run([req]).validation["status"] == "PASS"
second = TraceRequest.from_record(1, {"timestamp": 1, "input_length": 512,
                                     "output_length": 0, "hash_ids": [1]})
reactive = TaskMainEngine(TaskMainConfig(routing_policy="R_REQ_KV_TASK")).run([req, second])
assert reactive.validation["status"] == "PASS" and len(reactive.transfer_records) == 1
source, target = TaskMainCache(2), TaskMainCache(2)
source.publish_prompt(source.plan_prompt((1,)), 0)
network = IndependentTransfers([source, target])
transfer = network.start(network.preflight(req, 1, 0, 1).plan, 0, 0)
network.complete(transfer.transfer_id, transfer.ready_time)
assert target.lookup((1,)) == 1
try:
    import src.simulator.service
except RuntimeError as error:
    assert "legacy import attempted" in str(error)
else:
    raise AssertionError("legacy import guard did not reject service module")
'''
    subprocess.run([sys.executable, "-c", code], cwd=ROOT, check=True, capture_output=True, text=True)


@pytest.mark.parametrize("policy,expected_transfers", [("r_aff", 0), ("r_req_kv_task", 2)])
def test_synthetic_cli_writes_records_validation_and_input_hashes(tmp_path, policy, expected_transfers):
    trace = tmp_path / "synthetic.jsonl"
    trace.write_text("".join(json.dumps({"timestamp": time, "input_length": 512,
                                        "output_length": 9999, "hash_ids": [1]}) + "\n"
                             for time in (0, 1, 1)))
    output = tmp_path / "output"
    command = [sys.executable, "-m", "scripts.task_main.run_stage_a", "--config",
               str(ROOT / f"configs/task_main/stage_a_{policy}.yaml"), "--trace", str(trace),
               "--output", str(output)]
    subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    summary = json.loads((output / "summary.json").read_text())
    assert summary["request_count"] == summary["opportunity_count"] == 3
    assert summary["transfer_count"] == expected_transfers
    assert len(summary["provenance"]["inputs"]["trace_sha256"]) == 64
    assert json.loads((output / "validation.json").read_text())["status"] == "PASS"
    assert (output / "request_records.jsonl").exists()
    transfers = (output / "transfer_records.jsonl").read_text().splitlines()
    assert len(transfers) == expected_transfers
    assert all(json.loads(row)["status"] == "COMPLETED" for row in transfers)
    before = (output / "summary.json").read_bytes()
    refused = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    assert refused.returncode != 0 and "new directory" in refused.stderr
    assert (output / "summary.json").read_bytes() == before
