import subprocess
import sys

import pytest

from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.history import ExternalDemandHistory, ReuseHistory
from src.simulator.task_main.isolation import dependency_audit
from src.simulator.task_main.policies import Policy


@pytest.mark.parametrize("policy", ["FUTURE_DEMAND", "PERSISTENCE", "RECENCY", "PERSISTENCE_COST_AWARE"])
def test_proactive_requires_r_aff(cfg, policy):
    with pytest.raises(ValueError, match="R_AFF"):
        cfg(policy, routing_policy="R_REQ_KV_TASK")


@pytest.mark.parametrize("kwargs", [{"proactive_policy": "O2"}, {"future_window_ms": 1800000},
    {"shortlist_k": 11}, {"recency_quantile": .8}, {"recency_decay_ms": 1},
    {"visibility_end_ms": 3536999}, {"history_window_ms": 1}])
def test_stage_b_allowlist_frozen_constants(cfg, kwargs):
    policy = kwargs.pop("proactive_policy", "PERSISTENCE")
    with pytest.raises(ValueError):
        cfg(policy, **kwargs)


@pytest.mark.parametrize("policy", ["NONE", "PERSISTENCE", "RECENCY", "PERSISTENCE_COST_AWARE"])
def test_history_policies_never_construct_or_receive_future_index(req, cfg, monkeypatch, policy):
    def forbidden(*args, **kwargs):
        raise AssertionError("history policy tried to construct future index")
    monkeypatch.setattr("src.simulator.task_main.engine.FutureDemandIndex", forbidden)
    sim = TaskMainEngine(cfg(policy))
    sim.run([req(), req(1, 10)])
    assert sim.future_index is None
    with pytest.raises(ValueError, match="future index"):
        Policy(cfg(policy), ExternalDemandHistory(), ReuseHistory(), object())


def test_stage_b_dependency_check_and_all_policies_under_import_guard():
    assert dependency_audit()["status"] == "PASS"
    code = '''
from src.simulator.task_main.isolation import install_legacy_import_guard
install_legacy_import_guard()
from src.simulator.task_main.config import TaskMainConfig, STAGE_B_VERSION
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.trace import TraceRequest
requests = [TraceRequest.from_record(i, dict(timestamp=i*10, input_length=1024,
             output_length=0, hash_ids=[1,2])) for i in range(3)]
for policy in ('NONE','FUTURE_DEMAND','PERSISTENCE','RECENCY','PERSISTENCE_COST_AWARE'):
    result = TaskMainEngine(TaskMainConfig(protocol_version=STAGE_B_VERSION,
                            proactive_policy=policy)).run(requests)
    assert result.validation['status']=='PASS'
    assert all(r.routing_policy=='R_AFF' for r in result.request_records)
    if policy != 'NONE': assert result.proactive_action_records
'''
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


@pytest.mark.parametrize("field", ["queue", "o2", "ect", "node_gate", "timeout",
                                  "control_latency", "trigger_tick", "byte_budget", "V_ref", "T_ref"])
def test_stage_b_yaml_rejects_legacy_fields(tmp_path, field):
    from src.simulator.task_main.config import TaskMainConfig
    path = tmp_path/"bad.yaml"
    path.write_text(f"protocol_version: TASK_MAIN_V1_STAGE_B\n{field}: 1\n")
    with pytest.raises(ValueError, match="unsupported"):
        TaskMainConfig.from_yaml(path)
