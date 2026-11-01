import pytest

from src.simulator.task_main.config import STAGE_B_VERSION, TaskMainConfig
from src.simulator.task_main.trace import TraceRequest


@pytest.fixture
def req():
    def make(rid=0, time=0, path=(1,), tokens=None):
        return TraceRequest.from_record(rid, {
            "timestamp": time, "input_length": len(path)*512 if tokens is None else tokens,
            "output_length": 0, "hash_ids": list(path),
        })
    return make


@pytest.fixture
def cfg():
    def make(policy="PERSISTENCE", **kwargs):
        return TaskMainConfig(protocol_version=STAGE_B_VERSION, proactive_policy=policy, **kwargs)
    return make


@pytest.fixture
def publish():
    def do(cache, path, time=0):
        plan = cache.plan_prompt(path)
        assert plan is not None
        cache.publish_prompt(plan, time)
    return do
