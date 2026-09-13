import json
import random

from src.simulator.full_o2a import checkpoint, load_checkpoint


def controller_state():
    return {
        "action_sequence": [{"timestamp": 1, "action_type": "NO_COPY"}],
        "decision_count": 1, "copy_selected_count": 0, "no_copy_count": 1,
        "copy_failed_count": 0, "branch_count": 2, "candidate_count": 1,
        "evaluated_action_count": 2, "selected_prefix_ranks": [],
        "selected_targets": [], "latest_state_fingerprint": "abc",
    }


class Result:
    arrival_ms = 0
    service_done_ms = 2


def test_checkpoint_contains_required_files_and_resumes(tmp_path):
    random.seed(7)
    location = checkpoint(
        tmp_path, "ToolAgent", "commit", "config", "trace", 50, 49_000,
        [(1.0, 2, 0, 1)], controller_state(), [Result()],
        {"request_count": 1, "peak_memory_used_pages": 10},
    )
    assert {path.name for path in location.iterdir()} == {
        "simulator_state.pkl", "oracle_controller_state.json", "metrics.json",
        "rng_state.pkl", "processed_request_state.json", "checkpoint_manifest.json",
    }
    index, actions, controller = load_checkpoint(
        location, "ToolAgent", "commit", "config", "trace")
    assert index == 50 and actions == [(1.0, 2, 0, 1)]
    assert controller["action_sequence"][0]["action_type"] == "NO_COPY"
    manifest = json.loads((location / "checkpoint_manifest.json").read_text())
    assert manifest["decision_index"] == 50


def test_resume_rejects_hash_mismatch(tmp_path):
    location = checkpoint(
        tmp_path, "ToolAgent", "commit", "config", "trace", 1, 0,
        [], controller_state(), [], {"request_count": 0, "peak_memory_used_pages": 0},
    )
    try:
        load_checkpoint(location, "ToolAgent", "commit", "wrong", "trace")
    except ValueError as error:
        assert "config_hash mismatch" in str(error)
    else:
        raise AssertionError("mismatched checkpoint was accepted")
