from dataclasses import replace

from src.simulator.config import SimulatorConfig, SplitConfig
from src.simulator.engine import SimulatorEngine
from src.simulator.o2p import HORIZON_MS, cohort, run_branch, select_oracle
from src.simulator.o2a import choose_action, classify, spearman
from src.simulator.o2a_smoke import fingerprint, replay
from src.simulator.trace import TraceRequest


SPLIT = SplitConfig(0, 900_000, 1_500_000, 2_700_000)
T = 1_500_000.0


def req(i, arrival, blocks, tokens=None):
    tokens = tokens or 512 * len(blocks)
    return TraceRequest.from_record(i, {
        "timestamp": arrival, "input_length": tokens, "output_length": 0,
        "hash_ids": blocks,
    }, split_config=SPLIT)


BASE = SimulatorConfig(
    page_tokens=512, base_latency_ms=2, prefill_tokens_per_second=10_000,
    cache_capacity_pages=20, num_pods=4, routing_policy="R_REQ_KV",
    transfer_enabled=True, page_bytes=1,
    effective_bandwidth_bytes_per_s=1_000_000, control_latency_ms=1,
    cache_hit_threshold=0.3, relative_load_threshold=1.5,
    absolute_load_gap_ms=500, split_config=SPLIT, summary_split="EVALUATION",
    proactive_enabled=True, trigger_period_ms=1000, trigger_phase_ms=T,
    max_proactive_actions_per_tick=1, proactive_byte_rate=1000,
    proactive_burst_bytes=1000, proactive_initial_tokens=1000,
    oracle_horizon_ms=HORIZON_MS, oracle_visibility_end_ms=3_537_000,
    protocol_version="TASKMAIN_V1_1", proactive_action="O2P_PROBE",
    proactive_trigger_latest_ms=T,
)


REQUESTS = [
    req(0, 0, [1]),
    req(1, T + 100, [1]),
    req(2, T + HORIZON_MS, [1, 2]),
    req(3, T + HORIZON_MS + 1, [3]),
]


def state_and_probe():
    results, summary = SimulatorEngine(BASE).run(REQUESTS)
    states = summary["proactive"]["o2p_candidate_states"]
    assert len(states) == 1
    return states[0], results, summary


def test_probe_enumerates_multiple_targets_without_mutation():
    state, _, summary = state_and_probe()
    assert len(state["legal_targets"]) >= 2
    assert state["o1_target"] in state["legal_targets"]
    assert summary["proactive"]["actions_started"] == 0


def test_branches_share_identical_initial_state_and_are_isolated():
    state, _, _ = state_and_probe()
    no_results_1, no_summary_1, no_metrics_1 = run_branch(BASE, REQUESTS, state, None)
    target = state["legal_targets"][0]
    _, copy_summary, copy_metrics = run_branch(BASE, REQUESTS, state, target)
    no_results_2, no_summary_2, no_metrics_2 = run_branch(BASE, REQUESTS, state, None)
    assert state["state_fingerprint"] == no_metrics_1["state_fingerprint"] == copy_metrics["state_fingerprint"]
    assert no_results_1 == no_results_2
    assert no_summary_1 == no_summary_2
    assert no_metrics_1 == no_metrics_2
    assert no_summary_1["proactive"]["actions_started"] == 0
    assert copy_summary["proactive"]["actions_started"] == 1
    assert copy_summary["proactive"]["actions_completed"] == 1


def test_copy_branch_has_only_one_action_and_no_later_proactive():
    state, _, _ = state_and_probe()
    _, summary, metrics = run_branch(BASE, REQUESTS, state, state["legal_targets"][0])
    records = summary["proactive"]["action_records"]
    assert metrics["action_success"]
    assert len(records) == 1
    assert records[0]["trigger_time"] == T


def test_all_branches_use_same_future_sequence_and_cutoff_then_drain():
    state, _, _ = state_and_probe()
    no_results, _, _ = run_branch(BASE, REQUESTS, state, None)
    copy_results, _, _ = run_branch(BASE, REQUESTS, state, state["legal_targets"][0])
    no_ids = [r.request_id for r in cohort(no_results, T)]
    copy_ids = [r.request_id for r in cohort(copy_results, T)]
    assert no_ids == copy_ids == [1, 2]
    assert all(r.request_id != 3 for r in no_results + copy_results)
    assert next(r for r in no_results if r.request_id == 2).service_done_ms > T + HORIZON_MS


def test_illegal_or_failed_target_is_not_successful_copy():
    state, _, _ = state_and_probe()
    illegal = dict(state)
    illegal["legal_targets"] = [state["source_pod"]]
    _, summary, metrics = run_branch(BASE, REQUESTS, illegal, state["source_pod"])
    assert not metrics["action_success"]
    assert summary["proactive"]["actions_started"] == 0
    assert metrics["action_failure_reason"] == "branch_action_not_admitted"


def test_no_copy_can_be_oracle_optimum_and_ties_count_as_best():
    branches = [
        {"target_pod": "NO_COPY", "action_success": True, "delta_loss_total": 0},
        {"target_pod": 1, "action_success": True, "delta_loss_total": -1},
        {"target_pod": 2, "action_success": True, "delta_loss_total": -1},
    ]
    best, o1, action = select_oracle(branches, 2)
    assert best["target_pod"] == 1
    assert o1["target_pod"] == 2
    assert action == "NO_COPY"


def test_repeated_probe_and_branch_replay_are_deterministic():
    assert SimulatorEngine(BASE).run(REQUESTS) == SimulatorEngine(BASE).run(REQUESTS)
    state, _, _ = state_and_probe()
    assert run_branch(BASE, REQUESTS, state, None) == run_branch(BASE, REQUESTS, state, None)


def test_budget_failure_is_not_reported_as_copy_success():
    state, _, _ = state_and_probe()
    no_budget = replace(BASE, proactive_byte_rate=0, proactive_burst_bytes=0,
                        proactive_initial_tokens=0)
    _, summary, metrics = run_branch(no_budget, REQUESTS, state, state["legal_targets"][0])
    assert summary["proactive"]["actions_started"] == 0
    assert not metrics["action_success"]


def test_o2a_probe_reuses_o2p_state_and_contains_o1_top_prefix():
    state, _, _ = state_and_probe()
    config = replace(BASE, proactive_action="O2A_PROBE")
    _, summary = SimulatorEngine(config).run(REQUESTS)
    probe = summary["proactive"]["o2a_candidate_states"][0]
    assert probe["state_fingerprint"] == state["state_fingerprint"]
    assert probe["candidates"][0]["prefix_id"] == state["prefix_id"]
    assert len(probe["candidates"]) <= 5


def test_o2a_action_tie_break_and_no_copy_option():
    common = {"action_success": True, "prefix_depth_pages": 2,
              "delta_loss_total": 4, "copy_bytes": 10}
    best, action = choose_action([
        dict(common, prefix_id=9, target_pod=2),
        dict(common, prefix_id=8, target_pod=3),
    ])
    assert action == "COPY" and best["prefix_id"] == 8
    best, action = choose_action([
        dict(common, prefix_id=8, target_pod=1, delta_loss_total=0),
    ])
    assert action == "NO_COPY" and best is not None


def test_o2a_spearman_ties_and_classifications():
    assert spearman([1, 2, 3], [10, 20, 30]) == 1
    assert spearman([1, 1], [2, 3]) is None
    assert classify(True, False, False, False) == "TYPE_0_NO_OPPORTUNITY"
    assert classify(False, True, False, True) == "TYPE_3_O1_ALREADY_GOOD"
    assert classify(False, False, True, False) == "TYPE_2_PREFIX_SELECTION"


def test_closed_loop_probe_and_committed_action_share_pre_action_state():
    no_results, no_summary = replay(BASE, REQUESTS, [], T)
    candidate = no_summary["proactive"]["o2a_candidate_states"][0]["candidates"][0]
    target = candidate["legal_targets"][0]
    action = (T, candidate["prefix_id"], candidate["source_pod"], target)
    copy_results, copy_summary = replay(BASE, REQUESTS, [action], T)
    assert fingerprint(no_summary, T) == fingerprint(copy_summary, T)
    assert fingerprint(no_summary, T, True) == fingerprint(copy_summary, T, True)
    assert [r.request_id for r in cohort(no_results, T)] == [
        r.request_id for r in cohort(copy_results, T)]
    current = [record for record in copy_summary["proactive"]["action_records"]
               if record["trigger_time"] == T]
    assert len(current) == 1
