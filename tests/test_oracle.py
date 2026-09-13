import pytest

from src.simulator.oracle import (CandidateGenerator, FutureDemandIndex,
                                  OracleLeakageError, TokenBucket, trigger_times)
from src.simulator.pod import Pod
from src.simulator.trace import TraceRequest


def req(i, t, blocks):
    return TraceRequest.from_record(i, {
        "timestamp": t, "input_length": 512 * len(blocks),
        "output_length": 0, "hash_ids": blocks,
    })


def test_future_count_interval_is_open_closed():
    index = FutureDemandIndex([req(0, 10, [1]), req(1, 20, [1]), req(2, 30, [1])])
    assert index.future_count(1, 10, 20) == 2


def test_current_request_is_not_future():
    assert FutureDemandIndex([req(0, 10, [1])]).future_count(1, 10, 10) == 0


def test_horizon_boundary_is_future():
    assert FutureDemandIndex([req(0, 20, [1])]).future_count(1, 10, 10) == 1


def test_duplicate_hash_in_one_external_request_counts_once():
    malformed_but_indexable = req(0, 20, [1, 2])
    object.__setattr__(malformed_but_indexable, "block_ids", (1, 1))
    assert FutureDemandIndex([malformed_but_indexable]).future_count(1, 0, 30) == 1


def test_development_leakage_gate_rejects_cross_boundary_read():
    with pytest.raises(OracleLeakageError):
        FutureDemandIndex([]).future_count(1, 890, 20, allowed_end_ms=900)


def test_censored_window_is_not_returned_as_zero_demand():
    index = FutureDemandIndex([])
    with pytest.raises(OracleLeakageError):
        index.future_count(1, 899, 30, allowed_end_ms=900)


def test_unseen_future_prefix_is_not_candidate():
    generator = CandidateGenerator()
    pod = Pod(0)
    pod.cache.publish([99])
    assert generator.generate([pod, Pod(1)]) == []


def test_online_known_published_prefix_is_candidate_with_source():
    generator = CandidateGenerator(); generator.observe(req(0, 0, [1, 2]))
    pods = [Pod(0), Pod(1)]; pods[0].cache.publish([1, 2])
    candidates = generator.generate(pods)
    assert [(c.hash_id, c.source_pods) for c in candidates] == [(2, (0,))]


def test_candidate_without_current_source_is_excluded():
    generator = CandidateGenerator(); generator.observe(req(0, 0, [1]))
    assert generator.generate([Pod(0), Pod(1)]) == []


def test_fully_replicated_target_chain_is_excluded():
    generator = CandidateGenerator(); generator.observe(req(0, 0, [1]))
    pods = [Pod(0), Pod(1)]
    for pod in pods: pod.cache.publish([1])
    assert generator.generate(pods) == []


def test_oracle_ranking_key_is_demand_depth_then_id():
    generator = CandidateGenerator(); generator.observe(req(0, 0, [2, 3])); generator.observe(req(1, 0, [1])); generator.observe(req(2, 0, [2, 4]))
    pods = [Pod(0), Pod(1)]; pods[0].cache.publish([2, 3]); pods[0].cache.publish([2, 4]); pods[0].cache.publish([1])
    candidates = generator.generate(pods)
    ranked = sorted([(2, c) for c in candidates], key=lambda x: (-x[0], -x[1].depth, x[1].hash_id))
    assert [c.hash_id for _, c in ranked] == [3, 4, 1, 2]


def test_trigger_period_and_phase():
    assert list(trigger_times(6, 2, 20)) == [2, 8, 14]


def test_token_bucket_refill_and_start_deduction():
    bucket = TokenBucket(1000, 1000)
    assert bucket.consume(800, 0) and bucket.tokens == 200
    assert bucket.consume(700, 500) and bucket.tokens == 0


def test_token_bucket_insufficient_is_skip_without_deduction():
    bucket = TokenBucket(0, 10)
    assert not bucket.consume(11, 0)
    assert bucket.tokens == 10


def test_token_bucket_never_exceeds_burst():
    bucket = TokenBucket(1000, 100)
    bucket.refill(10000)
    assert bucket.tokens == 100
