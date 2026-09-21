from types import SimpleNamespace

import pytest

from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse


def test_straight_path_includes_every_intermediate_full_prefix(req):
    universe = CandidateUniverse()
    universe.observe(req(path=(1, 2, 3)))
    assert universe.seen == {(1,), (1, 2), (1, 2, 3)}


def test_partial_tail_not_in_candidate_identity(req):
    universe = CandidateUniverse()
    universe.observe(req(path=(1, 2), tokens=600))
    assert universe.seen == {(1,)}
    universe.observe(req(1, path=(3,), tokens=1))
    assert universe.seen == {(1,)}


def test_future_and_unseen_published_prefix_never_materialize(req, publish):
    universe = CandidateUniverse()
    caches = [TaskMainCache(4), TaskMainCache(4)]
    publish(caches[0], (9,))
    universe.observe(req(path=(1,)))
    candidates, reasons = universe.materialize(caches, (0, 0), [])
    assert candidates == [] and reasons == {"NO_SOURCE": 1}
    assert (9,) not in universe.seen


def test_no_source_and_all_holders_are_illegal(req, publish):
    universe = CandidateUniverse()
    universe.observe(req())
    caches = [TaskMainCache(2), TaskMainCache(2)]
    assert universe.materialize(caches, (0, 0), [])[1] == {"NO_SOURCE": 1}
    for cache in caches:
        publish(cache, (1,))
    assert universe.materialize(caches, (0, 0), [])[1] == {"NO_TARGET": 1}


def test_source_smallest_holder_id_target_load_then_id(req, publish):
    universe = CandidateUniverse()
    universe.observe(req())
    caches = [TaskMainCache(2) for _ in range(4)]
    publish(caches[0], (1,))
    publish(caches[2], (1,))
    row = universe.materialize(caches, (100, 10, 0, 10), [])[0][0]
    assert row.source == 0 and row.target == 1
    assert universe.materialize(caches, (100, 10, 0, 9), [])[0][0].target == 3


@pytest.mark.parametrize("wire,blocked", [((1,), {(1,)}), ((1, 2), {(1,), (1, 2)}),
                                         ((1, 2, 3), {(1,), (1, 2), (1, 2, 3)})])
def test_equal_deeper_cover_but_shallower_does_not(req, publish, wire, blocked):
    universe = CandidateUniverse()
    universe.observe(req(path=(1, 2, 3)))
    caches = [TaskMainCache(4), TaskMainCache(4)]
    publish(caches[0], (1, 2, 3))
    inflight = [SimpleNamespace(type="PROACTIVE", status="IN_FLIGHT", target=1,
                                transferable_chain=wire)]
    rows, reasons = universe.materialize(caches, (0, 0), inflight)
    assert {row.chain_id for row in rows} == universe.seen - blocked
    assert reasons["INFLIGHT_COVERED"] == len(blocked)


def test_covered_target_excluded_before_selecting_next_lowest(req, publish):
    universe = CandidateUniverse()
    universe.observe(req())
    caches = [TaskMainCache(2) for _ in range(3)]
    publish(caches[0], (1,))
    flight = SimpleNamespace(type="PROACTIVE", status="IN_FLIGHT", target=1, transferable_chain=(1,))
    assert universe.materialize(caches, (9, 0, 5), [flight])[0][0].target == 2
    flight.status = "COMPLETED"
    assert universe.materialize(caches, (9, 0, 5), [flight])[0][0].target == 1


def test_capacity_and_target_affinity_do_not_affect_structural_ranking(req, publish):
    universe = CandidateUniverse()
    universe.observe(req(path=(1, 2)))
    caches = [TaskMainCache(2) for _ in range(3)]
    publish(caches[0], (1, 2))
    publish(caches[1], (8, 9))
    caches[1].pin((8, 9))
    publish(caches[2], (1,))
    rows, _ = universe.materialize(caches, (0, 0, 0), [])
    assert [(r.chain_id, r.target) for r in rows] == [((1,), 1), ((1, 2), 1)]
    assert caches[1].plan_temporary(1) is None
