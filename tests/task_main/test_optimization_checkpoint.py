import json
import math
from pathlib import Path
from types import SimpleNamespace

from scripts.task_main.run_matrix import load_completed_replay, output_hashes, write_json
from src.simulator.task_main.cache import TaskMainCache
from src.simulator.task_main.candidates import CandidateUniverse
from src.simulator.task_main.config import STAGE_C_VERSION, TaskMainConfig
from src.simulator.task_main.engine import TaskMainEngine
from src.simulator.task_main.equivalence import execution_projection
from src.simulator.task_main.history import ExternalDemandHistory
from src.simulator.task_main.metrics import write_outputs
from src.simulator.task_main.validation import gate_a_evidence


class OnePassTransfers:
    def __init__(self, rows):
        self.rows = rows
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        if self.iterations > 1:
            raise AssertionError("historical transfer ledger was rescanned per candidate")
        return iter(self.rows)


def test_candidate_inflight_ledger_is_indexed_once_per_opportunity(req, publish):
    universe = CandidateUniverse()
    universe.observe(req(path=(1, 2, 3)))
    caches = [TaskMainCache(8), TaskMainCache(8), TaskMainCache(8)]
    publish(caches[0], (1, 2, 3))
    transfers = OnePassTransfers([
        SimpleNamespace(type="PROACTIVE", status="IN_FLIGHT", target=1,
                        transferable_chain=(1, 2)),
        SimpleNamespace(type="PROACTIVE", status="COMPLETED", target=2,
                        transferable_chain=(1, 2, 3)),
        SimpleNamespace(type="REACTIVE", status="IN_FLIGHT", target=2,
                        transferable_chain=(1, 2, 3)),
    ])
    rows, reasons = universe.materialize(caches, (9, 0, 1), transfers)
    assert transfers.iterations == 1
    assert [(row.chain_id, row.target) for row in rows] == [
        ((1,), 2), ((1, 2), 2), ((1, 2, 3), 1),
    ]
    assert reasons == {}


def test_persistence_bisect_index_matches_original_window_scan(req):
    history = ExternalDemandHistory()
    def request(rid,time,tokens):
        pages=math.ceil(tokens/512)
        return req(rid,time,tuple(range(1,pages+1)),tokens=tokens)
    requests = [
        request(0,60000,4999), request(1,0,5000),
        request(2,60001,20000), request(3,120000,300001),
    ]
    for request in requests:
        history.observe(request)
    for now,window in ((60000,60000),(60001,60000),(120000,60000),(120000,120000)):
        expected=[0]*6
        for event_time,index in history.events[(1,)]:
            if now-window < event_time <= now:
                expected[index]+=1
        assert history.bucket_counts((1,),now,window)==tuple(expected)


def test_cache_summary_memoization_is_versioned_and_returns_independent_dicts(publish):
    cache=TaskMainCache(4)
    publish(cache,(1,))
    first=cache.summary(); second=cache.summary()
    assert first==second and first is not second
    first["published_pages"]=999
    assert cache.summary()["published_pages"]==1
    publish(cache,(1,2))
    assert cache.summary()["state_digest"]!=second["state_digest"]


def test_progress_callback_is_observational_only(req):
    config=TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy="PERSISTENCE")
    trace=[req(i,i*10,(1,2)) for i in range(5)]
    plain=TaskMainEngine(config).run(trace)
    updates=[]
    observed=TaskMainEngine(config).run(
        trace,progress_callback=lambda done,total,now: updates.append((done,total,now)),
        progress_every_requests=2,
    )
    assert updates==[(2,5,10),(4,5,30),(5,5,40)]
    assert execution_projection(plain)==execution_projection(observed)


def test_atomic_control_json_and_completed_replay_reload(req,tmp_path):
    control=tmp_path/"checkpoint.json"
    write_json(control,{"completed":1})
    write_json(control,{"completed":2})
    assert json.loads(control.read_text())=={"completed":2}
    assert not (tmp_path/"checkpoint.json.tmp").exists()

    config=TaskMainConfig(protocol_version=STAGE_C_VERSION,proactive_policy="NONE",
                          workload="conversation",line_id=1)
    result=TaskMainEngine(config).run([req()])
    result.summary["provenance"]["input_identity"]={"synthetic":True}
    folder=tmp_path/"conversation_line_1"/"run_1"
    write_outputs(folder,result)
    write_json(folder/"execution_evidence.json",gate_a_evidence(result))
    hashes=output_hashes(folder)
    repeat,entry,summary,evidence=load_completed_replay(tmp_path,config,1,hashes)
    assert repeat==1 and entry["status"]=="PASS"
    assert entry["output_sha256"]==hashes
    assert summary["provenance"]["input_identity"]=={"synthetic":True}
    assert evidence["execution_projection_sha256"]==entry["execution_projection_sha256"]
