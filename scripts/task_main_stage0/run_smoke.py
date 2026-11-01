"""Synthetic only: write versioned diagnostics, never replay a formal trace."""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from src.simulator.task_main.stage0.config import CapacityStudyConfig
from src.simulator.task_main.stage0.engine import CapacityStudyEngine
from src.simulator.task_main.trace import TraceRequest
from src.simulator.task_main.metrics import write_outputs


def request(rid,t,path,tokens=None):
    return TraceRequest.from_record(rid,dict(timestamp=t,input_length=tokens or len(path)*512,
                                            output_length=0,hash_ids=list(path)))


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False)
    cases=[]
    for mode in ['finite','infinite']:
        for route,policy in [('R_AFF','NONE'),('R_LEAST','NONE'),('R_REQ_KV_TASK','NONE'),
                             ('R_AFF','PERSISTENCE'),('R_AFF','RECENCY'),('R_AFF','FUTURE_DEMAND'),
                             ('R_AFF','PERSISTENCE_COST_AWARE')]:
            cfg=CapacityStudyConfig(capacity_mode=mode,capacity_pages=4 if mode=='finite' else None,
                                    routing_policy=route,proactive_policy=policy)
            rows=[request(i,1500000+i,(100+i,)) for i in range(16)]
            rows += [request(16,1500020,(1,2),600),request(17,1500021,(1,2),600),
                     request(18,1500021.1,(1,2),600),request(19,1500040,(1,2),600)]
            first=CapacityStudyEngine(cfg).run(rows);second=CapacityStudyEngine(cfg).run(rows)
            def digest(result):return hashlib.sha256(json.dumps(asdict(result),sort_keys=True,allow_nan=False).encode()).hexdigest()
            one,two=digest(first),digest(second);assert one==two
            name=f'{mode}__{route}__{policy}'
            write_outputs(a.output/name,first)
            (a.output/name/'stage0_diagnostics.json').write_text(json.dumps(first.summary['stage0_diagnostics'],indent=2,allow_nan=False))
            (a.output/name/'config.json').write_text(json.dumps(cfg.as_dict(),indent=2))
            cases.append(dict(case=name,status='PASS',determinism_sha256=one,request_count=len(rows),
                              checks=first.validation['checks']))
    (a.output/'smoke_validation.json').write_text(json.dumps(dict(status='PASS',case_count=len(cases),cases=cases),indent=2))
    print(f'STAGE0_SMOKE_PASS: {len(cases)} cases, two deterministic runs each')


if __name__=='__main__':main()
