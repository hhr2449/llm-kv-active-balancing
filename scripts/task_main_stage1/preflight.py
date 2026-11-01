"""Portable preflight receipt plus real-trace optimization equivalence evidence."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import pickle
import resource
import time
import xml.etree.ElementTree as ET

from src.simulator.task_main.stage0.engine import CapacityStudyEngine
from src.simulator.task_main.trace import read_trace
from .optimized import OptimizedStudyEngine
from .diagnostics import build
from .run import ROOT, config_for, atomic, source_identity, digest, verify_done, sha


def compare(workload, output):
    output=Path(output)
    if output.exists():raise ValueError('Refusing to overwrite equivalence evidence')
    cfg=config_for(workload,'R_REQ_KV_TASK',1170)
    inputs=[r for r in read_trace(ROOT/cfg.trace_path) if r.arrival_ms<120000]
    results=[];times=[];diagnostics=[]
    for cls in [CapacityStudyEngine,OptimizedStudyEngine]:
        start=time.perf_counter();engine=cls(cfg);result=engine.run(inputs)
        diagnostics.append(digest(build(result,engine,inputs,120000)))
        times.append(time.perf_counter()-start);results.append(digest(asdict(result)))
    assert results[0]==results[1] and diagnostics[0]==diagnostics[1]
    atomic(output,dict(status='PASS',workload=workload,request_count=len(inputs),interval_ms=[0,120000],
        reference_wall_seconds=times[0],optimized_wall_seconds=times[1],speedup=times[0]/times[1],
        complete_run_result_sha256=results[0],diagnostics_sha256=diagnostics[0],source_sha256=source_identity()))
    print(workload,'optimization equivalence PASS',times,flush=True)


def receipt(root, junit, output):
    root=Path(root);junit=Path(junit)
    tree=ET.parse(junit).getroot();cases=tree.findall('.//testcase')
    assert cases and not any(tree.findall('.//'+key) for key in ['failure','error','skipped'])
    evidence=[]
    for w in ['conversation','toolagent']:
        equivalence=json.loads((root/f'equivalence_{w}.json').read_text())
        assert equivalence['status']=='PASS' and equivalence['source_sha256']==source_identity()
        pilot=root/('pilot_'+w)
        manifest=json.loads((pilot/'manifest.json').read_text())
        assert manifest['source_sha256']==source_identity()
        for route,cap in [('R_AFF','585'),('R_REQ_KV_TASK','585'),('R_REQ_KV_TASK','infinite')]:
            path=pilot/w/(route+'__'+cap)
            marker=json.loads((path/'COMPLETE.json').read_text())
            assert verify_done(path,marker['identity']) and marker['identity']['kind']=='pilot'
            validation=json.loads((path/'stage1_validation.json').read_text())
            assert validation['status']=='PASS' and validation['deterministic']
            evidence.append(dict(case=w+'/'+path.name,complete_sha256=sha(path/'COMPLETE.json'),validation=validation))
    value=dict(status='PASS',source_sha256=source_identity(),pytest_count=len(cases),junit_sha256=sha(junit),
               pilot_case_count=6,pilot_interval_ms=[0,120000],pilot_cases=evidence,
               equivalence_evidence=[json.loads((root/f'equivalence_{w}.json').read_text()) for w in ['conversation','toolagent']],
               formal_run_started=False,notes='Portable receipt validates exact source/config/tests content across commit/servers; pilot is short input only, boundary semantics separately covered by synthetic tests.')
    atomic(output,value);print('PREFLIGHT_PASS',len(cases),'tests, 6 deterministic pilot cases',flush=True)


def main():
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['equivalence','receipt'])
    p.add_argument('--workload',choices=['conversation','toolagent']);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--root',type=Path);p.add_argument('--junit',type=Path);a=p.parse_args()
    if a.mode=='equivalence':compare(a.workload,a.output)
    else:receipt(a.root,a.junit,a.output)


if __name__=='__main__':main()
