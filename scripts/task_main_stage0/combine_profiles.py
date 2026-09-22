"""Combine independent workload CSVs; refuse to overwrite prior summaries."""
import argparse
import csv
from pathlib import Path

from .profile_workload import write_csv


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args()
    names=['workload_capacity_profile','working_set_summary','prompt_bucket_support']
    targets=[args.root/(name+'.csv') for name in names]
    if any(p.exists() for p in targets):
        raise SystemExit('Refusing to overwrite existing combined profiles')
    combined=[]
    for name in names:
        rows=[]
        for workload in ['conversation','toolagent']:
            with (args.root/'profiles'/workload/(name+'.csv')).open() as f:
                rows.extend(csv.DictReader(f))
        combined.append(rows)
    for target,rows in zip(targets,combined):write_csv(target,rows)


if __name__=='__main__':main()
