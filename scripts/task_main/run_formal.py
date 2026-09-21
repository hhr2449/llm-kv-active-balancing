"""Prepared formal driver. Not authorized or invoked in Stage C."""
import argparse
from scripts.task_main.run_matrix import run_matrix

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--formal-authorized", required=True, action="store_true",
                        help="Use only after explicit authorization of formal 14 runs")
    parser.parse_args()
    run_matrix("formal", "results/task_main/formal")
