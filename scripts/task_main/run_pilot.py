"""Run only the independent 0..16 minute Stage C pipeline pilot."""
from scripts.task_main.run_matrix import run_matrix

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="results/task_main/stage_c_pilot/run_20260917_04")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--progress-interval", type=int, default=60)
    parser.add_argument("--request-progress-every", type=int, default=250)
    args = parser.parse_args()
    run_matrix("pilot", args.output, args.workers, resume=args.resume,
               progress_interval=args.progress_interval,
               request_progress_every=args.request_progress_every)
