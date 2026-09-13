from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path


REQUIRED = {
    "simulator_state.pkl", "oracle_controller_state.json", "metrics.json",
    "rng_state.pkl", "processed_request_state.json", "checkpoint_manifest.json",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate a Full O2-A checkpoint across a code-only commit")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--new-commit")
    args = parser.parse_args()
    present = {path.name for path in args.source.iterdir() if path.is_file()}
    if not REQUIRED <= present:
        raise ValueError(f"incomplete checkpoint: missing {sorted(REQUIRED - present)}")
    if args.destination.exists():
        raise FileExistsError(f"destination exists: {args.destination}")
    new_commit = args.new_commit or subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    shutil.copytree(args.source, args.destination)
    manifest_path = args.destination / "checkpoint_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    old_commit = manifest["git_commit"]
    manifest.update({
        "git_commit": new_commit,
        "migrated_from_git_commit": old_commit,
        "migration_scope": "parallel branch evaluation only; policy/config/trace unchanged",
    })
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(args.destination)


if __name__ == "__main__":
    main()
