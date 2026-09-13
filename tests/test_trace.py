import json
from pathlib import Path

import pytest

from src.simulator.trace import TraceRequest, load_trace


def test_partial_tail_is_preserved() -> None:
    request = TraceRequest.from_record(
        7,
        {"timestamp": 3, "input_length": 1025, "output_length": 4, "hash_ids": [1, 2, 3]},
    )
    assert request.block_ids == (1, 2, 3)
    assert request.block_valid_tokens == (512, 512, 1)


def test_exact_multiple_has_full_final_page() -> None:
    request = TraceRequest.from_record(
        0,
        {"timestamp": 0, "input_length": 1024, "output_length": 0, "hash_ids": [1, 2]},
    )
    assert request.block_valid_tokens == (512, 512)


def test_hash_length_must_equal_ceil() -> None:
    with pytest.raises(ValueError, match=r"ceil\(input_length/512\)"):
        TraceRequest.from_record(
            0,
            {"timestamp": 0, "input_length": 513, "output_length": 0, "hash_ids": [1]},
        )


def test_load_trace_checks_parent_consistency(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    records = [
        {"timestamp": 0, "input_length": 1024, "output_length": 0, "hash_ids": [1, 2]},
        {"timestamp": 1, "input_length": 1024, "output_length": 0, "hash_ids": [3, 2]},
    ]
    path.write_text("".join(json.dumps(x) + "\n" for x in records), encoding="utf-8")
    with pytest.raises(ValueError, match="conflicting parents"):
        load_trace(path)
