import pytest

from src.simulator.metrics import gini


def test_gini_all_zero_is_zero_without_epsilon() -> None:
    assert gini([0, 0, 0, 0]) == 0.0


def test_gini_known_distribution() -> None:
    assert gini([0, 0, 0, 4]) == pytest.approx(0.75)
