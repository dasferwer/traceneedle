import pytest


def test_pair_metrics_expose_both_overmerging_and_splitting():
    from scripts.evaluate_real_logs import pair_scores

    # A split loses one true pair; a merge adds two false pairs.
    scores = pair_scores(["a", "a", "b", "b"], [1, 1, 1, 2])
    assert scores["pair_precision"] == pytest.approx(1 / 3)
    assert scores["pair_recall"] == 0.5
    assert scores["false_merged_pairs"] == 2
    assert scores["missed_same_template_pairs"] == 1
