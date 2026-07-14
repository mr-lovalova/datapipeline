import pytest

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector.drop.horizontal import (
    DropSamplesTransform,
    DropTargetSamplesTransform,
)
from datapipeline.transforms.vector.drop.vertical import (
    ColumnCoverage,
    SelectFeaturesTransform,
    SelectTargetsTransform,
)
from tests.unit.transforms.helpers import make_vector


def test_drop_samples_uses_scalar_and_sequence_cell_coverage() -> None:
    samples = [
        make_vector(0, {"scalar": 1.0, "sequence": [1.0, None, None]}),
        make_vector(1, {"scalar": 1.0, "sequence": [1.0, 2.0, 3.0]}),
    ]
    transform = DropSamplesTransform(
        ["scalar", "sequence"],
        threshold=0.7,
    )

    output = list(transform.apply(iter(samples)))

    assert output == [samples[1]]


def test_drop_samples_honors_explicit_ids() -> None:
    sample = make_vector(0, {"required": 1.0, "ignored": None})
    transform = DropSamplesTransform(
        ["required", "ignored"],
        threshold=1.0,
        ids=["required"],
    )

    assert list(transform.apply(iter([sample]))) == [sample]


def test_drop_samples_rejects_unknown_ids() -> None:
    with pytest.raises(ValueError, match="Unknown vector ids"):
        DropSamplesTransform(["a"], threshold=1.0, ids=["typo"])


def test_drop_samples_counts_categorical_values_as_present() -> None:
    sample = make_vector(0, {"category": "healthcare"})
    transform = DropSamplesTransform(["category"], threshold=1.0)

    assert list(transform.apply(iter([sample]))) == [sample]


def test_drop_target_samples_counts_absent_target_vector_as_missing() -> None:
    sample = make_vector(0, {"feature": 1.0})
    transform = DropTargetSamplesTransform(["target"], threshold=0.1)

    assert list(transform.apply(iter([sample]))) == []


def test_select_features_drops_low_coverage_columns_without_context_mutation() -> None:
    transform = SelectFeaturesTransform(
        ["sparse", "complete"],
        [
            ColumnCoverage("sparse", "scalar", present_count=4, null_count=3),
            ColumnCoverage("complete", "scalar", present_count=4),
        ],
        total_opportunities=4,
        threshold=0.8,
    )
    sample = make_vector(0, {"sparse": 1.0, "complete": 2.0})

    output = list(transform.apply(iter([sample])))

    assert transform.retained_ids == ("complete",)
    assert transform.dropped_ids == frozenset({"sparse"})
    assert output[0].features.values == {"complete": 2.0}


def test_select_features_only_evaluates_explicit_ids() -> None:
    transform = SelectFeaturesTransform(
        ["selected", "unselected"],
        [ColumnCoverage("selected", "scalar", present_count=0)],
        total_opportunities=10,
        threshold=0.5,
        ids=["selected"],
    )

    assert transform.retained_ids == ("unselected",)


def test_sequence_coverage_uses_every_expected_element() -> None:
    transform = SelectFeaturesTransform(
        ["sequence"],
        [
            ColumnCoverage(
                "sequence",
                "list",
                present_count=1,
                sequence_length=4,
                observed_elements=4,
            )
        ],
        total_opportunities=100,
        threshold=0.5,
    )

    assert transform.dropped_ids == frozenset({"sequence"})


def test_select_targets_changes_only_target_vector() -> None:
    sample = Sample(
        key=(0,),
        features=Vector(values={"feature": 1.0}),
        targets=Vector(values={"sparse": None, "complete": 2.0}),
    )
    transform = SelectTargetsTransform(
        ["sparse", "complete"],
        [
            ColumnCoverage("sparse", "scalar", present_count=1, null_count=1),
            ColumnCoverage("complete", "scalar", present_count=1),
        ],
        total_opportunities=1,
        threshold=1.0,
    )

    output = list(transform.apply(iter([sample])))

    assert output[0].features.values == {"feature": 1.0}
    assert output[0].targets is not None
    assert output[0].targets.values == {"complete": 2.0}


def test_coverage_plan_requires_complete_valid_metadata() -> None:
    with pytest.raises(ValueError, match="Missing coverage metadata"):
        SelectFeaturesTransform(
            ["a"],
            [],
            total_opportunities=1,
            threshold=0.5,
        )

    with pytest.raises(ValueError, match="exceeds total opportunities"):
        SelectFeaturesTransform(
            ["a"],
            [ColumnCoverage("a", "scalar", present_count=2)],
            total_opportunities=1,
            threshold=0.5,
        )

    with pytest.raises(ValueError, match="unexpected id"):
        SelectFeaturesTransform(
            ["a"],
            [
                ColumnCoverage("a", "scalar", present_count=1),
                ColumnCoverage("typo", "scalar", present_count=1),
            ],
            total_opportunities=1,
            threshold=0.5,
        )


def test_sequence_coverage_rejects_impossible_observed_element_count() -> None:
    with pytest.raises(ValueError, match="present sequences"):
        ColumnCoverage(
            "sequence",
            "list",
            present_count=2,
            null_count=1,
            sequence_length=3,
            observed_elements=4,
        )
