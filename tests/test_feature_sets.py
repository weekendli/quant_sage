"""Persisted catalog resolution and FeatureSet freeze acceptance for increment 01-C."""

from __future__ import annotations

import copy

import pytest

from quantsage_discovery.core import Registry
from quantsage_discovery.research.errors import (
    ImmutableVersionError,
    ResearchValidationError,
    RevisionConflictError,
    UnknownReferenceError,
)
from quantsage_discovery.research.service import ResearchService
from tests.research_catalog_fixtures import (
    feature_set_document,
    make_source_fixture,
    make_study_document,
    metric_document,
    metric_set_document,
    primitive_template_document,
    provider_document,
    recipe_item,
    register_synthetic_catalog,
)

METRIC_MEAN = "metric_mean@pycatch22-0.4.5"
METRIC_MAX = "metric_max@pycatch22-0.4.5"
METRIC_STD = "metric_std@pycatch22-0.4.5"


def _open(tmp_path, name: str = "research.duckdb"):
    registry = Registry(tmp_path / name)
    return registry, ResearchService(registry)


def _freeze(service: ResearchService, document: dict):
    draft = service.save_feature_set(document)
    return service.freeze_feature_set(
        f"{document['feature_set_id']}@{document['version']}",
        expected_revision=draft["draft_revision"],
    )


def _item(frozen: dict, item_id: str) -> dict:
    return next(item for item in frozen["resolved_items"] if item["item_id"] == item_id)


def _outputs(frozen: dict, item_id: str) -> list[dict]:
    return _item(frozen, item_id)["outputs"]


def _stat_item(
    item_id: str = "summary",
    *,
    alias: str | None = None,
    input: dict[str, str] | None = None,
    metrics: list[str] | None = None,
    metric_set_ref: str | None = None,
    profile_window: str = "60s",
) -> dict:
    return recipe_item(
        item_id,
        alias=alias,
        template_ref="window_statistics@1",
        step=5,
        parameters={},
        input=input or {"item_id": "base"},
        profile_window=profile_window,
        sampling={"axis": "event_time", "interval": "1s", "evaluation_stride": "1s"},
        validity={"min_samples": 3},
        metrics=metrics,
        metric_set_ref=metric_set_ref,
    )


def test_direct_statistical_reference_preserves_output_identity_and_definition_audit(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        frozen = _freeze(service, feature_set_document(
            feature_set_id="stat_origin",
            items=[recipe_item("base"), _stat_item(metrics=[METRIC_MEAN])],
        ))
        original = _outputs(frozen, "summary")[0]
        definition = service.get_feature(original["feature_ref"])
        assert definition["created_at"]
        assert definition["created_by"] == frozen["created_by"]
        reused = _freeze(service, feature_set_document(
            feature_set_id="stat_ref",
            items=[{"item_id": "same", "feature_ref": original["feature_ref"]}],
        ))
        assert _outputs(reused, "same")[0]["output_id"] == original["output_id"]
    finally:
        registry.close()


def test_draft_editor_does_not_replace_creation_actor(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        document = feature_set_document(feature_set_id="creation_audit")
        first = service.save_feature_set(document)
        document["created_by"] = {"kind": "agent", "id": "second-editor"}
        document["change_reason"] = "Revise selection"
        edited = service.save_feature_set(document, expected_revision=1)
        assert edited["created_by"] == first["created_by"]
        assert edited["created_at"] == first["created_at"]
        assert edited["updated_by"] == document["created_by"]
    finally:
        registry.close()


def test_catalog_rejects_unknown_fields_unregistered_implementations_and_provider_refs(tmp_path):
    registry, service = _open(tmp_path)
    try:
        with pytest.raises(ResearchValidationError):
            service.register_provider(provider_document(provider_key="python:untrusted.module"))

        service.register_provider(provider_document())
        template = primitive_template_document()
        template["unexpected"] = "dynamic code is not a catalog field"
        with pytest.raises(ResearchValidationError):
            service.register_template(template)

        template = primitive_template_document(provider_ref="missing@1")
        with pytest.raises(UnknownReferenceError):
            service.register_template(template)

        metric = metric_document("strict_metric", version="pycatch22-0.4.5")
        metric["user_expression"] = "__import__('os')"
        with pytest.raises(ResearchValidationError):
            service.register_metric(metric)
    finally:
        registry.close()


def test_catalog_rejects_mutable_latest_resource_versions(tmp_path):
    registry, service = _open(tmp_path)
    try:
        with pytest.raises(ResearchValidationError):
            service.register_provider(provider_document(version="latest"))

        register_synthetic_catalog(service)
        with pytest.raises(ResearchValidationError):
            service.get_template("scaled_value@latest")
        with pytest.raises(ResearchValidationError):
            service.register_template(primitive_template_document(provider_ref="arith@latest"))
        with pytest.raises(ResearchValidationError):
            service.register_metric(metric_document("latest_metric", version="latest"))
    finally:
        registry.close()


def test_metric_and_metric_set_refs_are_exact_versioned_catalog_entries(tmp_path):
    registry, service = _open(tmp_path)
    try:
        service.register_provider(provider_document())
        with pytest.raises(UnknownReferenceError):
            service.register_metric_set(metric_set_document(
                metrics=("metric_mean@pycatch22-0.4.6",),
            ))

        service.register_metric(metric_document("metric_mean", version="pycatch22-0.4.5"))
        with pytest.raises(ResearchValidationError):
            service.register_metric_set({
                **metric_set_document(metrics=(METRIC_MEAN,)), "unknown": True,
            })

        with pytest.raises(UnknownReferenceError):
            service.register_metric_set(metric_set_document(
                metric_set_id="unregistered_metric", metrics=("metric_max@pycatch22-0.4.5",),
            ))
    finally:
        registry.close()


def test_recipe_rejects_unknown_template_metrics_and_mutually_exclusive_inputs(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        unknown_template = feature_set_document(items=[recipe_item(
            "bad", template_ref="unregistered_template@1",
        )], feature_set_id="unknown_template_set")
        with pytest.raises(UnknownReferenceError):
            _freeze(service, unknown_template)

        unknown_metric = feature_set_document(items=[
            recipe_item("base"),
            _stat_item(metrics=["metric_mean@pycatch22-0.4.6"]),
        ], feature_set_id="unknown_metric_set")
        with pytest.raises(UnknownReferenceError):
            _freeze(service, unknown_metric)

        both_metric_forms = feature_set_document(items=[
            recipe_item("base"),
            _stat_item(metrics=[METRIC_MEAN], metric_set_ref="synthetic_pair@1"),
        ], feature_set_id="conflicting_metric_forms")
        with pytest.raises(ResearchValidationError):
            _freeze(service, both_metric_forms)
    finally:
        registry.close()


def test_direct_feature_reference_and_recipe_are_mutually_exclusive(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        original = _freeze(service, feature_set_document(feature_set_id="seed"))
        feature_ref = _outputs(original, "scaled")[0]["feature_ref"]
        ambiguous = feature_set_document(feature_set_id="ambiguous", items=[{
            "item_id": "ambiguous_item",
            "alias": "ambiguous",
            "feature_ref": feature_ref,
            "recipe": {
                "template_ref": "scaled_value@1", "step": 3, "parameters": {"scale": 1},
            },
        }])
        with pytest.raises(ResearchValidationError):
            _freeze(service, ambiguous)
    finally:
        registry.close()


def test_numeric_size_and_duration_spellings_freeze_to_the_same_definition(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        first = feature_set_document(
            feature_set_id="size_duration_a",
            items=[recipe_item(
                "window", template_ref="sized_window@1", step=4,
                parameters={"space": "tick_distance", "size": {"value": 1, "unit": "levels"}},
                lookback="60s",
            )],
        )
        second = feature_set_document(
            feature_set_id="size_duration_b",
            items=[recipe_item(
                "window", template_ref="sized_window@1", step=4,
                parameters={"space": "tick_distance", "size": {"value": 1.0, "unit": "levels"}},
                lookback="1m",
            )],
        )
        third = feature_set_document(
            feature_set_id="size_duration_c",
            items=[recipe_item(
                "window", template_ref="sized_window@1", step=4,
                parameters={"space": "tick_distance", "size": {"value": 1, "unit": "levels"}},
                lookback="PT60S",
            )],
        )
        frozen = [_freeze(service, doc) for doc in (first, second, third)]
        definitions = [_outputs(doc, "window")[0]["definition_hash"] for doc in frozen]
        output_ids = [_outputs(doc, "window")[0]["output_id"] for doc in frozen]
        assert len(set(definitions)) == 1
        assert len(set(output_ids)) == 1
    finally:
        registry.close()


def test_alias_and_set_version_do_not_change_feature_definition_or_output_id(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        first = feature_set_document(
            feature_set_id="alias_set_a", version=1,
            items=[recipe_item("scaled", alias="close scaled", parameters={"scale": 1})],
        )
        second = feature_set_document(
            feature_set_id="alias_set_a", version=2, parent_ref="alias_set_a@1",
            items=[recipe_item("renamed", alias="market level", parameters={"scale": 1})],
        )
        frozen_a = _freeze(service, first)
        frozen_b = _freeze(service, second)
        output_a = _outputs(frozen_a, "scaled")[0]
        output_b = _outputs(frozen_b, "renamed")[0]
        assert output_a["definition_hash"] == output_b["definition_hash"]
        assert output_a["output_id"] == output_b["output_id"]
        assert output_a["feature_ref"] == output_b["feature_ref"]
    finally:
        registry.close()


def test_formula_parameter_and_template_version_changes_alter_definition_hash(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        service.register_template(primitive_template_document(
            version=2, formula_ref="synthetic.scale_v2@1",
        ))
        baseline = _freeze(service, feature_set_document(
            feature_set_id="identity_baseline",
            items=[recipe_item("value", parameters={"scale": 1})],
        ))
        changed_formula = _freeze(service, feature_set_document(
            feature_set_id="identity_formula",
            items=[recipe_item(
                "value", template_ref="scaled_value@2", parameters={"scale": 1},
            )],
        ))
        changed_parameter = _freeze(service, feature_set_document(
            feature_set_id="identity_parameter",
            items=[recipe_item("value", parameters={"scale": 2})],
        ))
        hashes = {
            _outputs(frozen, "value")[0]["definition_hash"]
            for frozen in (baseline, changed_formula, changed_parameter)
        }
        assert len(hashes) == 3
    finally:
        registry.close()


def test_one_input_and_three_metrics_freeze_to_three_stable_outputs(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        items = [
            recipe_item("base"),
            _stat_item(metrics=[METRIC_MEAN, METRIC_MAX, METRIC_STD]),
        ]
        frozen = _freeze(service, feature_set_document(feature_set_id="three_outputs", items=items))
        outputs = _outputs(frozen, "summary")
        assert len(outputs) == 3
        assert len({output["output_id"] for output in outputs}) == 3
        assert all(output["feature_ref"].startswith("feature_") for output in outputs)

        # The same semantic outputs retain their IDs when display names and set identity change.
        renamed = [
            recipe_item("underlier", alias="input series"),
            _stat_item(
                "moments", alias="renamed stats", input={"item_id": "underlier"},
                metrics=[METRIC_MEAN, METRIC_MAX, METRIC_STD],
            ),
        ]
        frozen_renamed = _freeze(service, feature_set_document(
            feature_set_id="three_outputs_renamed", items=renamed,
        ))
        assert {out["output_id"] for out in outputs} == {
            out["output_id"] for out in _outputs(frozen_renamed, "moments")
        }
    finally:
        registry.close()


def test_documented_sampling_and_validity_fields_are_canonical_and_alias_defaults_to_item_id(
    tmp_path,
):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        sampling = {
            "time_basis": "clock_time",
            "grid_origin": "session_start",
            "alignment": "backward_asof",
            "interval": "1s",
            "evaluation_stride": "1s",
            "max_age": "5s",
            "on_gap": "invalidate_window",
            "on_session_boundary": "reset",
        }
        validity = {
            "min_samples": 3,
            "allow_partial_window": False,
            "emit_subsegment_summaries": False,
        }
        implicit_alias_item = _stat_item(
            input={"item_id": "base"}, metrics=[METRIC_MEAN],
        )
        implicit_alias_item.pop("alias")
        implicit_alias_item["recipe"]["sampling"] = sampling
        implicit_alias_item["recipe"]["validity"] = validity
        explicit_alias_item = _stat_item(
            input={"item_id": "base"}, metrics=[METRIC_MEAN],
        )
        explicit_alias_item["alias"] = "summary"
        explicit_alias_item["recipe"]["sampling"] = sampling
        explicit_alias_item["recipe"]["validity"] = validity

        implicit = _freeze(service, feature_set_document(
            feature_set_id="canonical_sampling_implicit_alias",
            items=[recipe_item("base"), implicit_alias_item],
        ))
        explicit = _freeze(service, feature_set_document(
            feature_set_id="canonical_sampling_explicit_alias",
            items=[recipe_item("base"), explicit_alias_item],
        ))
        implicit_output = _outputs(implicit, "summary")[0]
        explicit_output = _outputs(explicit, "summary")[0]
        assert implicit_output["definition_hash"] == explicit_output["definition_hash"]
        assert implicit_output["output_id"] == explicit_output["output_id"]
        feature = service.get_feature(implicit_output["feature_ref"])
        assert feature["sampling"]["axis"] == "clock_time"
        assert feature["sampling"]["grid_origin"] == "session_start"
        assert feature["sampling"]["as_of"] == "backward_asof"
        assert feature["sampling"]["max_feature_age"] == "5000000000ns"
        assert feature["sampling"]["on_gap"] == "invalidate_window"
        assert feature["sampling"]["on_session_boundary"] == "reset"
        assert feature["validity"]["allow_partial_window"] is False
        assert feature["validity"]["emit_subsegment_summaries"] is False
    finally:
        registry.close()


def test_metric_set_expands_to_its_registered_metrics(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        frozen = _freeze(service, feature_set_document(
            feature_set_id="metric_set_expansion",
            items=[recipe_item("base"), _stat_item(metric_set_ref="synthetic_pair@1")],
        ))
        outputs = _outputs(frozen, "summary")
        assert len(outputs) == 2
        assert len({output["output_id"] for output in outputs}) == 2
        assert all(output["status"] == "ready" for output in outputs)
    finally:
        registry.close()


def test_item_dependencies_require_an_unambiguous_output_and_accept_explicit_output_ref(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        upstream = [recipe_item("base"), _stat_item(metrics=[METRIC_MEAN, METRIC_MAX, METRIC_STD])]
        multi_output_set = _freeze(service, feature_set_document(
            feature_set_id="multi_output_parent", items=upstream,
        ))
        selected_output = _outputs(multi_output_set, "summary")[1]["output_id"]

        ambiguous = [
            recipe_item("base"),
            _stat_item(metrics=[METRIC_MEAN, METRIC_MAX, METRIC_STD]),
            _stat_item("child", input={"item_id": "summary"}, metrics=[METRIC_MEAN]),
        ]
        with pytest.raises(ResearchValidationError):
            _freeze(service, feature_set_document(feature_set_id="ambiguous_input", items=ambiguous))

        explicit = [
            recipe_item("base"),
            _stat_item(metrics=[METRIC_MEAN, METRIC_MAX, METRIC_STD]),
            _stat_item(
                "child", input={"item_id": "summary", "output_id": selected_output},
                metrics=[METRIC_MEAN],
            ),
        ]
        frozen = _freeze(service, feature_set_document(
            feature_set_id="explicit_output_input", items=explicit,
        ))
        assert len(_outputs(frozen, "child")) == 1
    finally:
        registry.close()


def test_direct_feature_input_and_feature_ref_items_resolve_exact_registered_versions(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        base_set = _freeze(service, feature_set_document(feature_set_id="direct_feature_seed"))
        base_ref = _outputs(base_set, "scaled")[0]["feature_ref"]

        direct_input = _freeze(service, feature_set_document(
            feature_set_id="direct_feature_input",
            items=[_stat_item(input={"input_feature_ref": base_ref}, metrics=[METRIC_MEAN])],
        ))
        assert len(_outputs(direct_input, "summary")) == 1

        direct_item = _freeze(service, feature_set_document(
            feature_set_id="direct_feature_item",
            items=[{"item_id": "selected", "alias": "selected", "feature_ref": base_ref}],
        ))
        assert _outputs(direct_item, "selected")[0]["feature_ref"] == base_ref

        with pytest.raises(UnknownReferenceError):
            _freeze(service, feature_set_document(
                feature_set_id="missing_direct_feature",
                items=[{"item_id": "missing", "alias": "missing", "feature_ref": "absent@1"}],
            ))
    finally:
        registry.close()


def test_feature_dependency_cycles_are_rejected(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        cyclic = [
            _stat_item("first", input={"item_id": "second"}, metrics=[METRIC_MEAN]),
            _stat_item("second", input={"item_id": "first"}, metrics=[METRIC_MAX]),
        ]
        with pytest.raises(ResearchValidationError):
            _freeze(service, feature_set_document(feature_set_id="cycle", items=cyclic))
    finally:
        registry.close()


def test_pending_definitions_can_freeze_alongside_ready_outputs(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        service.register_template(primitive_template_document(
            template_id="pending_primitive", version=1, formula_ref="synthetic.pending@1",
            status="pending_semantics", reason="Formula endpoints remain under review.",
        ))
        frozen = _freeze(service, feature_set_document(
            feature_set_id="mixed_readiness",
            items=[
                recipe_item("ready"),
                recipe_item("pending", template_ref="pending_primitive@1"),
            ],
        ))
        assert _outputs(frozen, "ready")[0]["status"] == "ready"
        assert _outputs(frozen, "pending")[0]["status"] == "pending_semantics"
        assert frozen["status"] == "frozen"
    finally:
        registry.close()


def test_draft_updates_use_optimistic_revision_and_frozen_sets_are_immutable(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        first = feature_set_document(feature_set_id="revisioned_draft")
        draft_v1 = service.save_feature_set(first)
        assert draft_v1["draft_revision"] == 1
        assert draft_v1["status"] == "draft"

        changed = copy.deepcopy(first)
        changed["name"] = "Edited synthetic feature set"
        draft_v2 = service.save_feature_set(changed, expected_revision=1)
        assert draft_v2["draft_revision"] == 2
        with pytest.raises(RevisionConflictError):
            service.save_feature_set(first, expected_revision=1)
        assert service.get_feature_set("revisioned_draft@1")["name"] == changed["name"]

        frozen = service.freeze_feature_set("revisioned_draft@1", expected_revision=2)
        assert frozen["status"] == "frozen"
        changed["name"] = "Mutation after freeze"
        with pytest.raises(ImmutableVersionError):
            service.save_feature_set(changed, expected_revision=2)
        assert service.get_feature_set("revisioned_draft@1")["name"] == "Edited synthetic feature set"
    finally:
        registry.close()


def test_failed_freeze_leaves_no_partial_features_or_output_mappings(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        document = feature_set_document(
            feature_set_id="atomic_freeze",
            items=[
                recipe_item("valid"),
                {"item_id": "unknown_feature", "alias": "unknown", "feature_ref": "absent@1"},
            ],
        )
        before = service.list_features()
        draft = service.save_feature_set(document)
        with pytest.raises(UnknownReferenceError):
            service.freeze_feature_set(
                "atomic_freeze@1", expected_revision=draft["draft_revision"],
            )
        assert service.list_features() == before
        stored = service.get_feature_set("atomic_freeze@1")
        assert stored["status"] == "draft"
        assert "config_hash" not in stored
    finally:
        registry.close()


def test_frozen_catalog_reopens_and_study_backlog_must_name_actual_items(tmp_path):
    registry, service = _open(tmp_path)
    try:
        register_synthetic_catalog(service)
        source = make_source_fixture(tmp_path)
        service.register_source(source.document)
        frozen = _freeze(service, feature_set_document(feature_set_id="study_candidates"))
        valid_ref = f"{frozen['feature_set_id']}@{frozen['version']}"

        study = make_study_document()
        study["feature_set_refs"] = [valid_ref]
        study["default_feature_set_ref"] = valid_ref
        study["backlog"] = [{
            "feature_set_ref": valid_ref,
            "item_id": "scaled",
            "priority": 1,
            "state": "active",
            "reason": "Synthetic candidate for management verification.",
        }]
        registered_study = service.create_study(study)
        assert registered_study["backlog"][0]["item_id"] == "scaled"

        bad_item_study = make_study_document(study_id="bad_backlog_item")
        bad_item_study["feature_set_refs"] = [valid_ref]
        bad_item_study["backlog"] = [{
            **study["backlog"][0], "item_id": "not_in_frozen_set",
        }]
        with pytest.raises(UnknownReferenceError):
            service.create_study(bad_item_study)

        bad_ref_study = make_study_document(study_id="bad_backlog_ref")
        bad_ref_study["feature_set_refs"] = ["missing_set@1"]
        with pytest.raises(UnknownReferenceError):
            service.create_study(bad_ref_study)

        expected_feature_ref = _outputs(frozen, "scaled")[0]["feature_ref"]
    finally:
        registry.close()

    reopened, reopened_service = _open(tmp_path)
    try:
        persisted = reopened_service.get_feature_set(valid_ref)
        assert persisted["config_hash"] == frozen["config_hash"]
        assert reopened_service.get_feature(expected_feature_ref)["definition_hash"] == \
            _outputs(frozen, "scaled")[0]["definition_hash"]
        assert reopened_service.get_study("synthetic_study")["feature_set_refs"] == [valid_ref]
    finally:
        reopened.close()
