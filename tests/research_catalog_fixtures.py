"""Reusable serialized catalog and FeatureSet documents for increment 01-C tests."""

from __future__ import annotations

import copy
from typing import Any

from tests.research_fixtures import make_source_fixture, make_study_document


def provider_document(
    *, provider_id: str = "arith", version: str = "1",
    provider_key: str = "builtin.arithmetic",
) -> dict[str, Any]:
    return {
        "schema_version": "provider_v1",
        "provider_id": provider_id,
        "version": version,
        "provider_key": provider_key,
        "implementation_version": "1.0",
        "supported_steps": [3, 4, 5],
    }


def primitive_template_document(
    *, template_id: str = "scaled_value", version: int = 1,
    formula_ref: str = "synthetic.scale@1", provider_ref: str = "arith@1",
    status: str = "ready", reason: str | None = None,
) -> dict[str, Any]:
    template = {
        "schema_version": "feature_template_v1",
        "template_id": template_id,
        "version": version,
        "name": "Scaled value",
        "description": "Synthetic arithmetic fixture.",
        "step": 3,
        "kind": "primitive",
        "family": "synthetic",
        "formula_ref": formula_ref,
        "provider_ref": provider_ref,
        "parameters_schema": {
            "type": "object",
            "properties": {"scale": {"type": "number", "default": 1}},
            "required": ["scale"],
            "additionalProperties": False,
        },
        "defaults": {"scale": 1},
        "source_requirements": {},
        "time_policy": {},
        "validity": {},
        "output_schema": {"value": "float64"},
        "unit": "dimensionless",
        "status": status,
    }
    if reason is not None:
        template["reason"] = reason
    return template


def statistical_template_document(
    *, template_id: str = "window_statistics", version: int = 1,
    formula_ref: str = "synthetic.statistics@1", provider_ref: str = "arith@1",
    status: str = "ready", reason: str | None = None,
) -> dict[str, Any]:
    template = {
        "schema_version": "feature_template_v1",
        "template_id": template_id,
        "version": version,
        "name": "Window statistics",
        "description": "Synthetic multi-output statistic fixture.",
        "step": 5,
        "kind": "statistical",
        "family": "synthetic_summary",
        "formula_ref": formula_ref,
        "provider_ref": provider_ref,
        "parameters_schema": {"type": "object", "properties": {}, "required": [],
                              "additionalProperties": False},
        "defaults": {},
        "source_requirements": {},
        "time_policy": {"mode": "trailing", "profile_window": "60s"},
        "validity": {},
        "output_schema": {"value": "float64"},
        "unit": "dimensionless",
        "status": status,
    }
    if reason is not None:
        template["reason"] = reason
    return template


def business_template_document(
    *, template_id: str = "sized_window", version: int = 1,
    formula_ref: str = "synthetic.sized_window@1", provider_ref: str = "arith@1",
) -> dict[str, Any]:
    return {
        "schema_version": "feature_template_v1",
        "template_id": template_id,
        "version": version,
        "name": "Sized window",
        "description": "Synthetic business feature for canonical parameter tests.",
        "step": 4,
        "kind": "business",
        "family": "synthetic_window",
        "formula_ref": formula_ref,
        "provider_ref": provider_ref,
        "parameters_schema": {
            "type": "object",
            "properties": {
                "space": {"type": "string"},
                "size": {"type": "object"},
            },
            "required": ["space", "size"],
            "additionalProperties": False,
        },
        "defaults": {},
        "source_requirements": {},
        "time_policy": {"mode": "trailing"},
        "validity": {},
        "output_schema": {"value": "float64"},
        "unit": "dimensionless",
        "status": "ready",
    }


def metric_document(
    metric_id: str,
    *, version: str = "1",
    formula_ref: str | None = None,
    status: str = "ready",
    reason: str | None = None,
) -> dict[str, Any]:
    metric = {
        "schema_version": "metric_v1",
        "metric_id": metric_id,
        "version": version,
        "name": metric_id.replace("_", " ").title(),
        "description": f"Synthetic {metric_id} metric.",
        "formula_ref": formula_ref or f"synthetic.{metric_id}@1",
        "provider_ref": "arith@1",
        "parameters_schema": {"type": "object", "properties": {}, "required": [],
                              "additionalProperties": False},
        "defaults": {},
        "output_schema": {"value": "float64"},
        "unit": "dimensionless",
        "status": status,
    }
    if reason is not None:
        metric["reason"] = reason
    return metric


def metric_set_document(
    *, metric_set_id: str = "synthetic_pair", version: int = 1,
    metrics: tuple[str, ...] = ("metric_mean@pycatch22-0.4.5", "metric_max@pycatch22-0.4.5"),
) -> dict[str, Any]:
    return {
        "schema_version": "metric_set_v1",
        "metric_set_id": metric_set_id,
        "version": version,
        "metrics": list(metrics),
    }


def feature_set_document(
    *, feature_set_id: str = "synthetic_set", version: int = 1,
    items: list[dict[str, Any]] | None = None,
    name: str = "Synthetic feature set",
    description: str = "Synthetic 01-C catalog fixture.",
    parent_ref: str | None = None,
    change_reason: str = "Create serialized fixture.",
) -> dict[str, Any]:
    if items is None:
        items = [{
            "item_id": "scaled",
            "alias": "scaled value",
            "recipe": {
                "template_ref": "scaled_value@1",
                "step": 3,
                "parameters": {"scale": 1},
            },
        }]
    document = {
        "schema_version": "feature_set_v1",
        "feature_set_id": feature_set_id,
        "version": version,
        "name": name,
        "description": description,
        "items": copy.deepcopy(items),
        "created_by": {"kind": "human", "id": "fixture-user"},
        "change_reason": change_reason,
    }
    if parent_ref is not None:
        document["parent_ref"] = parent_ref
    return document


def recipe_item(
    item_id: str,
    *,
    alias: str | None = None,
    template_ref: str = "scaled_value@1",
    step: int = 3,
    parameters: dict[str, Any] | None = None,
    input: dict[str, str] | None = None,
    lookback: str | None = None,
    profile_window: str | None = None,
    sampling: dict[str, Any] | None = None,
    validity: dict[str, Any] | None = None,
    metrics: list[str] | None = None,
    metric_set_ref: str | None = None,
) -> dict[str, Any]:
    recipe: dict[str, Any] = {
        "template_ref": template_ref,
        "step": step,
        "parameters": copy.deepcopy({"scale": 1} if parameters is None else parameters),
    }
    if input is not None:
        recipe["input"] = copy.deepcopy(input)
    if lookback is not None:
        recipe["lookback"] = lookback
    if profile_window is not None:
        recipe["profile_window"] = profile_window
    if sampling is not None:
        recipe["sampling"] = copy.deepcopy(sampling)
    if validity is not None:
        recipe["validity"] = copy.deepcopy(validity)
    if metrics is not None:
        recipe["metrics"] = list(metrics)
    if metric_set_ref is not None:
        recipe["metric_set_ref"] = metric_set_ref
    return {
        "item_id": item_id,
        "alias": alias or item_id,
        "recipe": recipe,
    }


def register_synthetic_catalog(service) -> None:
    """Register only deterministic built-in providers and small synthetic definitions."""
    service.register_provider(provider_document())
    service.register_template(primitive_template_document())
    service.register_template(statistical_template_document())
    service.register_template(business_template_document())
    for metric_id in ("metric_mean", "metric_max", "metric_std"):
        service.register_metric(metric_document(metric_id, version="pycatch22-0.4.5"))
    service.register_metric_set(metric_set_document())


__all__ = [
    "business_template_document",
    "feature_set_document",
    "make_source_fixture",
    "make_study_document",
    "metric_document",
    "metric_set_document",
    "primitive_template_document",
    "provider_document",
    "recipe_item",
    "register_synthetic_catalog",
    "statistical_template_document",
]
