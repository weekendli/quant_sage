"""Serialized source, Study and Run-request fixtures for increment 01-D planning."""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from tests.research_catalog_fixtures import (
    feature_set_document,
    primitive_template_document,
    recipe_item,
    register_synthetic_catalog,
)
from tests.research_fixtures import (
    SourceFixture,
    make_source_fixture,
    make_study_document,
    refresh_source_fixture_refs,
)

EXCHANGE = "SYNTH"
INSTRUMENT = "SYNTH2406"
TRADE_DATES = ("2024-06-03", "2024-06-04", "2024-06-05")
SPLIT_ID = "synthetic_train_v1"
FEATURE_SET_REF = "planning_features@1"
METRIC_MEAN = "metric_mean@pycatch22-0.4.5"
METRIC_MAX = "metric_max@pycatch22-0.4.5"


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
                    encoding="utf-8")


def make_planning_source_fixture(
    directory: Path,
    *,
    source_id: str = "planning_source",
    instrument_id: str = INSTRUMENT,
    trade_dates: tuple[str, ...] = TRADE_DATES,
    missing_dates: tuple[str, ...] = (),
    missing_exchange: bool = False,
    split_id: str = SPLIT_ID,
    split_start: str | None = None,
    split_end: str | None = None,
    split_role: str | None = None,
) -> SourceFixture:
    """Create a real, hashed DataSource tree with night sessions across UTC dates.

    The night session for trade date D begins on calendar date D-1. This makes
    the partition identity demonstrate that exchange trade dates do not follow
    the UTC date. Optional missing partitions leave the calendar and session
    inventory intact so planner diagnostics can distinguish a source gap from
    absent exchange metadata.
    """
    fixture = make_source_fixture(
        directory,
        source_id=source_id,
        instrument_id=instrument_id,
        trade_dates=trade_dates,
    )
    fixture.document["coverage"]["partitions"] = [
        {
            **({} if missing_exchange else {"exchange": EXCHANGE}),
            "instrument_id": instrument_id,
            "trade_date": trade_date,
            "session_ids": ["night"],
        }
        for trade_date in trade_dates if trade_date not in missing_dates
    ]

    intervals = []
    for trade_date in trade_dates:
        current_day = date.fromisoformat(trade_date)
        previous_day = (current_day - timedelta(days=1)).isoformat()
        intervals.append({
            "instrument_id": instrument_id,
            "trade_date": trade_date,
            "session_id": "night",
            "start": f"{previous_day}T21:00:00+08:00",
            "end": f"{trade_date}T02:30:00+08:00",
        })
    _write_json(fixture.paths["session_policy"], {
        "schema_version": "session_policy_v1",
        "session_policy_id": "synthetic_night_sessions",
        "sessions": ["night"],
        "intervals": intervals,
    })

    split_path = fixture.paths["split"]
    default_split_start = (
        f"{(date.fromisoformat(trade_dates[0]) - timedelta(days=1)).isoformat()}T21:00:00+08:00"
    )
    default_split_end = f"{trade_dates[-1]}T02:30:00+08:00"
    _write_json(split_path, {
        "schema_version": "split_manifest_v1",
        "split_id": split_id,
        "method": "synthetic_chronological",
        **({} if split_role is None else {"role": split_role}),
        "read_ranges": [{
            "instruments": [instrument_id],
            "start": split_start or default_split_start,
            "end": split_end or default_split_end,
            "session_filter": ["night"],
        }],
    })
    refresh_source_fixture_refs(fixture, refresh_split=True)
    return fixture


def make_plan_request(
    split_ref: dict[str, str],
    *,
    study_id: str = "planning_study",
    study_revision: int = 1,
    feature_set_ref: str = FEATURE_SET_REF,
    source_refs: tuple[str, ...] = ("planning_source@1",),
    instrument_id: str = INSTRUMENT,
    start: str = "2024-06-03T13:10:00+00:00",
    end: str = "2024-06-03T13:11:00+00:00",
    selected_item_ids: list[str] | None = None,
    include_selection: bool = False,
    allowed_steps: tuple[int, ...] = (3, 4, 5),
    missing_dependencies: str = "compute_within_allowed_steps",
    budget: dict[str, int] | None = None,
) -> dict[str, Any]:
    request = {
        "schema_version": "run_request_v1",
        "study_id": study_id,
        "study_revision": study_revision,
        "feature_set_ref": feature_set_ref,
        "source_refs": list(source_refs),
        "split_ref": copy.deepcopy(split_ref),
        "scope": {
            "instruments": [instrument_id],
            "start": start,
            "end": end,
            "session_filter": ["night"],
        },
        "allowed_steps": list(allowed_steps),
        "missing_dependencies": missing_dependencies,
        "reuse_policy": "verified_only",
        "budget": budget or {
            "max_work_units": 30,
            "max_partitions": 30,
            "max_concurrent_jobs": 4,
        },
        "created_by": {"kind": "agent", "id": "planner-fixture"},
    }
    if include_selection:
        request["selected_item_ids"] = list(selected_item_ids or [])
    return request


def split_ref_from_fixture(fixture: SourceFixture) -> dict[str, str]:
    access_policy = json.loads(fixture.paths["access_policy"].read_text(encoding="utf-8"))
    return copy.deepcopy(access_policy["split_refs"][0])


def unlisted_split_ref(directory: Path, *, instrument_id: str = INSTRUMENT) -> dict[str, str]:
    """Build a valid, hashed split artifact that source access policies do not list."""
    path = directory / "unlisted-split.json"
    document = {
        "schema_version": "split_manifest_v1",
        "split_id": "synthetic_unlisted_split_v1",
        "read_ranges": [{
            "instruments": [instrument_id],
            "start": "2024-06-02T21:00:00+08:00",
            "end": "2024-06-05T02:30:00+08:00",
            "session_filter": ["night"],
        }],
    }
    _write_json(path, document)
    payload = path.read_bytes()
    return {
        "uri": path.resolve().as_uri(),
        "content_hash": "sha256:" + hashlib.sha256(payload).hexdigest(),
    }


def register_planning_catalog(service, *, include_pending: bool = False) -> None:
    register_synthetic_catalog(service)
    if include_pending:
        service.register_template(primitive_template_document(
            template_id="planning_pending_base",
            formula_ref="synthetic.pending_base@1",
            status="pending_semantics",
            reason="Synthetic input semantics are pending.",
        ))


def make_planning_feature_set_document(
    *,
    feature_set_id: str = "planning_features",
    items: list[dict[str, Any]] | None = None,
    include_pending: bool = False,
) -> dict[str, Any]:
    if items is None:
        items = [
            recipe_item("quote"),
            recipe_item(
                "window", template_ref="sized_window@1", step=4,
                parameters={"space": "tick_distance", "size": {"value": 1, "unit": "levels"}},
                input={"item_id": "quote"}, lookback="3m",
            ),
            recipe_item(
                "summary", template_ref="window_statistics@1", step=5, parameters={},
                input={"item_id": "window"}, profile_window="1m",
                sampling={"axis": "event_time", "interval": "1m", "evaluation_stride": "1m"},
                validity={"min_samples": 1}, metrics=[METRIC_MEAN, METRIC_MAX],
            ),
        ]
        if include_pending:
            items.extend([
                recipe_item("pending_base", template_ref="planning_pending_base@1"),
                recipe_item(
                    "pending_summary", template_ref="window_statistics@1", step=5,
                    parameters={}, input={"item_id": "pending_base"}, profile_window="1m",
                    sampling={"axis": "event_time", "interval": "1m",
                              "evaluation_stride": "1m"},
                    validity={"min_samples": 1}, metrics=[METRIC_MEAN],
                ),
            ])
    return feature_set_document(feature_set_id=feature_set_id, items=items)


def make_planning_study_document(
    *,
    study_id: str = "planning_study",
    source_ref: str = "planning_source@1",
    feature_set_ref: str = FEATURE_SET_REF,
    instrument_id: str = INSTRUMENT,
    budget: dict[str, int] | None = None,
) -> dict[str, Any]:
    study = make_study_document(study_id=study_id, source_refs=(source_ref,),
                                instrument_id=instrument_id)
    study["feature_set_refs"] = [feature_set_ref]
    study["limits"]["allowed_steps"] = [3, 4, 5]
    study["limits"]["allowed_scope"] = [{
        "instruments": [instrument_id],
        "start": "2024-06-02T21:00:00+08:00",
        "end": "2024-06-06T00:00:00+08:00",
        "session_filter": ["night"],
    }]
    study["limits"]["budget"] = budget or {
        "max_work_units": 30,
        "max_partitions": 30,
        "max_concurrent_jobs": 4,
    }
    return study


__all__ = [
    "EXCHANGE",
    "FEATURE_SET_REF",
    "INSTRUMENT",
    "METRIC_MAX",
    "METRIC_MEAN",
    "SPLIT_ID",
    "TRADE_DATES",
    "make_plan_request",
    "make_planning_feature_set_document",
    "make_planning_source_fixture",
    "make_planning_study_document",
    "register_planning_catalog",
    "split_ref_from_fixture",
    "unlisted_split_ref",
]
