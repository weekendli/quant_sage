"""Derive one consistent Run view from task history and immutable result links."""

from __future__ import annotations

from collections import Counter
from typing import Any

from quantsage_discovery.core.registry import Registry

from .catalog import FeatureCatalog


def active_linked_result(links: list[dict[str, Any]],
                         events: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Read the active binding from state history, preserving immutable link history."""
    by_id = {link["result_id"]: link["result"] for link in links}
    for event in reversed(events):
        if event["state"] in {"SUCCEEDED", "CACHED"}:
            result_id = event["fields"].get("result_id")
            if result_id in by_id:
                return by_id[result_id]
    return links[-1]["result"] if links else None


def enrich_run_view(registry: Registry, catalog: FeatureCatalog,
                    view: dict[str, Any]) -> dict[str, Any]:
    links = registry.list_run_result_links(view["run_id"])
    links_by_work: dict[str, list[dict[str, Any]]] = {}
    for link in links:
        links_by_work.setdefault(link["work_key"], []).append(link)
    all_result_refs: list[str] = []
    for work in view["item_executions"]:
        work_links = links_by_work.get(work["work_key"], [])
        refs = list(dict.fromkeys(link["result_id"] for link in work_links))
        work["result_refs"] = refs
        all_result_refs.extend(refs)
        events = registry.get_stage_events(work["stage_run_id"])
        active = active_linked_result(work_links, events)
        work["artifact_bindings"] = active["artifacts"] if active else []
        work["artifacts"] = [binding["artifact_ref"] for binding in work["artifact_bindings"]]
        work["quality"] = active["quality"] if active else None
        work["coverage"] = active["coverage"] if active else None
        work["health_checks"] = active.get("health_checks", []) if active else []
        work["needs_repair"] = bool(
            work["health_checks"]
            and work["health_checks"][-1]["status"] in {"missing", "invalid"}
        )
        work["producer_run_id"] = active["producer_run_id"] if active else None
        work["cache_source_run_id"] = (
            active["producer_run_id"]
            if active and work["state"] == "CACHED" else None
        )
        attempts: dict[int, dict[str, Any]] = {}
        for event in events:
            attempt = event["attempt"]
            if attempt < 1:
                continue
            record = attempts.setdefault(attempt, {"attempt": attempt, "events": []})
            record["events"].append(event)
            record["state"] = event["state"]
        work["attempts"] = list(attempts.values())
        if work["state"] != "BLOCKED":
            work["blocked_reason"] = None
    view["result_refs"] = list(dict.fromkeys(all_result_refs))
    selected = set(view["selected_item_ids"])
    dependency_items = {
        item_id for work in view["item_executions"]
        for item_id in work["item_ids"] if item_id not in selected
    }
    feature_set = catalog.get_feature_set(view["feature_set_ref"])
    view["inventory"] = [
        {"item_id": item["item_id"], "outputs": item["outputs"],
         "selection": ("REQUESTED" if item["item_id"] in selected else
                       "DEPENDENCY_ONLY" if item["item_id"] in dependency_items else
                       "NOT_SELECTED")}
        for item in feature_set["resolved_items"]
    ]
    counts = Counter(work["state"] for work in view["item_executions"])
    total = len(view["item_executions"])
    requested = [work for work in view["item_executions"] if work["role"] == "requested"]
    requested_counts = Counter(work["state"] for work in requested)
    if not total:
        status = "NO_WORK" if view["plan"]["status"] == "NO_WORK" else "BLOCKED"
    elif counts["RUNNING"]:
        status = "IN_PROGRESS"
    elif requested and all(work["state"] in {"SUCCEEDED", "CACHED"} for work in requested):
        status = "COMPLETE"
    elif requested_counts["SUCCEEDED"] + requested_counts["CACHED"]:
        status = "PARTIAL"
    elif requested_counts["FAILED"]:
        status = "FAILED"
    elif requested_counts["BLOCKED"]:
        status = "BLOCKED"
    elif requested_counts["SKIPPED"]:
        status = "SKIPPED"
    else:
        status = "NOT_STARTED"
    view["summary"] = {**view["summary"], "status": status,
                       "state_counts": dict(counts), "result_count": len(view["result_refs"])}
    return view
