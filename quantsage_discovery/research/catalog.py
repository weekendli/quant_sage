"""Versioned Feature catalog, draft FeatureSet storage, and frozen resolution."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

import duckdb

from quantsage_discovery.core.registry import Registry, utc_now
from quantsage_discovery.core.utils import canonical_json

from .errors import (
    ImmutableVersionError,
    ResearchValidationError,
    RevisionConflictError,
    UnknownReferenceError,
)
from .feature_schemas import (
    _normalize_sampling,
    _normalize_validity,
    feature_set_ref,
    normalize_duration,
    normalize_parameters,
    parse_feature_ref,
    parse_feature_set_ref,
    parse_resource_ref,
    validate_feature_set_draft,
    validate_metric,
    validate_metric_set,
    validate_provider,
    validate_template,
)
from .statistical_windows import (
    INPUT_VIEW_POLICY,
    validate_statistical_input,
)

_T = TypeVar("_T")


def _fetchone(connection: duckdb.DuckDBPyConnection, sql: str,
              params: tuple[Any, ...] | list[Any] = ()) -> dict[str, Any] | None:
    cursor = connection.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return None
    return {item[0]: value for item, value in zip(cursor.description, row, strict=True)}


def _fetchall(connection: duckdb.DuckDBPyConnection, sql: str,
              params: tuple[Any, ...] | list[Any] = ()) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, params)
    return [{item[0]: value for item, value in zip(cursor.description, row, strict=True)}
            for row in cursor.fetchall()]


class FeatureCatalog:
    """Persist trusted versioned resources and immutable resolved definitions."""

    def __init__(self, registry: Registry):
        self._registry = registry

    def _call(self, operation: Callable[[duckdb.DuckDBPyConnection], _T]) -> _T:
        return self._registry._call(operation)

    def register_provider(self, document: Mapping[str, Any]) -> dict[str, Any]:
        doc = validate_provider(dict(document))
        self._register_resource("providers", "provider_id", doc["provider_id"],
                                "version", doc["version"], doc)
        return doc

    def register_template(self, document: Mapping[str, Any]) -> dict[str, Any]:
        doc = validate_template(dict(document))
        ref = doc["provider_ref"]
        self._register_with_provider(
            "feature_templates", "template_id", doc["template_id"], "version", doc["version"],
            doc, ref,
        )
        return doc

    def register_metric(self, document: Mapping[str, Any]) -> dict[str, Any]:
        doc = validate_metric(dict(document))
        self._register_with_provider(
            "metrics", "metric_id", doc["metric_id"], "version", doc["version"], doc,
            doc["provider_ref"],
        )
        return doc

    def register_metric_set(self, document: Mapping[str, Any]) -> dict[str, Any]:
        doc = validate_metric_set(dict(document))

        def insert(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            for metric in doc["metrics"]:
                metric_id, version = parse_resource_ref(metric, "metric_set.metrics[]")
                if _fetchone(connection, "SELECT metric_id FROM metrics WHERE metric_id=? AND version=?",
                             (metric_id, version)) is None:
                    raise UnknownReferenceError(f"unknown metric ref: {metric}", details={"ref": metric})
            self._insert(connection, "metric_sets", "metric_set_id", doc["metric_set_id"],
                         "version", doc["version"], doc)
            return doc

        return self._call(insert)

    def get_provider(self, ref: str) -> dict[str, Any]:
        return self._get_resource("providers", "provider_id", ref)

    def get_template(self, ref: str) -> dict[str, Any]:
        return self._get_resource("feature_templates", "template_id", ref)

    def get_metric(self, ref: str) -> dict[str, Any]:
        return self._get_resource("metrics", "metric_id", ref)

    def get_metric_set(self, ref: str) -> dict[str, Any]:
        set_id, version = parse_feature_set_ref(ref, "metric_set_ref")
        row = self._call(lambda connection: _fetchone(
            connection, "SELECT document_json FROM metric_sets WHERE metric_set_id=? AND version=?",
            (set_id, version),
        ))
        if row is None:
            raise UnknownReferenceError(f"unknown metric set ref: {ref}", details={"ref": ref})
        return json.loads(row["document_json"])

    def save_feature_set(self, document: Mapping[str, Any], *,
                         expected_revision: int | None = None) -> dict[str, Any]:
        doc = validate_feature_set_draft(dict(document))
        set_id, version = doc["feature_set_id"], doc["version"]
        if expected_revision is not None and (type(expected_revision) is not int
                                              or expected_revision < 1):
            raise ResearchValidationError("expected_revision must be an integer >= 1")
        now = utc_now()

        def save(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            row = _fetchone(connection,
                            "SELECT * FROM feature_sets WHERE set_id=? AND version=?",
                            (set_id, version))
            if row is None:
                if expected_revision is not None:
                    raise RevisionConflictError(
                        "a new FeatureSet must not specify expected_revision",
                        details={"feature_set_ref": feature_set_ref(set_id, version)},
                    )
                latest = _fetchone(
                    connection, "SELECT max(version) AS version FROM feature_sets WHERE set_id=?",
                    (set_id,),
                )
                expected_version = 1 if latest["version"] is None else int(latest["version"]) + 1
                if version != expected_version:
                    raise ImmutableVersionError(
                        f"FeatureSet version must be the next immutable version ({expected_version})",
                        details={"feature_set_id": set_id, "expected_version": expected_version},
                    )
                if doc.get("parent_ref"):
                    parent_id, parent_version = parse_feature_set_ref(
                        doc["parent_ref"], "feature_set.parent_ref"
                    )
                    parent = _fetchone(
                        connection,
                        "SELECT state FROM feature_sets WHERE set_id=? AND version=?",
                        (parent_id, parent_version),
                    )
                    if parent is None or parent["state"] != "frozen":
                        raise UnknownReferenceError(
                            f"parent FeatureSet must be frozen: {doc['parent_ref']}",
                            details={"ref": doc["parent_ref"]},
                        )
                revision = 1
                stored = {**doc, "status": "draft", "draft_revision": revision,
                          "created_at": now, "updated_at": now, "updated_by": doc["created_by"]}
                connection.execute(
                    "INSERT INTO feature_sets(set_id,version,state,draft_revision,config_hash,"
                    "document_json,created_at,updated_at) VALUES(?,?,'draft',?,NULL,?,?,?)",
                    (set_id, version, revision, canonical_json(stored), now, now),
                )
            else:
                if row["state"] != "draft":
                    raise ImmutableVersionError(
                        f"frozen FeatureSet cannot be edited: {feature_set_ref(set_id, version)}"
                    )
                current_revision = int(row["draft_revision"])
                if expected_revision is None or expected_revision != current_revision:
                    raise RevisionConflictError(
                        f"FeatureSet draft revision changed from expected {expected_revision} "
                        f"to {current_revision}",
                        details={"feature_set_ref": feature_set_ref(set_id, version),
                                 "expected_revision": expected_revision,
                                 "current_revision": current_revision},
                    )
                current = json.loads(row["document_json"])
                revision = current_revision + 1
                stored = {**doc, "status": "draft", "draft_revision": revision,
                          "created_at": current["created_at"], "updated_at": now,
                          "created_by": current["created_by"],
                          "updated_by": doc["created_by"]}
                connection.execute(
                    "UPDATE feature_sets SET draft_revision=?,document_json=?,updated_at=? "
                    "WHERE set_id=? AND version=? AND state='draft' AND draft_revision=?",
                    (revision, canonical_json(stored), now, set_id, version, current_revision),
                )
            connection.execute(
                "INSERT INTO feature_set_revisions(set_id,version,draft_revision,document_json,"
                "actor_json,reason,created_at) VALUES(?,?,?,?,?,?,?)",
                (set_id, version, revision, canonical_json(stored), canonical_json(doc["created_by"]),
                 doc["change_reason"], now),
            )
            return stored

        return self._call(save)

    def get_feature_set(self, ref: str) -> dict[str, Any]:
        set_id, version = parse_feature_set_ref(ref, "feature_set_ref")
        row = self._call(lambda connection: _fetchone(
            connection, "SELECT document_json FROM feature_sets WHERE set_id=? AND version=?",
            (set_id, version),
        ))
        if row is None:
            raise UnknownReferenceError(f"unknown FeatureSet ref: {ref}", details={"ref": ref})
        return json.loads(row["document_json"])

    def freeze_feature_set(self, ref: str, *, expected_revision: int) -> dict[str, Any]:
        set_id, version = parse_feature_set_ref(ref, "feature_set_ref")
        if type(expected_revision) is not int or expected_revision < 1:
            raise ResearchValidationError("expected_revision must be an integer >= 1")
        frozen_at = utc_now()

        def freeze(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            row = _fetchone(connection,
                            "SELECT * FROM feature_sets WHERE set_id=? AND version=?",
                            (set_id, version))
            if row is None:
                raise UnknownReferenceError(f"unknown FeatureSet ref: {ref}", details={"ref": ref})
            revision = int(row["draft_revision"])
            if revision != expected_revision:
                raise RevisionConflictError(
                    f"FeatureSet draft revision changed from expected {expected_revision} to {revision}",
                    details={"feature_set_ref": ref, "expected_revision": expected_revision,
                             "current_revision": revision},
                )
            stored_document = json.loads(row["document_json"])
            if row["state"] == "frozen":
                return stored_document
            if row["state"] != "draft":
                raise ImmutableVersionError(f"FeatureSet state cannot be frozen: {row['state']}")

            resolved_items, feature_documents = _resolve_feature_set(connection, stored_document)
            config = {
                "schema_version": stored_document["schema_version"],
                "feature_set_id": stored_document["feature_set_id"],
                "version": stored_document["version"],
                "name": stored_document["name"],
                "description": stored_document["description"],
                "parent_ref": stored_document.get("parent_ref"),
                "defaults": stored_document.get("defaults", {}),
                "items": stored_document["items"],
                "resolved_items": resolved_items,
            }
            config_hash = _hash_json(config)
            frozen = {**stored_document, "status": "frozen", "resolved_items": resolved_items,
                      "config_hash": config_hash, "frozen_at": frozen_at}

            for feature in feature_documents.values():
                feature = {**feature, "created_at": frozen_at,
                           "created_by": stored_document.get("updated_by",
                                                               stored_document["created_by"])}
                existing = _fetchone(
                    connection,
                    "SELECT definition_hash FROM features WHERE feature_id=? AND version=?",
                    (feature["feature_id"], feature["version"]),
                )
                if existing is not None:
                    if existing["definition_hash"] != feature["definition_hash"]:
                        raise ResearchValidationError("content-addressed Feature identity collision")
                    continue
                connection.execute(
                    "INSERT INTO features(feature_id,version,definition_hash,kind,step,status,document_json) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (feature["feature_id"], feature["version"], feature["definition_hash"],
                     feature["kind"], feature["step"], feature["status"], canonical_json(feature)),
                )

            for item in resolved_items:
                for output in item["outputs"]:
                    feature_id, feature_version = parse_feature_ref(output["feature_ref"],
                                                                     "resolved_items.feature_ref")
                    connection.execute(
                        "INSERT INTO feature_set_outputs(set_id,version,item_id,output_id,feature_id,"
                        "feature_version,definition_hash) VALUES(?,?,?,?,?,?,?)",
                        (set_id, version, item["item_id"], output["output_id"], feature_id,
                         feature_version, output["definition_hash"]),
                    )
            connection.execute(
                "UPDATE feature_sets SET state='frozen',config_hash=?,document_json=?,updated_at=? "
                "WHERE set_id=? AND version=? AND state='draft' AND draft_revision=?",
                (config_hash, canonical_json(frozen), frozen_at, set_id, version, expected_revision),
            )
            return frozen

        return self._call(freeze)

    def get_feature(self, ref: str) -> dict[str, Any]:
        feature_id, version = parse_feature_ref(ref, "feature_ref")
        row = self._call(lambda connection: _fetchone(
            connection,
            "SELECT document_json FROM features WHERE feature_id=? AND version=?",
            (feature_id, version),
        ))
        if row is None:
            raise UnknownReferenceError(f"unknown Feature ref: {ref}", details={"ref": ref})
        return json.loads(row["document_json"])

    def list_features(self, filters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        allowed = {"kind", "step", "status"}
        filters = dict(filters or {})
        unknown = set(filters) - allowed
        if unknown:
            raise ResearchValidationError(f"unknown Feature filters: {', '.join(sorted(unknown))}")
        clauses, params = [], []
        if "kind" in filters:
            if filters["kind"] not in {"primitive", "business", "statistical"}:
                raise ResearchValidationError("kind filter must be primitive, business, or statistical")
            clauses.append("kind=?")
            params.append(filters["kind"])
        if "step" in filters:
            if type(filters["step"]) is not int or filters["step"] not in {3, 4, 5}:
                raise ResearchValidationError("step filter must be 3, 4, or 5")
            clauses.append("step=?")
            params.append(filters["step"])
        if "status" in filters:
            if filters["status"] not in {"ready", "pending_semantics", "retired"}:
                raise ResearchValidationError("invalid Feature status filter")
            clauses.append("status=?")
            params.append(filters["status"])
        query = "SELECT document_json FROM features"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY feature_id,version"
        rows = self._call(lambda connection: _fetchall(connection, query, params))
        return [json.loads(row["document_json"]) for row in rows]

    def list_feature_page(self, filters: Mapping[str, Any] | None = None, *, limit: int = 100,
                          cursor: str | None = None) -> dict[str, Any]:
        """Page immutable Feature identities in a stable keyset order."""
        allowed = {"kind", "step", "status"}
        normalized = dict(filters or {})
        unknown = set(normalized) - allowed
        if unknown:
            raise ResearchValidationError(
                f"unknown Feature filters: {', '.join(sorted(unknown))}"
            )
        if "kind" in normalized and normalized["kind"] not in {
            "primitive", "business", "statistical",
        }:
            raise ResearchValidationError("kind filter must be primitive, business, or statistical")
        if "step" in normalized and (
            type(normalized["step"]) is not int or normalized["step"] not in {3, 4, 5}
        ):
            raise ResearchValidationError("step filter must be 3, 4, or 5")
        if "status" in normalized and normalized["status"] not in {
            "ready", "pending_semantics", "retired",
        }:
            raise ResearchValidationError("invalid Feature status filter")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ResearchValidationError("limit must be an integer from 1 through 500")
        signature = hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()[:24]
        last: tuple[str, int] | None = None
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor:
                raise ResearchValidationError("cursor must be a non-empty string")
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"=" * (-len(cursor) % 4))
                payload = json.loads(decoded)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ResearchValidationError("cursor is malformed") from exc
            if not isinstance(payload, dict) or payload.get("query") != signature:
                raise ResearchValidationError("cursor does not match this query")
            key = payload.get("after")
            if (not isinstance(key, list) or len(key) != 2 or not isinstance(key[0], str)
                    or type(key[1]) is not int or key[1] < 1):
                raise ResearchValidationError("cursor key is invalid")
            last = (key[0], key[1])

        clauses, params = [], []
        for field in ("kind", "step", "status"):
            if field in normalized:
                clauses.append(f"{field}=?")
                params.append(normalized[field])
        if last is not None:
            clauses.append("(feature_id>? OR (feature_id=? AND version>?))")
            params.extend([last[0], last[0], last[1]])
        query = "SELECT feature_id,version,document_json FROM features"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY feature_id,version LIMIT ?"
        params.append(limit + 1)
        rows = self._call(lambda connection: _fetchall(connection, query, params))
        truncated = len(rows) > limit
        page = rows[:limit]
        next_cursor = None
        if truncated and page:
            tail = page[-1]
            encoded = json.dumps(
                {"query": signature, "after": [tail["feature_id"], tail["version"]]},
                ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
            next_cursor = base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")
        return {
            "schema_version": "feature_page_v1",
            "items": [json.loads(row["document_json"]) for row in page],
            "next_cursor": next_cursor,
            "truncated": truncated,
        }

    def _register_resource(self, table: str, id_col: str, resource_id: str,
                           version_col: str, version: str, document: dict[str, Any]) -> None:
        def insert(connection: duckdb.DuckDBPyConnection) -> None:
            self._insert(connection, table, id_col, resource_id, version_col, version, document)

        self._call(insert)

    def _register_with_provider(self, table: str, id_col: str, resource_id: str,
                                version_col: str, version: str, document: dict[str, Any],
                                provider: str) -> None:
        provider_id, provider_version = parse_resource_ref(provider, "provider_ref")

        def insert(connection: duckdb.DuckDBPyConnection) -> None:
            if _fetchone(connection,
                         "SELECT provider_id FROM providers WHERE provider_id=? AND version=?",
                         (provider_id, provider_version)) is None:
                raise UnknownReferenceError(f"unknown provider ref: {provider}", details={"ref": provider})
            self._insert(connection, table, id_col, resource_id, version_col, version, document)

        self._call(insert)

    @staticmethod
    def _insert(connection: duckdb.DuckDBPyConnection, table: str, id_col: str, resource_id: str,
                version_col: str, version: str | int, document: dict[str, Any]) -> None:
        if _fetchone(connection,
                     f"SELECT {id_col} FROM {table} WHERE {id_col}=? AND {version_col}=?",
                     (resource_id, version)) is not None:
            raise ImmutableVersionError(
                f"versioned resource already exists: {resource_id}@{version}",
                details={"ref": f"{resource_id}@{version}"},
            )
        if table == "providers":
            connection.execute(
                "INSERT INTO providers(provider_id,version,provider_key,document_json) "
                "VALUES(?,?,?,?)",
                (resource_id, version, document["provider_key"], canonical_json(document)),
            )
        else:
            connection.execute(
                f"INSERT INTO {table}({id_col},{version_col},document_json) VALUES(?,?,?)",
                (resource_id, version, canonical_json(document)),
            )

    def _get_resource(self, table: str, id_col: str, ref: str) -> dict[str, Any]:
        resource_id, version = parse_resource_ref(ref, f"{id_col}_ref")
        row = self._call(lambda connection: _fetchone(
            connection,
            f"SELECT document_json FROM {table} WHERE {id_col}=? AND version=?",
            (resource_id, version),
        ))
        if row is None:
            raise UnknownReferenceError(f"unknown resource ref: {ref}", details={"ref": ref})
        return json.loads(row["document_json"])


def _resolve_feature_set(connection: duckdb.DuckDBPyConnection,
                         draft: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    items = draft["items"]
    by_id = {item["item_id"]: item for item in items}
    resolved_by_item: dict[str, list[dict[str, Any]]] = {}
    feature_documents: dict[str, dict[str, Any]] = {}
    visiting: list[str] = []

    def resolve_item(item_id: str) -> list[dict[str, Any]]:
        if item_id in resolved_by_item:
            return resolved_by_item[item_id]
        if item_id in visiting:
            cycle_start = visiting.index(item_id)
            cycle = visiting[cycle_start:] + [item_id]
            raise ResearchValidationError(f"FeatureSet dependency cycle: {' -> '.join(cycle)}")
        item = by_id.get(item_id)
        if item is None:
            raise ResearchValidationError(f"unknown FeatureSet item id: {item_id}")
        visiting.append(item_id)
        try:
            if "feature_ref" in item:
                feature = _load_feature(connection, item["feature_ref"])
                outputs = [_resolved_output(feature)]
            else:
                outputs = _resolve_recipe(connection, draft, item["recipe"], resolve_item,
                                          feature_documents)
            output_ids = [output["output_id"] for output in outputs]
            if len(set(output_ids)) != len(output_ids):
                raise ResearchValidationError(f"item {item_id} resolves to duplicate output ids")
            resolved_by_item[item_id] = outputs
            return outputs
        finally:
            visiting.pop()

    resolved_items = []
    for item in items:
        outputs = resolve_item(item["item_id"])
        resolved_items.append({"item_id": item["item_id"], "alias": item["alias"],
                               "outputs": outputs})
    return resolved_items, feature_documents


def _resolve_recipe(connection: duckdb.DuckDBPyConnection, feature_set: dict[str, Any],
                    recipe: dict[str, Any], resolve_item: Callable[[str], list[dict[str, Any]]],
                    feature_documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    template = _load_resource(connection, "feature_templates", "template_id",
                              recipe["template_ref"], "template_ref")
    if recipe["step"] != template["step"]:
        raise ResearchValidationError("recipe.step must match its registered template")
    provider = _load_resource(connection, "providers", "provider_id",
                              template["provider_ref"], "provider_ref")
    if recipe["step"] not in provider["supported_steps"]:
        raise ResearchValidationError("provider does not support the registered template step")

    metric_docs: list[tuple[str, dict[str, Any]]] = []
    kind = template["kind"]
    if kind == "statistical":
        if ("metrics" in recipe) == ("metric_set_ref" in recipe):
            raise ResearchValidationError(
                "statistical recipe requires exactly one of metrics or metric_set_ref"
            )
        if "input" not in recipe:
            raise ResearchValidationError("statistical recipe requires one input reference")
        if "metrics" in recipe:
            metric_refs = recipe["metrics"]
        else:
            metric_set_id, metric_set_version = parse_feature_set_ref(
                recipe["metric_set_ref"], "recipe.metric_set_ref"
            )
            metric_set = _fetchone(
                connection,
                "SELECT document_json FROM metric_sets WHERE metric_set_id=? AND version=?",
                (metric_set_id, metric_set_version),
            )
            if metric_set is None:
                raise UnknownReferenceError(f"unknown metric set ref: {recipe['metric_set_ref']}",
                                            details={"ref": recipe["metric_set_ref"]})
            metric_refs = json.loads(metric_set["document_json"])["metrics"]
        for ref in metric_refs:
            metric_docs.append((ref, _load_resource(connection, "metrics", "metric_id", ref,
                                                    "metric_ref")))
    elif "metrics" in recipe or "metric_set_ref" in recipe:
        raise ResearchValidationError("metrics and metric_set_ref are only valid for step 05")

    input_refs, input_hashes, dependency_reasons, input_bindings = _resolve_inputs(
        connection, recipe, resolve_item, feature_documents
    )
    input_requirements = template.get("source_requirements", {})
    expected_roles = input_requirements.get("input_roles")
    if expected_roles is not None:
        actual_roles = [binding["role"] for binding in input_bindings]
        if set(actual_roles) != set(expected_roles) or len(actual_roles) != len(expected_roles):
            raise ResearchValidationError(
                f"recipe input roles must be exactly {expected_roles}"
            )
        expected_units = input_requirements.get("input_units", {})
        for binding in input_bindings:
            dependency = (
                feature_documents[binding["feature_ref"]]
                if binding["feature_ref"] in feature_documents
                else _load_feature(connection, binding["feature_ref"])
            )
            expected_unit = expected_units.get(binding["role"])
            if expected_unit is not None and dependency["unit"] != expected_unit:
                raise ResearchValidationError(
                    f"input {binding['role']} unit must be {expected_unit}"
                )
    if kind == "statistical" and not input_refs:
        raise ResearchValidationError("statistical recipe must resolve one input Feature")
    if (kind == "statistical"
            and template.get("source_requirements", {}).get("input_view_policy")
            == INPUT_VIEW_POLICY):
        if len(input_refs) != 1:
            raise ResearchValidationError(
                "statistical recipe must resolve exactly one whitelisted input Feature"
            )
        dependency = (
            feature_documents[input_refs[0]]
            if input_refs[0] in feature_documents
            else _load_feature(connection, input_refs[0])
        )
        validate_statistical_input(dependency, [ref for ref, _ in metric_docs])

    set_defaults = feature_set.get("defaults", {})
    time_policy = dict(template.get("time_policy", {}))
    for field in ("lookback", "profile_window"):
        if field in set_defaults and field not in time_policy:
            time_policy[field] = set_defaults[field]
        if field in recipe:
            time_policy[field] = recipe[field]
        if field in time_policy:
            time_policy[field] = normalize_duration(time_policy[field], f"recipe.{field}")
    for field in ("max_endpoint_age", "max_quote_age", "max_feature_age"):
        if field in recipe:
            time_policy[field] = recipe[field]
        if field in time_policy:
            time_policy[field] = normalize_duration(time_policy[field], f"template.time_policy.{field}")
    if recipe["step"] == 4 and "lookback" not in time_policy:
        raise ResearchValidationError("step 04 recipe requires an explicit lookback")
    if (recipe["step"] == 4
            and template["source_requirements"].get("requires_max_endpoint_age")
            and "max_endpoint_age" not in time_policy):
        raise ResearchValidationError(
            "step 04 endpoint recipe requires an explicit max_endpoint_age"
        )
    if recipe["step"] == 5 and "profile_window" not in time_policy:
        raise ResearchValidationError("step 05 recipe requires an explicit profile_window")

    sampling = _merge_objects(template.get("sampling", {}), set_defaults.get("sampling", {}),
                              recipe.get("sampling", {}))
    sampling = _normalize_sampling(sampling, "recipe.sampling") if sampling else {}
    if recipe["step"] == 5:
        if not sampling or "interval" not in sampling or "evaluation_stride" not in sampling:
            raise ResearchValidationError("step 05 recipe requires sampling interval and evaluation_stride")
        if "axis" not in sampling:
            raise ResearchValidationError("step 05 sampling requires an explicit axis/time_basis")
        if (template["source_requirements"].get("requires_max_feature_age")
                and "max_feature_age" not in sampling):
            raise ResearchValidationError(
                "step 05 recipe requires sampling.max_feature_age"
            )

    validity = _merge_objects(template.get("validity", {}), set_defaults.get("validity", {}),
                              recipe.get("validity", {}))
    validity = _normalize_validity(validity, "recipe.validity")
    if recipe["step"] == 5 and "min_samples" not in validity:
        raise ResearchValidationError("step 05 recipe requires validity.min_samples")

    feature_outputs = []
    metrics: list[tuple[str | None, dict[str, Any] | None]] = metric_docs or [(None, None)]
    for metric_ref_value, metric in metrics:
        formula_ref = metric["formula_ref"] if metric else template["formula_ref"]
        if metric:
            template_schema = template["parameters_schema"]
            metric_schema = metric["parameters_schema"]
            combined_schema = _combine_parameter_schemas(template_schema, metric_schema)
            defaults = {**template["defaults"], **metric["defaults"]}
            metric_provider = _load_resource(connection, "providers", "provider_id",
                                              metric["provider_ref"], "metric.provider_ref")
            if recipe["step"] not in metric_provider["supported_steps"]:
                raise ResearchValidationError("metric provider does not support step 05")
        else:
            combined_schema = template["parameters_schema"]
            defaults = dict(template["defaults"])
            metric_provider = None
        parameters = normalize_parameters({**defaults, **recipe["parameters"]}, combined_schema,
                                          "recipe.parameters")
        if metric_ref_value is not None:
            parameters = {**parameters, "metric_ref": metric_ref_value}

        implementation = {
            "provider_ref": template["provider_ref"],
            "provider_key": provider["provider_key"],
            "implementation_version": provider["implementation_version"],
        }
        if metric_provider is not None:
            implementation["metric_provider_ref"] = metric["provider_ref"]
            implementation["metric_provider_key"] = metric_provider["provider_key"]
            implementation["metric_implementation_version"] = metric_provider["implementation_version"]

        output_schema = metric["output_schema"] if metric else template["output_schema"]
        unit = metric["unit"] if metric else template["unit"]
        name = metric["name"] if metric else template["name"]
        description = metric["description"] if metric else template["description"]
        status_reasons = []
        if template["status"] == "pending_semantics":
            status_reasons.append(template["reason"])
        if metric and metric["status"] == "pending_semantics":
            status_reasons.append(metric["reason"])
        status_reasons.extend(dependency_reasons)
        status = "pending_semantics" if status_reasons else "ready"
        reason = "; ".join(dict.fromkeys(status_reasons)) if status_reasons else None

        semantic = {
            "kind": kind,
            "step": recipe["step"],
            "family": template["family"],
            "formula_ref": formula_ref,
            "template_formula_ref": template["formula_ref"],
            "implementation": implementation,
            "parameters": parameters,
            "input_bindings": input_bindings,
            "input_definition_hashes": input_hashes,
            "source_requirements": template["source_requirements"],
            "time_policy": time_policy,
            "sampling": sampling,
            "validity": validity,
            "output_schema": output_schema,
            "unit": unit,
            "status": status,
            "reason": reason,
        }
        definition_hash = _hash_json(semantic)
        digest = definition_hash.removeprefix("sha256:")
        feature_id = "feature_" + digest[:24]
        feature = {
            "schema_version": "feature_v1",
            "feature_id": feature_id,
            "version": 1,
            "name": name,
            "description": description,
            "step": recipe["step"],
            "kind": kind,
            "family": template["family"],
            "formula_ref": formula_ref,
            "template_formula_ref": template["formula_ref"],
            "template_ref": recipe["template_ref"],
            "implementation": implementation,
            "parameters": parameters,
            "input_feature_refs": input_refs,
            "input_bindings": input_bindings,
            "source_requirements": template["source_requirements"],
            "time_policy": time_policy,
            "sampling": sampling,
            "validity": validity,
            "output_schema": output_schema,
            "unit": unit,
            "definition_hash": definition_hash,
            "status": status,
        }
        if reason:
            feature["reason"] = reason
        if metric_ref_value is not None:
            feature["metric_ref"] = metric_ref_value
        feature_ref_value = f"{feature_id}@1"
        feature_documents[feature_ref_value] = feature
        feature_outputs.append(_resolved_output(feature, metric_ref_value))
    return feature_outputs


def _resolve_inputs(connection: duckdb.DuckDBPyConnection, recipe: dict[str, Any],
                    resolve_item: Callable[[str], list[dict[str, Any]]],
                    feature_documents: dict[str, dict[str, Any]]
                    ) -> tuple[list[str], list[str], list[str], list[dict[str, str]]]:
    if "input" in recipe:
        bindings = {"input": recipe["input"]}
    elif "inputs" in recipe:
        bindings = recipe["inputs"]
    else:
        return [], [], [], []
    refs: list[str] = []
    normalized_bindings = []
    for role, input_ref in bindings.items():
        refs.extend(_resolve_one_input(input_ref, resolve_item))
        normalized_bindings.append({"role": role, "feature_ref": refs[-1]})
    docs = [feature_documents[ref] if ref in feature_documents else _load_feature(connection, ref)
            for ref in refs]
    reason = [f"input {feature['feature_id']} is {feature['status']}"
              for feature in docs if feature["status"] != "ready"]
    return refs, [feature["definition_hash"] for feature in docs], reason, normalized_bindings


def _resolve_one_input(input_ref: dict[str, Any],
                       resolve_item: Callable[[str], list[dict[str, Any]]]) -> list[str]:
    if "input_feature_ref" in input_ref:
        return [input_ref["input_feature_ref"]]
    else:
        candidates = resolve_item(input_ref["item_id"])
        output_id = input_ref.get("output_id")
        if output_id is None:
            if len(candidates) != 1:
                raise ResearchValidationError(
                    f"input item {input_ref['item_id']} has multiple outputs; output_id is required"
                )
            selected = candidates[0]
        else:
            selected = next((output for output in candidates if output["output_id"] == output_id), None)
            if selected is None:
                raise ResearchValidationError(
                    f"unknown output_id {output_id!r} for item {input_ref['item_id']}"
                )
        return [selected["feature_ref"]]


def _resolved_output(feature: dict[str, Any], metric: str | None = None) -> dict[str, Any]:
    metric = metric if metric is not None else feature.get("metric_ref")
    identity = {"definition_hash": feature["definition_hash"], "metric_ref": metric}
    output_id = "out_" + hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()[:16]
    return {"output_id": output_id, "feature_ref": f"{feature['feature_id']}@{feature['version']}",
            "definition_hash": feature["definition_hash"], "status": feature["status"]}


def _load_feature(connection: duckdb.DuckDBPyConnection, ref: str) -> dict[str, Any]:
    feature_id, version = parse_feature_ref(ref, "feature_ref")
    row = _fetchone(connection,
                    "SELECT document_json FROM features WHERE feature_id=? AND version=?",
                    (feature_id, version))
    if row is None:
        raise UnknownReferenceError(f"unknown concrete Feature ref: {ref}", details={"ref": ref})
    return json.loads(row["document_json"])


def _load_resource(connection: duckdb.DuckDBPyConnection, table: str, id_col: str,
                   ref: str, path: str) -> dict[str, Any]:
    resource_id, version = parse_resource_ref(ref, path)
    row = _fetchone(connection,
                    f"SELECT document_json FROM {table} WHERE {id_col}=? AND version=?",
                    (resource_id, version))
    if row is None:
        raise UnknownReferenceError(f"unknown versioned resource: {ref}", details={"ref": ref})
    return json.loads(row["document_json"])


def _combine_parameter_schemas(*schemas: dict[str, Any]) -> dict[str, Any]:
    properties: dict[str, Any] = {}
    required: set[str] = set()
    for schema in schemas:
        for name, spec in schema["properties"].items():
            if name in properties and canonical_json(properties[name]) != canonical_json(spec):
                raise ResearchValidationError(
                    f"ambiguous parameter {name!r} appears with different template/metric schemas"
                )
            properties[name] = spec
        required.update(schema["required"])
    return {"type": "object", "properties": properties, "required": sorted(required),
            "additionalProperties": False}


def _merge_objects(*values: dict[str, Any]) -> dict[str, Any]:
    merged = {}
    for value in values:
        merged.update(value)
    return merged


def _hash_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
