"""DuckDB persistence for the first research management increment."""

from __future__ import annotations

import json
from collections.abc import Callable
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
from .schemas import parse_versioned_ref

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


class ResearchRepository:
    """Persist validated research documents through the Registry's owner queue."""

    def __init__(self, registry: Registry):
        self._registry = registry

    def _call(self, operation: Callable[[duckdb.DuckDBPyConnection], _T]) -> _T:
        return self._registry._call(operation)

    def register_source(self, document: dict[str, Any]) -> dict[str, Any]:
        created_at = utc_now()
        source_id = document["source_id"]
        version = document["version"]
        manifest_ref = document["manifest_ref"]
        document = {**document, "created_at": created_at}

        def insert(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            existing = _fetchone(
                connection, "SELECT version FROM data_sources WHERE source_id=? ORDER BY version DESC LIMIT 1",
                (source_id,),
            )
            expected_version = 1 if existing is None else int(existing["version"]) + 1
            if version != expected_version:
                raise ImmutableVersionError(
                    f"source version must be the next immutable version ({expected_version})",
                    details={"source_id": source_id, "expected_version": expected_version},
                )
            try:
                connection.execute(
                    "INSERT INTO data_sources(source_id,version,content_hash,manifest_uri,manifest_hash,"
                    "document_json,created_at,created_by_json) VALUES(?,?,?,?,?,?,?,?)",
                    (source_id, version, document["content_hash"], manifest_ref["uri"],
                     manifest_ref["content_hash"], canonical_json(document), created_at,
                     canonical_json(document["created_by"])),
                )
            except duckdb.ConstraintException as exc:
                raise ImmutableVersionError(
                    f"DataSource identity already exists: {source_id}@{version}",
                    details={"source_id": source_id, "version": version},
                ) from exc
            return document

        return self._call(insert)

    def get_source(self, source_id: str, version: int) -> dict[str, Any] | None:
        row = self._call(lambda connection: _fetchone(
            connection,
            "SELECT document_json FROM data_sources WHERE source_id=? AND version=?",
            (source_id, version),
        ))
        return json.loads(row["document_json"]) if row else None

    def require_sources(self, connection: duckdb.DuckDBPyConnection,
                        source_refs: list[str]) -> None:
        for source_ref in source_refs:
            source_id, version = parse_versioned_ref(source_ref, "study.source_refs[]")
            if _fetchone(connection,
                         "SELECT source_id FROM data_sources WHERE source_id=? AND version=?",
                         (source_id, version)) is None:
                raise UnknownReferenceError(
                    f"unknown DataSource ref: {source_ref}", details={"ref": source_ref}
                )

    def create_study(self, document: dict[str, Any]) -> dict[str, Any]:
        created_at = utc_now()
        study = {**document, "revision": 1, "created_at": created_at,
                 "updated_at": created_at, "updated_by": document["created_by"]}
        study_id = study["study_id"]

        def insert(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            self.require_sources(connection, study["source_refs"])
            if _fetchone(connection, "SELECT study_id FROM studies WHERE study_id=?", (study_id,)):
                raise ImmutableVersionError(
                    f"Study identity already exists: {study_id}", details={"study_id": study_id}
                )
            connection.execute(
                "INSERT INTO studies(study_id,current_revision,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?)",
                (study_id, 1, study["status"], created_at, created_at),
            )
            connection.execute(
                "INSERT INTO study_revisions(study_id,revision,document_json,actor_json,reason,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (study_id, 1, canonical_json(study), canonical_json(study["created_by"]),
                 study["change_reason"], created_at),
            )
            return study

        return self._call(insert)

    def update_study(self, study_id: str, document: dict[str, Any], *,
                     expected_revision: int, actor: dict[str, str], reason: str) -> dict[str, Any]:
        updated_at = utc_now()

        def update(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            row = _fetchone(connection, "SELECT current_revision,status FROM studies WHERE study_id=?",
                            (study_id,))
            if row is None:
                raise UnknownReferenceError(f"unknown Study: {study_id}", details={"study_id": study_id})
            current_revision = int(row["current_revision"])
            if current_revision != expected_revision:
                raise RevisionConflictError(
                    f"Study {study_id} revision changed from expected {expected_revision} "
                    f"to {current_revision}",
                    details={"study_id": study_id, "expected_revision": expected_revision,
                             "current_revision": current_revision},
                )
            current_row = _fetchone(
                connection,
                "SELECT document_json FROM study_revisions WHERE study_id=? AND revision=?",
                (study_id, current_revision),
            )
            current = json.loads(current_row["document_json"])
            if current["status"] == "closed" and document["status"] != "closed":
                raise ResearchValidationError("closed Study status cannot be reopened")
            self.require_sources(connection, document["source_refs"])
            next_revision = current_revision + 1
            updated = {**document, "revision": next_revision,
                       "created_at": current["created_at"], "updated_at": updated_at,
                       "updated_by": actor, "change_reason": reason}
            connection.execute(
                "INSERT INTO study_revisions(study_id,revision,document_json,actor_json,reason,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (study_id, next_revision, canonical_json(updated), canonical_json(actor), reason,
                 updated_at),
            )
            connection.execute(
                "UPDATE studies SET current_revision=?,status=?,updated_at=? WHERE study_id=? "
                "AND current_revision=?",
                (next_revision, updated["status"], updated_at, study_id, current_revision),
            )
            return updated

        return self._call(update)

    def get_study(self, study_id: str, revision: int | None = None) -> dict[str, Any] | None:
        def select(connection: duckdb.DuckDBPyConnection) -> dict[str, Any] | None:
            if revision is None:
                row = _fetchone(
                    connection,
                    "SELECT r.document_json FROM studies s JOIN study_revisions r "
                    "ON s.study_id=r.study_id AND s.current_revision=r.revision WHERE s.study_id=?",
                    (study_id,),
                )
            else:
                row = _fetchone(
                    connection,
                    "SELECT document_json FROM study_revisions WHERE study_id=? AND revision=?",
                    (study_id, revision),
                )
            return json.loads(row["document_json"]) if row else None

        return self._call(select)

    def study_history(self, study_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT revision,document_json,actor_json,reason,created_at FROM study_revisions "
            "WHERE study_id=? ORDER BY revision",
            (study_id,),
        ))
        return [{"revision": int(row["revision"]), "document": json.loads(row["document_json"]),
                 "actor": json.loads(row["actor_json"]), "reason": row["reason"],
                 "created_at": row["created_at"]} for row in rows]

    def add_review(self, document: dict[str, Any]) -> dict[str, Any]:
        """Append a validated, evidence-bound Review without changing its evidence."""
        created_at = utc_now()
        stored = {**document, "created_at": created_at, "created_by": dict(document["actor"])}

        def insert(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
            study_id = stored["study_id"]
            if _fetchone(connection, "SELECT study_id FROM studies WHERE study_id=?", (study_id,)) is None:
                raise UnknownReferenceError(f"unknown Study: {study_id}", details={"study_id": study_id})

            for run_id in stored["run_refs"]:
                run = _fetchone(
                    connection,
                    "SELECT research_study_id FROM runs WHERE run_id=? AND research_study_id IS NOT NULL",
                    (run_id,),
                )
                if run is None:
                    raise UnknownReferenceError(f"unknown research Run: {run_id}",
                                                details={"run_id": run_id})
                if run["research_study_id"] != study_id:
                    raise ResearchValidationError(
                        f"Review Run {run_id} does not belong to Study {study_id}",
                        details={"run_id": run_id, "study_id": study_id},
                    )

            for result_id in stored["result_refs"]:
                result_row = _fetchone(
                    connection,
                    "SELECT result_json FROM feature_results WHERE result_id=?",
                    (result_id,),
                )
                if result_row is None:
                    raise UnknownReferenceError(f"unknown FeatureResult: {result_id}",
                                                details={"result_id": result_id})
                result = json.loads(result_row["result_json"])
                producer = result.get("producer_run_id")
                producer_row = _fetchone(
                    connection,
                    "SELECT research_study_id FROM runs WHERE run_id=? AND research_study_id IS NOT NULL",
                    (producer,),
                )
                if producer_row is None:
                    raise ResearchValidationError(
                        f"Review FeatureResult {result_id} has no registered producer Run",
                        details={"result_id": result_id},
                    )
                if not any(
                    _fetchone(connection,
                             "SELECT result_id FROM run_result_links WHERE run_id=? AND result_id=?",
                             (run_id, result_id)) is not None
                    for run_id in stored["run_refs"]
                ):
                    raise ResearchValidationError(
                        f"Review FeatureResult {result_id} is not linked to a referenced Run",
                        details={"result_id": result_id},
                    )

            supersedes = stored.get("supersedes_review_id")
            if supersedes is not None:
                previous = _fetchone(
                    connection, "SELECT study_id FROM reviews WHERE review_id=?", (supersedes,)
                )
                if previous is None:
                    raise UnknownReferenceError(f"unknown Review: {supersedes}",
                                                details={"review_id": supersedes})
                if previous["study_id"] != study_id:
                    raise ResearchValidationError(
                        "superseded Review must belong to the same Study",
                        details={"review_id": supersedes, "study_id": study_id},
                    )

            try:
                connection.execute(
                    "INSERT INTO reviews(review_id,study_id,run_refs_json,result_refs_json,created_at,"
                    "document_json) VALUES(?,?,?,?,?,?)",
                    (stored["review_id"], study_id, canonical_json(stored["run_refs"]),
                     canonical_json(stored["result_refs"]), created_at, canonical_json(stored)),
                )
            except duckdb.ConstraintException as exc:
                raise ImmutableVersionError(
                    f"Review identity already exists: {stored['review_id']}",
                    details={"review_id": stored["review_id"]},
                ) from exc
            return stored

        return self._call(insert)

    def get_review(self, review_id: str) -> dict[str, Any] | None:
        row = self._call(lambda connection: _fetchone(
            connection, "SELECT document_json FROM reviews WHERE review_id=?", (review_id,)
        ))
        return json.loads(row["document_json"]) if row else None

    def list_reviews(self, study_id: str) -> list[dict[str, Any]]:
        rows = self._call(lambda connection: _fetchall(
            connection,
            "SELECT document_json FROM reviews WHERE study_id=? ORDER BY review_order",
            (study_id,),
        ))
        return [json.loads(row["document_json"]) for row in rows]

    def list_reviews_page(self, study_id: str, *, run_id: str | None = None,
                          result_ref: str | None = None, offset: int = 0,
                          limit: int = 101) -> tuple[list[dict[str, Any]], bool]:
        """Read one bounded Review page, filtering references in DuckDB."""
        clauses = ["study_id=?"]
        params: list[Any] = [study_id]
        if run_id is not None:
            clauses.append("json_contains(run_refs_json, to_json(?))")
            params.append(run_id)
        if result_ref is not None:
            clauses.append("json_contains(result_refs_json, to_json(?))")
            params.append(result_ref)
        where = " AND ".join(clauses)

        def select(connection: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], bool]:
            rows = _fetchall(
                connection,
                f"SELECT document_json FROM reviews WHERE {where} ORDER BY review_order "
                "LIMIT ? OFFSET ?",
                [*params, limit, offset],
            )
            return ([json.loads(row["document_json"]) for row in rows[:limit - 1]],
                    len(rows) >= limit)

        return self._call(select)
