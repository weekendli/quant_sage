"""`quantsage research` CLI for the local ResearchService owner."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from quantsage_discovery.core.errors import WorkflowError

from .research.rpc import (
    CLIENT_TIMEOUT_SECONDS,
    MAX_REQUEST_BYTES,
    ResearchOwner,
    RpcError,
    RpcProtocolError,
    RpcRequestTooLargeError,
    _envelope,
    _error_document,
    _error_exit_code,
    rpc_call,
)


def _timeout_value(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a number of seconds") from exc
    if not 0.1 <= value <= 60:
        raise argparse.ArgumentTypeError("timeout must be between 0.1 and 60 seconds")
    return value


def _rpc_options(parser: argparse.ArgumentParser, *, timeout: bool = True) -> None:
    parser.add_argument("--database", required=True, help="explicit DuckDB registry path")
    parser.add_argument("--socket", help="owner Unix socket path (derived from --database by default)")
    if timeout:
        parser.add_argument("--timeout", type=_timeout_value, default=CLIENT_TIMEOUT_SECONDS,
                            help="socket request timeout in seconds (default: 25)")


def add_research_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "research", help="manage research metadata and runs through the local owner",
    )
    resources = parser.add_subparsers(dest="research_resource", required=True)

    owner = resources.add_parser("owner", help="start, inspect, or stop the local owner")
    owner_actions = owner.add_subparsers(dest="owner_action", required=True)
    serve = owner_actions.add_parser("serve", help="hold the ResearchService registry owner")
    _rpc_options(serve, timeout=False)
    stop = owner_actions.add_parser("stop", help="request graceful owner shutdown")
    _rpc_options(stop)
    status = owner_actions.add_parser("status", help="inspect the active owner")
    _rpc_options(status)

    sources = resources.add_parser("sources", help="register and inspect frozen DataSources")
    source_actions = sources.add_subparsers(dest="source_action", required=True)
    source_register = source_actions.add_parser("register")
    _rpc_options(source_register)
    source_register.add_argument("--input", required=True, help="DataSource JSON file or - for stdin")
    source_get = source_actions.add_parser("get")
    _rpc_options(source_get)
    source_get.add_argument("ref")

    studies = resources.add_parser("studies", help="create, revise, and inspect Studies")
    study_actions = studies.add_subparsers(dest="study_action", required=True)
    study_create = study_actions.add_parser("create")
    _rpc_options(study_create)
    study_create.add_argument("--input", required=True, help="Study JSON file or - for stdin")
    study_update = study_actions.add_parser("update")
    _rpc_options(study_update)
    study_update.add_argument("study_id")
    study_update.add_argument("--input", required=True, help="Study changes JSON file or - for stdin")
    study_update.add_argument("--expected-revision", required=True, type=int)
    study_update.add_argument("--actor-kind", required=True, choices=("human", "agent"))
    study_update.add_argument("--actor-id", required=True)
    study_update.add_argument("--reason", required=True)
    study_get = study_actions.add_parser("get")
    _rpc_options(study_get)
    study_get.add_argument("study_id")
    study_get.add_argument("--revision", type=int)
    study_coverage = study_actions.add_parser("coverage")
    _rpc_options(study_coverage)
    study_coverage.add_argument("--input", required=True,
                                help="coverage params JSON file or - for stdin")

    catalog = resources.add_parser("catalog", help="register provider and feature catalog documents")
    catalog_actions = catalog.add_subparsers(dest="catalog_action", required=True)
    catalog_register = catalog_actions.add_parser("register")
    _rpc_options(catalog_register)
    catalog_register.add_argument("resource_type", choices=("provider", "template", "metric", "metric-set"))
    catalog_register.add_argument("--input", required=True,
                                  help="catalog document JSON file or - for stdin")
    catalog_get = catalog_actions.add_parser("get")
    _rpc_options(catalog_get)
    catalog_get.add_argument("resource_type", choices=("provider", "template", "metric", "metric-set"))
    catalog_get.add_argument("ref")

    feature_sets = resources.add_parser("feature-sets", help="save, freeze, and inspect FeatureSets")
    feature_set_actions = feature_sets.add_subparsers(dest="feature_set_action", required=True)
    feature_set_save = feature_set_actions.add_parser("save")
    _rpc_options(feature_set_save)
    feature_set_save.add_argument("--input", required=True,
                                  help="FeatureSet draft JSON file or - for stdin")
    feature_set_save.add_argument("--expected-revision", type=int)
    feature_set_freeze = feature_set_actions.add_parser("freeze")
    _rpc_options(feature_set_freeze)
    feature_set_freeze.add_argument("feature_set_ref")
    feature_set_freeze.add_argument("--expected-revision", required=True, type=int)
    feature_set_get = feature_set_actions.add_parser("get")
    _rpc_options(feature_set_get)
    feature_set_get.add_argument("feature_set_ref")

    features = resources.add_parser("features", help="inspect registered Feature definitions")
    feature_actions = features.add_subparsers(dest="feature_action", required=True)
    feature_get = feature_actions.add_parser("get")
    _rpc_options(feature_get)
    feature_get.add_argument("feature_ref")
    feature_list = feature_actions.add_parser("list")
    _rpc_options(feature_list)
    feature_list.add_argument("--input", help="filters JSON file or - for stdin")
    feature_list.add_argument("--limit", type=int, default=100)
    feature_list.add_argument("--cursor")

    runs = resources.add_parser("runs", help="plan, submit, execute, resume, and compare Runs")
    run_actions = runs.add_subparsers(dest="run_action", required=True)
    run_plan = run_actions.add_parser("plan")
    _rpc_options(run_plan)
    run_plan.add_argument("--input", required=True, help="Run request JSON file or - for stdin")
    run_submit = run_actions.add_parser("submit")
    _rpc_options(run_submit)
    run_submit.add_argument("--input", required=True, help="Run request JSON file or - for stdin")
    run_submit.add_argument("--request-key", required=True)
    run_submit.add_argument("--expected-plan-hash")
    for action in ("execute", "resume", "get"):
        run = run_actions.add_parser(action)
        _rpc_options(run)
        run.add_argument("run_id")
    run_compare = run_actions.add_parser("compare")
    _rpc_options(run_compare)
    run_compare.add_argument("left_run_id")
    run_compare.add_argument("right_run_id")
    run_compare.add_argument("--input", help="comparison scope JSON file or - for stdin")
    run_compare.add_argument("--no-verify", action="store_true")

    results = resources.add_parser("results", help="list FeatureResults or read bounded rows")
    result_actions = results.add_subparsers(dest="result_action", required=True)
    result_list = result_actions.add_parser("list")
    _rpc_options(result_list)
    result_list.add_argument("--input", help="filters JSON file or - for stdin")
    result_list.add_argument("--limit", type=int, default=100)
    result_list.add_argument("--cursor")
    result_list.add_argument("--no-verify", action="store_true")
    result_read = result_actions.add_parser("read")
    _rpc_options(result_read)
    result_read.add_argument("--input", required=True,
                             help="result_refs/columns/scope JSON file or - for stdin")
    result_read.add_argument("--limit", type=int, default=500)
    result_read.add_argument("--cursor")

    reviews = resources.add_parser("reviews", help="append and inspect evidence-bound Reviews")
    review_actions = reviews.add_subparsers(dest="review_action", required=True)
    review_add = review_actions.add_parser("add")
    _rpc_options(review_add)
    review_add.add_argument("--input", required=True, help="Review JSON file or - for stdin")
    review_list = review_actions.add_parser("list")
    _rpc_options(review_list)
    review_list.add_argument("study_id")
    review_list.add_argument("--run-id")
    review_list.add_argument("--result-ref")
    review_list.add_argument("--limit", type=int, default=100)
    review_list.add_argument("--cursor")

    return parser


def _read_input(value: str | None, *, optional: bool = False) -> dict[str, Any]:
    if value is None:
        if optional:
            return {}
        raise RpcProtocolError("--input is required")
    if value == "-":
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    else:
        try:
            with Path(value).expanduser().open("rb") as stream:
                raw = stream.read(MAX_REQUEST_BYTES + 1)
        except OSError as exc:
            raise RpcProtocolError(f"cannot read JSON input {value!r}: {exc}") from exc
    if len(raw) > MAX_REQUEST_BYTES:
        raise RpcRequestTooLargeError(
            "JSON input exceeds the 8 MiB limit",
            details={"maximum_bytes": MAX_REQUEST_BYTES},
        )
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RpcProtocolError(f"input is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise RpcProtocolError("input JSON must be an object")
    return document


def _request(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    resource = args.research_resource
    if resource == "owner":
        if args.owner_action == "serve":
            raise RuntimeError("owner serve is handled locally")
        return f"owner.{args.owner_action}", {}
    if resource == "sources":
        if args.source_action == "register":
            return "sources.register", {"document": _read_input(args.input)}
        return "sources.get", {"ref": args.ref}
    if resource == "studies":
        if args.study_action == "create":
            return "studies.create", {"document": _read_input(args.input)}
        if args.study_action == "update":
            return "studies.update", {
                "study_id": args.study_id,
                "changes": _read_input(args.input),
                "expected_revision": args.expected_revision,
                "actor": {"kind": args.actor_kind, "id": args.actor_id},
                "reason": args.reason,
            }
        if args.study_action == "get":
            params = {"study_id": args.study_id}
            if args.revision is not None:
                params["revision"] = args.revision
            return "studies.get", params
        return "studies.coverage", _read_input(args.input)
    if resource == "catalog":
        resource_type = args.resource_type.replace("-", "_") + "s"
        if args.catalog_action == "register":
            return f"catalog.{resource_type}.register", {"document": _read_input(args.input)}
        return f"catalog.{resource_type}.get", {"ref": args.ref}
    if resource == "feature-sets":
        if args.feature_set_action == "save":
            params = {"document": _read_input(args.input)}
            if args.expected_revision is not None:
                params["expected_revision"] = args.expected_revision
            return "feature_sets.save", params
        if args.feature_set_action == "freeze":
            return "feature_sets.freeze", {
                "ref": args.feature_set_ref, "expected_revision": args.expected_revision,
            }
        return "feature_sets.get", {"ref": args.feature_set_ref}
    if resource == "features":
        if args.feature_action == "get":
            return "features.get", {"ref": args.feature_ref}
        return "features.list", {
            "filters": _read_input(args.input, optional=True),
            "limit": args.limit, "cursor": args.cursor,
        }
    if resource == "runs":
        if args.run_action == "plan":
            return "runs.plan", {"request": _read_input(args.input)}
        if args.run_action == "submit":
            params = {"request": _read_input(args.input), "request_key": args.request_key}
            if args.expected_plan_hash is not None:
                params["expected_plan_hash"] = args.expected_plan_hash
            return "runs.submit", params
        if args.run_action in {"execute", "resume", "get"}:
            return f"runs.{args.run_action}", {"run_id": args.run_id}
        scope = _read_input(args.input, optional=True)
        return "runs.compare", {
            "left_run_id": args.left_run_id, "right_run_id": args.right_run_id,
            "scope": scope or None, "verify": not args.no_verify,
        }
    if resource == "results":
        if args.result_action == "list":
            return "results.list", {
                "filters": _read_input(args.input, optional=True),
                "limit": args.limit, "cursor": args.cursor, "verify": not args.no_verify,
            }
        params = _read_input(args.input)
        params["limit"] = args.limit
        params["cursor"] = args.cursor
        return "results.read", params
    if resource == "reviews":
        if args.review_action == "add":
            return "reviews.add", {"document": _read_input(args.input)}
        params = {
            "study_id": args.study_id, "limit": args.limit, "cursor": args.cursor,
        }
        if args.run_id is not None:
            params["run_id"] = args.run_id
        if args.result_ref is not None:
            params["result_ref"] = args.result_ref
        return "reviews.list", params
    raise RpcProtocolError("unknown research CLI resource")


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str))


def handle_research(args: argparse.Namespace) -> int:
    if args.research_resource == "owner" and args.owner_action == "serve":
        owner = ResearchOwner(args.database, args.socket)
        try:
            owner.serve_forever()
            return 0
        except (RpcError, WorkflowError, OSError) as exc:
            error = _error_document(exc)
            _print(_envelope(request_id=None, ok=False, error=error))
            return _error_exit_code(error)

    try:
        if not 0.1 <= args.timeout <= 60:
            raise RpcProtocolError("timeout must be between 0.1 and 60 seconds")
        method, params = _request(args)
        response = rpc_call(
            args.database, method, params, socket_path=args.socket, timeout=args.timeout,
        )
    except (RpcError, OSError, ValueError) as exc:
        error = _error_document(exc)
        _print(_envelope(request_id=None, ok=False, error=error))
        return _error_exit_code(error)
    _print(response)
    return _error_exit_code(response.get("error"))
