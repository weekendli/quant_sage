"""Argparse CLI for config validation, workflow planning, and run inspection."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from .config import configured_path, load_config, load_stage_plugins
from .core.artifacts import ArtifactStore, artifact_path
from .core.errors import WorkflowError
from .core.registry import Registry
from .core.research import BatchEngine, batch_status
from .core.workflow import WorkflowEngine


def _registry_path(config: dict[str, Any] | None, explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    if config is not None:
        value = config.get("storage", {}).get("registry_uri")
        if value:
            path = Path(value).expanduser()
            return path if path.is_absolute() else (Path.cwd() / path).resolve()
        return configured_path(config, "artifacts_uri", "artifacts") / "registry.sqlite"
    return (Path.cwd() / "artifacts" / "registry.sqlite").resolve()


def _artifact_root(config: dict[str, Any]) -> Path:
    return configured_path(config, "artifacts_uri", "artifacts")


def _engine(config: dict[str, Any], *, registry: Registry | None = None,
            artifact_root: Path | None = None) -> WorkflowEngine:
    store = ArtifactStore(artifact_root or _artifact_root(config))
    if "research_batch" in config:
        return BatchEngine(config, registry=registry or Registry(_registry_path(config)),
                           artifact_store=store)
    return WorkflowEngine(stages=load_stage_plugins(config), config=config,
                          artifact_store=store, registry=registry)


def _json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, default=str))


def _partition_scopes(values: list[str] | None) -> list[dict[str, str]] | None:
    if not values:
        return None
    scopes = []
    for value in values:
        scope: dict[str, str] = {}
        for item in value.split(","):
            if "=" not in item:
                raise WorkflowError(f"partition scope must use key=value pairs: {item!r}")
            key, partition_value = item.split("=", 1)
            key = key.strip()
            if not key or key in scope:
                raise WorkflowError(f"invalid or duplicate partition key in {value!r}")
            scope[key] = partition_value.strip()
        scopes.append(scope)
    return scopes


def _add_config_argument(parser: argparse.ArgumentParser, required: bool = True) -> None:
    parser.add_argument("--config", required=required, help="YAML workflow configuration")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quantsage", description="QuantSage local workflow engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="validate config and registered stage graph")
    _add_config_argument(validate)

    plan = subparsers.add_parser("plan", help="print deterministic stage graph and selection")
    _add_config_argument(plan)
    plan.add_argument("--from", dest="from_stage")
    plan.add_argument("--until", dest="until_stage")
    plan.add_argument("--only", action="append", dest="only_stages")

    run = subparsers.add_parser("run", help="execute or resume a workflow")
    _add_config_argument(run, required=False)
    run.add_argument("--resume", metavar="RUN_ID")
    run.add_argument("--run-id", help="explicit id for a new run")
    run.add_argument("--from", dest="from_stage")
    run.add_argument("--until", dest="until_stage")
    run.add_argument("--only", action="append", dest="only_stages")
    run.add_argument("--force-stage", action="append", dest="force_stages")
    run.add_argument("--partition-scope", action="append", dest="partition_scopes",
                     metavar="KEY=VALUE[,KEY=VALUE...]")
    run.add_argument("--registry", help="registry sqlite path (primarily for --resume)")

    status = subparsers.add_parser("status", help="show persisted run and stage states")
    status.add_argument("--run-id", required=True)
    _add_config_argument(status, required=False)
    status.add_argument("--registry")
    status.add_argument('--item', action='append', dest='item_filters',
                        help='research item alias or catalog definition key (repeatable)')
    status.add_argument('--item-state', choices=('PENDING', 'RUNNING', 'SUCCEEDED', 'CACHED',
                                               'FAILED', 'BLOCKED', 'SKIPPED'))
    status.add_argument('--item-kind', choices=('primitive', 'business', 'statistical', 'validation'))

    artifacts = subparsers.add_parser("artifacts", help="list persisted artifact refs for a run")
    artifacts.add_argument("--run-id", required=True)
    artifacts.add_argument("--stage")
    _add_config_argument(artifacts, required=False)
    artifacts.add_argument("--registry")

    explain = subparsers.add_parser("explain", help="show artifact manifest and input lineage")
    explain.add_argument("--artifact-id", required=True)
    _add_config_argument(explain, required=False)
    explain.add_argument("--registry")
    return parser


def _load_registry(args: argparse.Namespace, config: dict[str, Any] | None = None) -> Registry:
    return Registry(_registry_path(config, getattr(args, "registry", None)))


def _validate_or_plan(args: argparse.Namespace, is_plan: bool) -> int:
    config = load_config(args.config)
    if "research_batch" in config:
        if any(getattr(args, key, None) for key in ('from_stage', 'until_stage', 'only_stages')):
            raise WorkflowError('research_batch targets/allowed_stages replace CLI stage selectors')
        engine = _engine(config)
        plan = engine.batch_plan()
        _json(plan if is_plan else {'valid': True, 'graph': plan})
        return 0
    plugins = load_stage_plugins(config)
    with tempfile.TemporaryDirectory(prefix="quantsage-plan-") as temporary:
        engine = WorkflowEngine(stages=plugins, config=config,
                                artifact_store=ArtifactStore(Path(temporary) / "artifacts"),
                                registry=Registry(Path(temporary) / "registry.sqlite"))
        graph = engine.plan(from_stage=getattr(args, "from_stage", None),
                            until_stage=getattr(args, "until_stage", None),
                            only=getattr(args, "only_stages", None))
        if is_plan:
            _json(graph)
        else:
            _json({"valid": True, "config_hash": engine.config_digest,
                   "project": config.get("project"), "stage_count": len(graph["order"]),
                   "graph": graph})
    return 0


def _run(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else None
    if args.resume and args.run_id:
        raise WorkflowError("--run-id cannot be combined with --resume")
    if config is None and not args.resume:
        raise WorkflowError("run requires --config unless --resume is used")
    if args.resume and config is None:
        registry = _load_registry(args)
        stored_run = registry.get_run(args.resume)
        if stored_run is None:
            raise WorkflowError(f"unknown run_id {args.resume!r}")
        config = stored_run["config"]
    assert config is not None
    registry = _load_registry(args, config)
    engine = _engine(config, registry=registry)
    if isinstance(engine, BatchEngine):
        if any((args.from_stage, args.until_stage, args.only_stages,
                args.force_stages, args.partition_scopes)):
            raise WorkflowError('research batches use frozen targets/scope, not CLI stage overrides')
        result = engine.run_batch(run_id=args.run_id, resume=args.resume)
        _json(result)
        return 0 if result['status'] == 'SUCCEEDED' else 1
    summary = engine.run(
        config=config,
        run_id=args.run_id,
        resume=args.resume,
        from_stage=args.from_stage,
        until_stage=args.until_stage,
        only=args.only_stages,
        force_stages=args.force_stages,
        partitions=_partition_scopes(args.partition_scopes),
    )
    _json(summary.to_dict())
    return 0 if summary.status == "SUCCEEDED" else 1


def _inspect(args: argparse.Namespace) -> int:
    config = load_config(args.config) if args.config else None
    registry = _load_registry(args, config)
    if args.command == "status":
        run = registry.get_run(args.run_id)
        if run and 'research_plan' in run['graph']:
            report = batch_status(registry, args.run_id)
            report['query_filters'] = {'items': args.item_filters,
                                       'state': args.item_state, 'kind': args.item_kind}
            requested = set(args.item_filters or ())
            filtered = []
            for item in report['items']:
                if requested and not (requested & (set(item['aliases']) | {item['key']})):
                    continue
                if args.item_kind and item['kind'] != args.item_kind:
                    continue
                if args.item_state:
                    item['partitions'] = [p for p in item['partitions']
                                          if p['state'] == args.item_state]
                    if not item['partitions']:
                        continue
                filtered.append(item)
            report['items'] = filtered
            # Summary/counts remain batch-wide, with filtered rows clearly labeled.
            _json(report)
            return 0
        if args.item_filters or args.item_state or args.item_kind:
            raise WorkflowError('item filters require a research batch run')
        _json(registry.get_stage_statuses(args.run_id))
        return 0
    if args.command == "artifacts":
        stages = registry.get_run_stages(args.run_id)
        output = []
        for stage in stages:
            if args.stage and stage["stage_name"] != args.stage:
                continue
            for alias, ref in sorted(stage["artifacts"].items()):
                output.append({"stage": stage["stage_name"], "stage_run_id": stage["stage_run_id"],
                               "status": stage["status"], "partition": stage["partition"],
                               "alias": alias, "artifact": ref})
        _json(output)
        return 0
    matches = registry.explain_artifact(args.artifact_id)
    results = []
    for match in matches:
        ref = match["artifact"]
        manifest_path = ref.get("manifest_uri")
        if manifest_path and manifest_path.startswith("file://"):
            try:
                manifest = json.loads(artifact_path(manifest_path).read_text(encoding="utf-8"))
                match["manifest"] = manifest
            except (OSError, json.JSONDecodeError):
                match["manifest_error"] = "manifest is missing or invalid JSON"
        results.append(match)
    _json(results)
    return 0 if results else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command in {"validate", "plan"}:
            return _validate_or_plan(args, args.command == "plan")
        if args.command == "run":
            return _run(args)
        return _inspect(args)
    except (WorkflowError, ValueError, OSError, ImportError) as exc:
        print(f"quantsage: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
