"""Reproducible preflight plus Step 02 workflow runner."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from quantsage_discovery.config import load_stage_plugins
from quantsage_discovery.core.artifacts import ArtifactStore
from quantsage_discovery.core.registry import Registry
from quantsage_discovery.core.workflow import WorkflowEngine
from quantsage_discovery.ingest.readers import prepare_onboarding_config


def run_onboarding(
    template: str | Path,
    *,
    run_id: str | None = None,
    until_stage: str = "onboard",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Freeze source/split inputs and run through the requested onboarding stage."""
    config = prepare_onboarding_config(template)
    resolved_path = Path(config["bootstrap"]["resolved_config_path"])
    stages = load_stage_plugins(config)
    artifact_store = ArtifactStore(config["storage"]["artifacts_uri"])
    registry_path = Path(config["storage"].get("registry_uri", artifact_store.root / "registry.sqlite"))
    if not registry_path.is_absolute():
        registry_path = (Path.cwd() / registry_path).resolve()
    registry = Registry(registry_path)
    engine = WorkflowEngine(stages=stages, config=config, artifact_store=artifact_store, registry=registry)
    summary = engine.run(until_stage=until_stage, run_id=run_id)
    result = summary.to_dict()
    result["resolved_config"] = str(resolved_path)
    onboard = result.get("stage_results", {}).get("onboard", [])
    for stage_result in onboard:
        if stage_result.get("status") == "SUCCEEDED":
            entry_ref = stage_result.get("artifacts", {}).get("onboarding")
            if entry_ref:
                result["onboarding_entry_uri"] = entry_ref.get("uri")
    return config, result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="onboarding template YAML")
    parser.add_argument("--run-id", help="optional reproducible workflow run ID")
    parser.add_argument("--until", default="onboard", help="last workflow stage; default onboard")
    parser.add_argument("--preflight-only", action="store_true", help="freeze inputs without running stages")
    args = parser.parse_args(argv)
    try:
        config = prepare_onboarding_config(args.config)
        resolved_path = Path(config["bootstrap"]["resolved_config_path"])
        if args.preflight_only:
            print(json.dumps({"preflight": "SUCCEEDED", "resolved_config": str(resolved_path),
                              "data_snapshot_id": config["data_snapshot_id"],
                              "split_id": config["splits"]["split_id"]}, indent=2))
            return 0
        _, result = run_onboarding(args.config, run_id=args.run_id, until_stage=args.until)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "SUCCEEDED" else 1
    except (OSError, ValueError, KeyError, ImportError) as exc:
        print(f"onboarding: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
