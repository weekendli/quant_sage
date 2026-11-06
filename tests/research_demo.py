"""Run an isolated arithmetic A/B/C batch demonstration from the repository root."""

from __future__ import annotations

import argparse
import contextlib
import copy
import io
import json
import tempfile
from pathlib import Path

import yaml

from quantsage_discovery.cli import main
from tests.research_provider import make_environment


def demo():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.output or Path(tempfile.mkdtemp(prefix="quantsage-research-demo-"))
    if root.exists() and any(root.iterdir()):
        parser.error("--output must be an empty directory (prior runs are immutable)")
    root.mkdir(parents=True, exist_ok=True)
    config, _, _ = make_environment(root)
    reports = []
    for label in ("A", "B", "C"):
        current = copy.deepcopy(config)
        batch = current["research_batch"]
        batch["id"] = label
        if label in ("B", "C"):
            batch["data_scope"]["output_interval"]["end"] = "2026-01-07T00:00:00+00:00"
            batch["parent_batch_ref"] = "A" if label == "B" else "B"
        if label == "C":
            batch["targets"]["business_features"].append("long")
        config_path = root / f"{label}.yaml"
        config_path.write_text(yaml.safe_dump(current))
        for command in ("plan", "run"):
            output = io.StringIO()
            argv = [command, "--config", str(config_path)]
            if command == "run":
                argv += ["--run-id", label]
            with contextlib.redirect_stdout(output):
                code = main(argv)
            if code:
                raise RuntimeError(f"{label} {command} failed, exit {code}")
            path = root / f"{label}.{command}.json"
            path.write_text(output.getvalue())
            if command == "run":
                report = json.loads(output.getvalue())
                reports.append(
                    {
                        "batch": label,
                        "summary": report["summary"],
                        "states": report["state_counts"],
                        "added_definitions": len(report["delta"]["added_definitions"]),
                        "added_partitions": len(report["delta"]["added_partitions"]),
                    }
                )
    print(
        json.dumps(
            {"kind": "synthetic_arithmetic_only", "output": str(root), "batches": reports}, indent=2
        )
    )


if __name__ == "__main__":
    demo()
