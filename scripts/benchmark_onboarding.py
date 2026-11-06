"""Run Step 02 and save its wall-clock and peak-RSS benchmark record."""

from __future__ import annotations

import argparse
import json
import resource
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantsage_discovery.onboarding import run_onboarding


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="onboarding configuration template")
    parser.add_argument("--run-id", help="optional workflow run ID")
    parser.add_argument("--report", required=True, help="JSON benchmark output path")
    args = parser.parse_args(argv)

    started_at = datetime.now(UTC)
    start = time.perf_counter()
    config, result = run_onboarding(args.config, run_id=args.run_id)
    elapsed = time.perf_counter() - start
    usage = resource.getrusage(resource.RUSAGE_SELF)
    report: dict[str, Any] = {
        "schema_version": "1.0.0",
        "started_at": started_at.isoformat(),
        "elapsed_seconds": elapsed,
        "peak_rss_kib": usage.ru_maxrss,
        "configured_memory_limit": config.get("resources", {}).get("memory_limit"),
        "configured_threads": config.get("resources", {}).get("threads"),
        "data_snapshot_id": config.get("data_snapshot_id"),
        "status": result.get("status"),
        "onboarding_entry_uri": result.get("onboarding_entry_uri"),
        "run_summary": result,
    }
    target = Path(args.report).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "run_summary"},
                     indent=2, sort_keys=True))
    return 0 if result.get("status") == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
