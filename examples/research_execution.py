"""Execute a frozen research request in an already registered workspace.

    python -m examples.research_execution --workspace /tmp/research-demo \
        --request /tmp/request.json --request-key demo-1
    python -m examples.research_execution --workspace /tmp/research-demo \
        --resume run_<id>

Register the source, trusted catalog, frozen FeatureSet and Study through
ResearchService first. This increment supports the synthetic arithmetic and
identity providers; market-specific providers follow in the domain increments.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quantsage_discovery.core import Registry
from quantsage_discovery.core.utils import canonical_json
from quantsage_discovery.research import ResearchService


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--request", type=Path)
    action.add_argument("--resume", metavar="RUN_ID")
    parser.add_argument("--request-key")
    args = parser.parse_args(argv)
    if args.request is not None and not args.request_key:
        parser.error("--request requires --request-key")
    if args.resume is not None and args.request_key is not None:
        parser.error("--request-key applies only to a new request")
    with Registry(args.workspace / "research.duckdb") as registry:
        service = ResearchService(registry)
        if args.resume is not None:
            run = service.resume_run(args.resume)
        else:
            request = json.loads(args.request.read_text(encoding="utf-8"))
            plan = service.plan_run(request)
            submitted = service.submit_run(
                request, request_key=args.request_key, expected_plan_hash=plan["plan_hash"],
            )
            run = service.execute_run(submitted["run_id"])
        print(canonical_json(run))
    return 0 if run["summary"]["status"] in {"COMPLETE", "NO_WORK"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
