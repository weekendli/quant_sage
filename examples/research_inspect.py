"""Inspect registered research results and coverage through ResearchService.

    python -m examples.research_inspect --database /tmp/demo/research.duckdb \
        --study planning_study --run run_<id> --limit 10

Use --scope with a scope JSON document to narrow coverage and row queries.
--review-json explicitly appends the supplied Review after reference validation.
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
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--study", required=True)
    parser.add_argument("--run")
    parser.add_argument("--compare-run")
    parser.add_argument("--result", action="append", default=[])
    parser.add_argument("--scope", type=Path)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--fast", action="store_true",
                        help="Reuse metadata health checks; row reads always verify")
    parser.add_argument("--review-json", type=Path)
    args = parser.parse_args(argv)
    if args.compare_run is not None and args.run is None:
        parser.error("--compare-run requires --run")
    scope = json.loads(args.scope.read_text(encoding="utf-8")) if args.scope else None
    with Registry(args.database) as registry:
        service = ResearchService(registry)
        report = {
            "schema_version": "research_inspection_v1",
            "coverage": service.study_coverage(args.study, scope=scope, verify=not args.fast),
            "results": service.list_results(
                {"study_id": args.study, **({"run_id": args.run} if args.run else {})},
                limit=args.limit, verify=not args.fast,
            ),
            "reviews": service.list_reviews(args.study, run_id=args.run, limit=args.limit),
        }
        if args.run:
            report["run"] = service.get_run(args.run)
        if args.compare_run:
            report["comparison"] = service.compare_runs(
                args.run, args.compare_run, scope=scope, verify=not args.fast,
            )
        if args.result:
            report["values"] = service.read_results(args.result, scope=scope, limit=args.limit)
        if args.review_json:
            review = json.loads(args.review_json.read_text(encoding="utf-8"))
            if review.get("study_id") != args.study:
                parser.error("Review study_id must match --study")
            report["added_review"] = service.add_review(review)
        print(canonical_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
