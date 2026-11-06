"""Reproducible Python entry point for registering a source and creating a Study.

Usage:
    python -m examples.research_management --workspace /tmp/qs.duckdb \
        --source source.json --study study.json

The JSON documents follow the 01-B schemas and reference local serialized fixture artifacts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from quantsage_discovery.core.registry import Registry
from quantsage_discovery.research import ResearchService


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True, type=Path, help="DuckDB workspace file")
    parser.add_argument("--source", required=True, type=Path, help="DataSource JSON document")
    parser.add_argument("--study", required=True, type=Path, help="Study JSON document")
    args = parser.parse_args()
    source_document = json.loads(args.source.read_text(encoding="utf-8"))
    study_document = json.loads(args.study.read_text(encoding="utf-8"))
    with Registry(args.workspace) as registry:
        service = ResearchService(registry)
        source = service.register_source(source_document)
        study = service.create_study(study_document)
    print(json.dumps({"source": source, "study": study}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
