from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from locallens.config import get_settings
from locallens.ingestion import build_corpus


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the LocalLens source corpus and SQLite database.")
    parser.add_argument("--locations", nargs="*", default=None, help="Optional subset of city names to ingest.")
    parser.add_argument("--skip-reddit", action="store_true", help="Skip Reddit ingestion.")
    parser.add_argument("--skip-places", action="store_true", help="Skip OSM place ingestion.")
    parser.add_argument("--skip-local-web", action="store_true", help="Skip curated local-web ingestion.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(ROOT)
    counts = build_corpus(
        settings,
        selected_locations=args.locations,
        include_reddit=not args.skip_reddit,
        include_places=not args.skip_places,
        include_local_web=not args.skip_local_web,
    )
    print(
        f"Built LocalLens corpus with {counts['documents']} documents, "
        f"{counts['chunks']} chunks, {counts['places']} place records across "
        f"{counts['locations']} locations."
    )


if __name__ == "__main__":
    main()
