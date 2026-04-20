from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from locallens.service import LocalLensService


QUERIES = [
    "Good sunset spots in San Francisco",
    "What hidden gems or locals-only spots should I know in San Francisco?",
    "What should a newcomer know before moving to Seattle?",
    "Which neighborhoods in Seattle feel quiet but still interesting?",
    "Best taco place in San Jose over 4.5",
    "Can I rely on public transit in Chicago?",
    "What local norms should I know before visiting New Orleans?",
    "Where do locals actually go for nightlife in Austin?",
    "horse riding activities under 2 hours from san jose",
    "What are some family-friendly outdoor things to do in San Diego?",
]


def main() -> None:
    service = LocalLensService()
    for index, query in enumerate(QUERIES, start=1):
        answer = service.answer(query)
        citation_types = [citation["source_type"] for citation in answer.citations[:5]]
        print(f"[{index}] {query}")
        print(f"  answer: {answer.answer}")
        print(f"  why: {answer.why_this_recommendation}")
        print(f"  source summary: {answer.source_summary}")
        print(f"  used local llm: {answer.used_local_llm}")
        print(f"  citations: {len(answer.citations)} | top source types: {citation_types}")
        print(f"  place cards: {len(answer.place_cards)}")
        print()


if __name__ == "__main__":
    main()
