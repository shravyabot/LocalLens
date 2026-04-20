from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from locallens.service import LocalLensService


SMOKE_QUERIES = [
    "What should I do in San Francisco if I want a practical first itinerary?",
    "Best taco place in San Jose over 4.5?",
    "Can I rely on public transit in Seattle?",
    "What local norms should I know before visiting New Orleans?",
]


def main() -> None:
    service = LocalLensService()
    for query in SMOKE_QUERIES:
        response = service.answer(query)
        print("=" * 80)
        print(query)
        print(response.answer)
        print(response.why_this_recommendation)
        print(response.key_tips[:3])


if __name__ == "__main__":
    main()

