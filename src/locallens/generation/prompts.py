from __future__ import annotations

from locallens.schemas import PlaceCandidate, SearchResult


def build_grounded_prompt(
    query: str,
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate] | None = None,
) -> str:
    place_candidates = place_candidates or []
    blocks: list[str] = []
    for index, result in enumerate(results[:4], start=1):
        chunk = result.chunk
        blocks.append(
            "\n".join(
                [
                    f"[Source {index}] {chunk.title}",
                    f"Location: {chunk.location}",
                    f"Topic: {chunk.topic}",
                    f"Source type: {chunk.source_type}",
                    f"URL: {chunk.source_url}",
                    f"Passage: {_normalize_evidence_text(_truncate_text(chunk.passage_text))}",
                ]
            )
        )

    for index, candidate in enumerate(place_candidates[:3], start=1):
        place = candidate.place
        blocks.append(
            "\n".join(
                [
                    f"[Place {index}] {place.name}",
                    f"Category: {place.category}",
                    f"Location: {place.location}",
                    f"Provider: {place.source_provider}",
                    f"Rating: {place.rating}",
                    f"Address: {place.address}",
                    f"Source URL: {place.source_url}",
                    f"Summary: {_truncate_text(place.description, limit_words=50)}",
                    f"Review snippets: {_truncate_text(' | '.join(place.review_snippets[:2]), limit_words=50)}",
                ]
            )
        )

    context = "\n\n".join(blocks)
    return f"""
You are LocalLens, a local-first travel and relocation assistant.
Answer only from the provided evidence.
Give the user a short narrative answer, not a pile of bullet fragments.
Write in fresh prose. Do not copy evidence labels such as "Thread title" or "Original post" into the answer.
If the evidence is centered on one city or park, stay inside that location and do not mention any other place.
Under "why_this_recommendation", explain the concrete evidence that drove the answer:
neighborhood fit, rating threshold, transit reality, timing, tradeoffs, or review signals.
Name the strongest places or sources directly instead of giving generic wording.
If a place recommendation is present, mention why it stands out compared with the next-best options.
If the evidence is thin, say so directly.
Return strict JSON with keys:
answer, why_this_recommendation, key_tips, confidence_note.
`key_tips` must be a JSON array of short strings.

User question: {query}

Evidence:
{context}
""".strip()


def _truncate_text(text: str, *, limit_words: int = 90) -> str:
    words = text.split()
    if len(words) <= limit_words:
        return text
    return " ".join(words[:limit_words]) + " ..."


def _normalize_evidence_text(text: str) -> str:
    return (
        text.replace("Thread title:", "Discussion topic:")
        .replace("Original post:", "Reported experience:")
        .replace("Top comments:", "Community observations:")
    )
