from __future__ import annotations

import re

from locallens.cities import CITY_CATALOG
from locallens.generation.ollama import OllamaClient
from locallens.generation.prompts import build_grounded_prompt
from locallens.schemas import AnswerPayload, PlaceCandidate, SearchResult
from locallens.utils import unique_preserve_order


def compose_answer(
    query: str,
    results: list[SearchResult],
    filters_applied: dict[str, str],
    *,
    place_candidates: list[PlaceCandidate] | None = None,
    ollama_client: OllamaClient | None = None,
    gallery_images: list[dict[str, str]] | None = None,
) -> AnswerPayload:
    place_candidates = place_candidates or []
    gallery_images = gallery_images or []
    local_knowledge_query = _is_local_knowledge_query(query)
    answer_place_candidates = [] if (local_knowledge_query and results) else place_candidates
    if not results and not place_candidates:
        lower_query = query.lower()
        if any(token in lower_query for token in ["horse riding", "horseback", "equestrian", "stables", "stable", "ranch"]):
            return AnswerPayload(
                answer=(
                    "I could not find grounded horse-riding or equestrian evidence for that San Jose area query "
                    "in the current LocalLens corpus."
                ),
                why_this_recommendation=(
                    "The system searched the stored passages and place records, but none of the retrieved evidence "
                    "actually mentioned horse-riding venues, stables, or equestrian trails near the requested area."
                ),
                key_tips=[
                    "Try naming a specific stable, ranch, or nearby town.",
                    "If you have Google Places enabled, rerun the query for live equestrian listings.",
                    "Treat this as a corpus coverage gap rather than a recommendation."
                ],
                confidence_note="Low confidence because the current corpus does not contain grounded equestrian evidence for this query.",
                citations=[],
                filters_applied=filters_applied,
                used_local_llm=False,
                source_summary="No grounded equestrian sources retrieved.",
                place_cards=[],
                gallery_images=gallery_images,
            )
        return AnswerPayload(
            answer="I could not find enough grounded evidence for that question in the current LocalLens corpus.",
            why_this_recommendation="No relevant documents or place records were retrieved after filtering.",
            key_tips=[
                "Name the city or park directly.",
                "Ask about one constraint at a time, such as tacos, transit, safety, or lodging.",
            ],
            confidence_note="Low confidence because the system could not ground an answer in stored evidence.",
            citations=[],
            filters_applied=filters_applied,
            used_local_llm=False,
            source_summary="No sources retrieved.",
            place_cards=[],
            gallery_images=gallery_images,
        )

    if place_candidates and not (local_knowledge_query and results):
        return AnswerPayload(
            answer=_fallback_answer(query, results, place_candidates),
            why_this_recommendation=_fallback_why(results, place_candidates),
            key_tips=_fallback_tips(query, results, place_candidates),
            confidence_note=_confidence_note(results, place_candidates),
            citations=[result.to_citation() for result in results],
            filters_applied=filters_applied,
            used_local_llm=False,
            source_summary=_source_summary(results, place_candidates),
            place_cards=[candidate.to_card() for candidate in place_candidates],
            gallery_images=gallery_images,
        )

    if ollama_client and ollama_client.is_available():
        try:
            payload = ollama_client.generate_json(
                build_grounded_prompt(query, results, place_candidates=answer_place_candidates)
            )
            answer = _clean_text(payload.get("answer")) or _fallback_answer(
                query,
                results,
                answer_place_candidates,
            )
            why = _clean_text(payload.get("why_this_recommendation")) or _fallback_why(
                results,
                answer_place_candidates,
            )
            key_tips = _coerce_tips(payload.get("key_tips")) or _fallback_tips(
                query,
                results,
                answer_place_candidates,
            )
            confidence_note = _clean_text(payload.get("confidence_note")) or _confidence_note(
                results,
                answer_place_candidates,
            )
            if not _passes_grounding_checks(answer, why, key_tips, results, answer_place_candidates):
                raise ValueError("LLM response failed grounding checks.")
            return AnswerPayload(
                answer=answer,
                why_this_recommendation=why,
                key_tips=key_tips,
                confidence_note=confidence_note,
                citations=[result.to_citation() for result in results],
                filters_applied=filters_applied,
                used_local_llm=True,
                source_summary=_source_summary(results, answer_place_candidates),
                place_cards=[candidate.to_card() for candidate in place_candidates],
                gallery_images=gallery_images,
            )
        except Exception:
            pass

    return AnswerPayload(
        answer=_fallback_answer(query, results, answer_place_candidates),
        why_this_recommendation=_fallback_why(results, answer_place_candidates),
        key_tips=_fallback_tips(query, results, answer_place_candidates),
        confidence_note=_confidence_note(results, answer_place_candidates),
        citations=[result.to_citation() for result in results],
        filters_applied=filters_applied,
        used_local_llm=False,
        source_summary=_source_summary(results, answer_place_candidates),
        place_cards=[candidate.to_card() for candidate in place_candidates],
        gallery_images=gallery_images,
    )


def _fallback_answer(
    query: str,
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate],
) -> str:
    if place_candidates:
        top = place_candidates[:3]
        names = ", ".join(candidate.place.name for candidate in top)
        location = top[0].place.location
        return (
            f"For {location}, the strongest matches in this corpus are {names}. "
            f"They fit because the stored place data and supporting passages point to the right category, neighborhood, and practical tradeoffs for the question."
        )
    if _is_sunset_query(query):
        scenic_names = _extract_scenic_names(results)
        if scenic_names:
            joined = ", ".join(scenic_names[:4])
            location = results[0].chunk.location if results else "the area"
            return (
                f"For {location}, the grounded passages point most clearly to {joined} for a sunset-oriented outing. "
                f"Those names surfaced from the parks, waterfront, and viewpoint sections rather than generic activity listings."
            )
    if _is_local_knowledge_query(query):
        location = results[0].chunk.location if results else "the area"
        section_titles = unique_preserve_order(
            str(result.chunk.metadata.get("section_title", "")).strip()
            for result in results
            if str(result.chunk.metadata.get("section_title", "")).strip()
        )
        titles = unique_preserve_order(result.chunk.title for result in results if result.chunk.title)
        section_text = ", ".join(section_titles[:3]) if section_titles else "local discussion and guide passages"
        title_text = ", ".join(titles[:2]) if titles else "the retrieved evidence"
        return (
            f"For {location}, the strongest grounded local-knowledge evidence comes from {section_text}. "
            f"The answer leans on sources like {title_text}, which capture what locals highlight, not just generic visitor summaries."
        )
    best = results[0]
    sentences = _supporting_sentences(query, results, limit=3)
    if sentences:
        return " ".join(sentences)
    return (
        f"The best grounded evidence I found centers on {best.chunk.title} for {best.chunk.location}."
    )


def _fallback_why(
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate],
) -> str:
    if place_candidates:
        top = place_candidates[0].place
        runner_up = place_candidates[1].place.name if len(place_candidates) > 1 else ""
        factors: list[str] = []
        if top.rating is not None:
            review_count = top.review_count if top.review_count is not None else "an unknown number of"
            factors.append(f"{top.name} carries a rating signal of {top.rating:.1f} from {review_count} reviews")
        if top.neighborhood:
            factors.append(f"it is tagged in or near {top.neighborhood}")
        if top.address:
            factors.append(f"the stored address is {top.address}")
        if top.review_snippets:
            factors.append(f"the review evidence mentions details like '{top.review_snippets[0]}'")
        if top.category:
            factors.append(f"it matches the requested {top.category} category")
        reason_text = "; ".join(factors[:3]) or f"{top.name} best matches the structured place evidence"
        comparison = f" It ranked ahead of {runner_up} because more of its stored signals lined up with the query." if runner_up else ""
        return (
            f"The recommendation centers on {top.name}: {reason_text}.{comparison} "
            f"This section is driven by concrete metadata and retrieved evidence rather than a generic city summary."
        )
    source_types = unique_preserve_order(result.chunk.source_type for result in results)
    locations = unique_preserve_order(result.chunk.location for result in results)
    section_titles = unique_preserve_order(
        str(result.chunk.metadata.get("section_title", "")).strip()
        for result in results
        if str(result.chunk.metadata.get("section_title", "")).strip()
    )
    details: list[str] = []
    if section_titles:
        details.append(f"the strongest passages came from sections like {', '.join(section_titles[:3])}")
    if source_types:
        details.append(f"the evidence spans {', '.join(source_types[:3])}")
    if locations:
        details.append(f"the ranking stayed focused on {', '.join(locations[:2])}")
    if results:
        details.append(f"the top citation was {results[0].chunk.title}")
    joined = "; ".join(details) or "the retrieved passages repeatedly pointed to the same tradeoffs"
    return (
        f"This recommendation comes from the highest-ranked grounded passages rather than a generic summary: {joined}. "
        f"The system emphasized passages whose wording overlapped the question and whose location and topic metadata matched the requested context."
    )


def _fallback_tips(
    query: str,
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate],
) -> list[str]:
    if place_candidates:
        tips: list[str] = []
        for candidate in place_candidates[:3]:
            place = candidate.place
            if place.rating is not None:
                tips.append(
                    f"{place.name}: rating {place.rating:.1f} from {place.review_count or 'unknown'} reviews."
                )
            elif place.address:
                tips.append(f"{place.name}: {place.address}.")
            if len(tips) == 3:
                break
        if not tips:
            tips.append("Inspect the place cards and citations before committing to one option.")
        return tips

    if _is_sunset_query(query):
        scenic_names = _extract_scenic_names(results)
        if scenic_names:
            return [f"Check {name} in the cited passages." for name in scenic_names[:4]]
    if _is_local_knowledge_query(query):
        tips: list[str] = []
        for result in results[:4]:
            section = str(result.chunk.metadata.get("section_title", "")).strip()
            if section:
                tip = f"Use the {section} evidence in {result.chunk.title} as the local context."
            else:
                tip = f"Inspect the citation from {result.chunk.title} for the local detail."
            if tip not in tips:
                tips.append(tip)
        return tips or ["Inspect the citations for the local-detail context behind this answer."]

    tips: list[str] = []
    for sentence in _supporting_sentences(query, results, limit=6):
        cleaned = sentence.strip()
        if cleaned and cleaned not in tips:
            tips.append(cleaned)
        if len(tips) == 4:
            break
    if not tips:
        tips = ["Compare official guidance with local discussion before deciding."]
    return tips


def _confidence_note(
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate],
) -> str:
    if place_candidates and any(candidate.place.rating is not None for candidate in place_candidates):
        return "Moderate confidence: the shortlist is grounded in structured place data and supporting passages, but availability and ratings can change."
    unique_docs = len({result.chunk.doc_id for result in results})
    if unique_docs >= 4:
        return "Moderate confidence: multiple independent passages support the answer."
    return "Low-to-moderate confidence: inspect the citations because the answer is based on limited evidence."


def _source_summary(
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate],
) -> str:
    source_types = unique_preserve_order(result.chunk.source_type for result in results)
    summary = f"{len({result.chunk.doc_id for result in results})} unique documents"
    if source_types:
        summary += f" across {', '.join(source_types)}"
    if place_candidates:
        summary += f" and {len(place_candidates)} structured place matches"
    return summary + "."


def _coerce_tips(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _clean_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text or text.startswith("{") or text.startswith("["):
        return ""
    lowered = text.lower()
    if any(marker in lowered for marker in ["thread title:", "original post:", "discussion topic:", "reported experience:"]):
        return ""
    if len(text.split()) < 10:
        return ""
    return text


def _supporting_sentences(query: str, results: list[SearchResult], *, limit: int) -> list[str]:
    output: list[str] = []
    prioritized: list[str] = []
    secondary: list[str] = []
    lower_query = query.lower()
    sunset_query = any(token in lower_query for token in ["sunset", "golden hour"])
    sunset_markers = [
        "sunset",
        "ocean beach",
        "baker beach",
        "crissy field",
        "lands end",
        "twin peaks",
        "presidio",
        "beach",
        "view",
        "views",
        "overlook",
        "park",
    ]
    for result in results:
        for sentence in re.split(r"(?<=[.!?])\s+", result.chunk.passage_text):
            cleaned = sentence.strip()
            if len(cleaned) < 35:
                continue
            lowered = cleaned.lower()
            if (
                "thread title:" in lowered
                or "original post:" in lowered
                or "discussion topic:" in lowered
                or "reported experience:" in lowered
                or lowered.startswith("local discussion about")
            ):
                continue
            target = secondary
            if sunset_query and any(marker in lowered for marker in sunset_markers):
                target = prioritized
            if cleaned not in prioritized and cleaned not in secondary:
                target.append(cleaned)
    output.extend(prioritized)
    output.extend(secondary)
    return output[:limit]


def _is_sunset_query(query: str) -> bool:
    lower_query = query.lower()
    return any(token in lower_query for token in ["sunset", "golden hour"])


def _is_local_knowledge_query(query: str) -> bool:
    lower_query = query.lower()
    markers = [
        "hidden gem",
        "locals only",
        "locals recommend",
        "what locals do",
        "unwritten rule",
        "custom",
        "etiquette",
        "before moving",
        "newcomer",
        "neighborhood",
        "quiet but",
    ]
    return any(marker in lower_query for marker in markers)


def _extract_scenic_names(results: list[SearchResult]) -> list[str]:
    scenic_suffixes = (
        "Beach",
        "Field",
        "End",
        "Peaks",
        "Peak",
        "Park",
        "Heights",
        "Point",
        "Presidio",
        "Overlook",
        "Ridge",
        "Hill",
        "Hills",
        "Marina",
    )
    names: list[str] = []
    seen: set[str] = set()
    pattern = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b")
    for result in results:
        for match in pattern.findall(result.chunk.passage_text):
            cleaned = match.strip()
            if not cleaned.endswith(scenic_suffixes):
                continue
            if cleaned in {"National Park Service", "Golden Gate National Recreation Area", "San Francisco Recreation"}:
                continue
            if cleaned not in seen:
                seen.add(cleaned)
                names.append(cleaned)
    return names


def _passes_grounding_checks(
    answer: str,
    why: str,
    key_tips: list[str],
    results: list[SearchResult],
    place_candidates: list[PlaceCandidate],
) -> bool:
    combined = " ".join([answer, why, *key_tips]).strip().lower()
    if not combined:
        return False
    banned_markers = ["thread title:", "original post:", "discussion topic:", "reported experience:"]
    if any(marker in combined for marker in banned_markers):
        return False

    allowed_locations = unique_preserve_order(
        [candidate.place.location for candidate in place_candidates] + [result.chunk.location for result in results]
    )
    mentioned_locations = {
        city.name
        for city in CITY_CATALOG
        if city.name.lower() in combined or any(alias.lower() in combined for alias in city.aliases)
    }
    if mentioned_locations and any(location not in allowed_locations for location in mentioned_locations):
        return False

    if place_candidates:
        top_names = [candidate.place.name.lower() for candidate in place_candidates[:3]]
        if not any(name in combined for name in top_names):
            return False

    return True
