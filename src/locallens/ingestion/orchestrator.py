from __future__ import annotations

import time
from pathlib import Path

from locallens.chunking import chunk_documents
from locallens.cities import CITY_CATALOG
from locallens.config import Settings
from locallens.ingestion.local_web import (
    fetch_city_local_knowledge,
    synthesize_local_knowledge_fallback,
)
from locallens.ingestion.overpass import fetch_city_places
from locallens.ingestion.reddit import fetch_city_threads
from locallens.ingestion.wikipedia import fetch_city_background
from locallens.ingestion.wikivoyage import fetch_city_documents
from locallens.schemas import ChunkRecord, PlaceRecord, SourceDocument
from locallens.storage import connect, replace_chunks, replace_documents, replace_places
from locallens.utils import ensure_parent, write_json


def build_corpus(
    settings: Settings,
    *,
    selected_locations: list[str] | None = None,
    include_reddit: bool = True,
    include_places: bool = True,
    include_local_web: bool = True,
) -> dict[str, int]:
    conn = connect(settings.database_path)
    documents: list[SourceDocument] = []
    places: list[PlaceRecord] = []
    gallery_images: dict[str, str] = {}

    requested = set(selected_locations or [])
    cities = [
        city for city in CITY_CATALOG if not requested or city.name in requested
    ]

    for index, city in enumerate(cities, start=1):
        print(f"[{index}/{len(cities)}] Ingesting {city.name}...")
        city_started = time.time()
        guide_docs: list[SourceDocument] = []
        docs_bg: list[SourceDocument] = []
        reddit_docs: list[SourceDocument] = []
        guide_meta: dict[str, str] = {}
        bg_meta: dict[str, str] = {}
        city_places: list[PlaceRecord] = []

        try:
            guide_docs, guide_meta = fetch_city_documents(city)
            documents.extend(guide_docs)
            print(f"  guide docs: {len(guide_docs)}")
        except Exception as error:
            print(f"  guide docs failed: {error}")

        try:
            docs_bg, bg_meta = fetch_city_background(city)
            documents.extend(docs_bg)
            print(f"  background docs: {len(docs_bg)}")
        except Exception as error:
            print(f"  background docs failed: {error}")
            docs_bg = []

        if include_reddit:
            try:
                reddit_docs = fetch_city_threads(city, user_agent=settings.reddit_user_agent)
                documents.extend(reddit_docs)
                print(f"  reddit docs: {len(reddit_docs)}")
            except Exception as error:
                print(f"  reddit docs failed: {error}")
                reddit_docs = []

        if include_local_web:
            try:
                local_docs = fetch_city_local_knowledge(city)
                if not local_docs:
                    local_docs = synthesize_local_knowledge_fallback(
                        city,
                        guide_docs=guide_docs,
                        background_docs=docs_bg,
                        forum_docs=reddit_docs,
                    )
                documents.extend(local_docs)
                print(f"  local web docs: {len(local_docs)}")
            except Exception as error:
                print(f"  local web docs failed: {error}")

        if include_places:
            try:
                city_places = fetch_city_places(city)
                places.extend(city_places)
                place_docs = _place_documents(city_places)
                documents.extend(place_docs)
                print(f"  places: {len(city_places)} | place docs: {len(place_docs)}")
            except Exception as error:
                print(f"  places failed: {error}")

        image_url = bg_meta.get("image_url", "") or guide_meta.get("image_url", "")
        if image_url:
            gallery_images[city.name] = image_url
        print(
            f"  finished {city.name} in {time.time() - city_started:.1f}s | "
            f"running totals -> docs: {len(documents)}, places: {len(places)}"
        )

    documents = _normalize_document_ids(documents)
    chunks = chunk_documents(
        documents,
        max_words=settings.chunk_max_words,
        overlap_words=settings.chunk_overlap_words,
    )
    replace_documents(conn, documents)
    replace_chunks(conn, chunks)
    replace_places(conn, places)

    write_json(
        settings.processed_dir / "gallery_images.json",
        gallery_images,
    )
    return {
        "documents": len(documents),
        "chunks": len(chunks),
        "places": len(places),
        "locations": len(cities),
    }


def _normalize_document_ids(documents: list[SourceDocument]) -> list[SourceDocument]:
    normalized: list[SourceDocument] = []
    seen_ids: dict[str, int] = {}
    seen_signatures: set[tuple[str, str, str, str]] = set()
    for document in documents:
        signature = (
            document.location,
            document.source_type,
            document.title,
            document.source_url,
        )
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        count = seen_ids.get(document.doc_id, 0)
        seen_ids[document.doc_id] = count + 1
        if count == 0:
            normalized.append(document)
            continue

        metadata = dict(document.metadata)
        metadata["original_doc_id"] = document.doc_id
        normalized.append(
            SourceDocument(
                doc_id=f"{document.doc_id}-{count + 1}",
                title=document.title,
                source_url=document.source_url,
                location=document.location,
                topic=document.topic,
                source_type=document.source_type,
                timestamp=document.timestamp,
                text=document.text,
                metadata=metadata,
            )
        )
    return normalized


def _place_documents(places: list[PlaceRecord]) -> list[SourceDocument]:
    documents: list[SourceDocument] = []
    for place in places:
        topic = _topic_for_place(place)
        details: list[str] = [
            f"{place.name} is a {place.category} in {place.location}.",
        ]
        if place.address:
            details.append(f"Address: {place.address}.")
        if place.neighborhood:
            details.append(f"Neighborhood: {place.neighborhood}.")
        if place.description:
            details.append(place.description)
        if place.cuisine:
            details.append(f"Cuisine or food tags: {', '.join(place.cuisine[:4])}.")
        if place.tags:
            details.append(f"Provider tags: {', '.join(place.tags[:5])}.")
        if place.rating is not None:
            reviews = place.review_count if place.review_count is not None else "an unknown number of"
            details.append(f"Current rating signal: {place.rating:.1f} from {reviews} reviews.")
        if place.review_snippets:
            details.append("Recent review signals:")
            details.extend(f"- {snippet}" for snippet in place.review_snippets[:3])
        details.append(f"Source provider: {place.source_provider}.")
        documents.append(
            SourceDocument(
                doc_id=f"{place.place_id}-doc",
                title=f"{place.location} - {place.name}",
                source_url=place.source_url,
                location=place.location,
                topic=topic,
                source_type="place_record",
                timestamp=str(place.metadata.get("fetched_at", "")),
                text="\n\n".join(part for part in details if part).strip(),
                metadata={
                    "provider": place.source_provider,
                    "place_id": place.place_id,
                    "place_category": place.category,
                    "neighborhood": place.neighborhood,
                },
            )
        )
    return documents


def _topic_for_place(place: PlaceRecord) -> str:
    if place.category == "restaurant":
        return "food"
    if place.category == "hotel":
        return "lodging"
    if place.category == "transit":
        return "transit"
    return "activities"
