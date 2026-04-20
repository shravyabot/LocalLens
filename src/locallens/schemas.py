from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class CityRecord:
    name: str
    slug: str
    region: str
    latitude: float
    longitude: float
    radius_km: float
    kind: str = "city"
    aliases: list[str] = field(default_factory=list)
    wikivoyage_page: str = ""
    wikipedia_page: str = ""
    official_url: str = ""
    reddit_subreddits: list[str] = field(default_factory=list)
    curated_urls: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SourceDocument:
    doc_id: str
    title: str
    source_url: str
    location: str
    topic: str
    source_type: str
    timestamp: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ChunkRecord:
    chunk_id: str
    doc_id: str
    title: str
    source_url: str
    location: str
    topic: str
    source_type: str
    timestamp: str
    passage_text: str
    passage_index: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PlaceRecord:
    place_id: str
    location: str
    name: str
    category: str
    source_provider: str
    source_url: str
    latitude: float
    longitude: float
    address: str = ""
    neighborhood: str = ""
    rating: float | None = None
    review_count: int | None = None
    price_level: str = ""
    cuisine: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    description: str = ""
    image_url: str = ""
    review_snippets: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SearchResult:
    chunk: ChunkRecord
    bm25_score: float = 0.0
    dense_score: float = 0.0
    rrf_score: float = 0.0
    rerank_score: float = 0.0
    final_score: float = 0.0

    def to_citation(self) -> dict[str, Any]:
        evidence_dom_id = f"evidence-{self.chunk.chunk_id}"
        return {
            "chunk_id": self.chunk.chunk_id,
            "doc_id": self.chunk.doc_id,
            "title": self.chunk.title,
            "evidence_dom_id": evidence_dom_id,
            "evidence_anchor": f"#{evidence_dom_id}",
            "source_url": self.chunk.source_url,
            "location": self.chunk.location,
            "topic": self.chunk.topic,
            "source_type": self.chunk.source_type,
            "timestamp": self.chunk.timestamp,
            "passage_text": self.chunk.passage_text,
            "passage_index": self.chunk.passage_index,
            "score": round(self.final_score, 4),
        }


@dataclass(slots=True)
class PlaceCandidate:
    place: PlaceRecord
    score: float
    why: str

    def to_card(self) -> dict[str, Any]:
        return {
            "place_id": self.place.place_id,
            "name": self.place.name,
            "location": self.place.location,
            "category": self.place.category,
            "rating": self.place.rating,
            "review_count": self.place.review_count,
            "price_level": self.place.price_level,
            "address": self.place.address,
            "image_url": self.place.image_url,
            "source_url": self.place.source_url,
            "source_provider": self.place.source_provider,
            "review_snippets": self.place.review_snippets,
            "why": self.why,
            "score": round(self.score, 4),
        }


@dataclass(slots=True)
class QueryIntent:
    query: str
    location: str = ""
    topic: str = ""
    category: str = ""
    cuisine: str = ""
    rating_min: float | None = None
    wants_places: bool = False
    route: str = "hybrid"
    activity_types: list[str] = field(default_factory=list)
    audience: str = ""
    vibe: str = ""
    time_of_day: str = ""
    distance_hours: float | None = None
    distance_km: float | None = None
    wants_local_knowledge: bool = False
    wants_hidden_gems: bool = False
    wants_local_opinion: bool = False
    wants_newcomer_advice: bool = False
    wants_distance_expansion: bool = False


@dataclass(slots=True)
class AnswerPayload:
    answer: str
    why_this_recommendation: str
    key_tips: list[str]
    confidence_note: str
    citations: list[dict[str, Any]]
    filters_applied: dict[str, str]
    used_local_llm: bool
    source_summary: str
    place_cards: list[dict[str, Any]] = field(default_factory=list)
    gallery_images: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
