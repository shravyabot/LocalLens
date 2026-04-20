from __future__ import annotations

from urllib.parse import quote

from bs4 import BeautifulSoup, Tag
import requests

from locallens.schemas import CityRecord, SourceDocument
from locallens.utils import now_iso_date, slugify


SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"
PARSE_API = "https://en.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "LocalLens/0.1 (+course project; local travel RAG ingestion)",
    "Accept": "application/json",
}
SECTION_KEYWORDS = {
    "climate": "timing",
    "weather": "timing",
    "transportation": "transit",
    "transport": "transit",
    "public transportation": "transit",
    "culture": "etiquette",
    "arts": "activities",
    "music": "activities",
    "tourism": "activities",
    "landmarks": "activities",
    "parks": "activities",
    "recreation": "activities",
    "cuisine": "food",
    "food": "food",
    "neighborhood": "lodging",
    "district": "lodging",
    "demographics": "orientation",
    "history": "orientation",
    "economy": "orientation",
}
MAX_SECTION_DOCS = 10


def fetch_city_background(city: CityRecord) -> tuple[list[SourceDocument], dict[str, str]]:
    documents: list[SourceDocument] = []

    summary_url = SUMMARY_API + quote(city.wikipedia_page)
    summary_response = requests.get(summary_url, timeout=20, headers=HEADERS)
    summary_response.raise_for_status()
    payload = summary_response.json()
    text = str(payload.get("extract", "")).strip()
    page_url = str(payload.get("content_urls", {}).get("desktop", {}).get("page", ""))
    image_url = str(payload.get("thumbnail", {}).get("source", ""))

    if text:
        documents.append(
            SourceDocument(
                doc_id=f"{city.slug}-wikipedia-summary",
                title=str(payload.get("title", city.name)),
                source_url=page_url,
                location=city.name,
                topic="orientation",
                source_type="background",
                timestamp=now_iso_date(),
                text=text,
                metadata={
                    "provider": "wikipedia",
                    "image_url": image_url,
                    "description": str(payload.get("description", "")),
                    "city_kind": city.kind,
                },
            )
        )

    try:
        parse_response = requests.get(
            PARSE_API,
            params={
                "action": "parse",
                "page": city.wikipedia_page,
                "prop": "text",
                "format": "json",
                "formatversion": 2,
                "redirects": 1,
            },
            headers=HEADERS,
            timeout=20,
        )
        parse_response.raise_for_status()
        parse_payload = parse_response.json()
        html = parse_payload.get("parse", {}).get("text", "")
        documents.extend(_section_documents(city, html, page_url))
    except Exception:
        pass

    return documents, {"image_url": image_url}


def _section_documents(city: CityRecord, html: str, page_url: str) -> list[SourceDocument]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_="mw-parser-output") or soup

    documents: list[SourceDocument] = []
    current_heading = ""
    current_topic = ""
    buffer: list[str] = []

    def flush() -> None:
        nonlocal buffer
        if not current_heading or not current_topic:
            buffer = []
            return
        text = "\n\n".join(part for part in buffer if part.strip()).strip()
        if len(text.split()) < 40:
            buffer = []
            return
        documents.append(
            SourceDocument(
                doc_id=f"{city.slug}-wikipedia-{len(documents) + 1:03d}-{slugify(current_heading)}",
                title=f"{city.name} - {current_heading}",
                source_url=page_url,
                location=city.name,
                topic=current_topic,
                source_type="background",
                timestamp=now_iso_date(),
                text=text,
                metadata={
                    "provider": "wikipedia",
                    "section_title": current_heading,
                    "city_kind": city.kind,
                },
            )
        )
        buffer = []

    for element in content.descendants:
        if not isinstance(element, Tag):
            continue
        if element.name in {"h2", "h3"}:
            heading = element.get_text(" ", strip=True).replace("[edit]", "").strip()
            if not heading:
                continue
            flush()
            topic = _topic_for_heading(heading)
            if not topic:
                current_heading = ""
                current_topic = ""
                continue
            current_heading = heading
            current_topic = topic
            continue
        if element.name in {"p", "ul", "ol"} and current_topic:
            text = element.get_text(" ", strip=True).strip()
            if len(text) >= 40:
                buffer.append(text)

    flush()
    return documents[:MAX_SECTION_DOCS]


def _topic_for_heading(heading: str) -> str:
    heading_lower = heading.lower()
    for keyword, topic in SECTION_KEYWORDS.items():
        if keyword in heading_lower:
            return topic
    return ""
