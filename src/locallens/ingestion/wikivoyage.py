from __future__ import annotations

from bs4 import BeautifulSoup, Tag
import requests

from locallens.schemas import CityRecord, SourceDocument
from locallens.taxonomy import WIKIVOYAGE_TOPIC_MAP
from locallens.utils import now_iso_date, slugify


WIKIVOYAGE_API = "https://en.wikivoyage.org/w/api.php"
HEADERS = {
    "User-Agent": "LocalLens/0.1 (+course project; local travel RAG ingestion)",
    "Accept": "application/json",
}


def fetch_city_documents(city: CityRecord) -> tuple[list[SourceDocument], dict[str, str]]:
    response = requests.get(
        WIKIVOYAGE_API,
        params={
            "action": "parse",
            "page": city.wikivoyage_page,
            "prop": "text",
            "format": "json",
            "formatversion": 2,
            "redirects": 1,
        },
        headers=HEADERS,
        timeout=25,
    )
    response.raise_for_status()
    payload = response.json()
    html = payload.get("parse", {}).get("text", "")
    source_url = f"https://en.wikivoyage.org/wiki/{city.wikivoyage_page.replace(' ', '_')}"
    return _html_to_documents(city, html, source_url), {"source_url": source_url}


def _html_to_documents(
    city: CityRecord,
    html: str,
    source_url: str,
) -> list[SourceDocument]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_="mw-parser-output") or soup

    documents: list[SourceDocument] = []
    current_heading = "Overview"
    current_topic = "orientation"
    buffer: list[str] = []

    def flush() -> None:
        nonlocal buffer
        text = "\n\n".join(part for part in buffer if part.strip()).strip()
        if len(text.split()) < 35:
            buffer = []
            return
        documents.append(
            SourceDocument(
                doc_id=f"{city.slug}-wikivoyage-{len(documents) + 1:03d}-{slugify(current_heading)}",
                title=f"{city.name} - {current_heading}",
                source_url=source_url,
                location=city.name,
                topic=current_topic,
                source_type="guide",
                timestamp=now_iso_date(),
                text=text,
                metadata={
                    "provider": "wikivoyage",
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
            headline = element.get_text(" ", strip=True).replace("[edit]", "").strip()
            if not headline:
                continue
            flush()
            current_heading = headline
            current_topic = WIKIVOYAGE_TOPIC_MAP.get(headline.lower(), "orientation")
            continue
        if element.name in {"p", "ul", "ol"}:
            text = element.get_text(" ", strip=True).strip()
            if len(text) >= 35:
                buffer.append(text)

    flush()

    deduped: list[SourceDocument] = []
    seen_pairs: set[tuple[str, str]] = set()
    for document in documents:
        key = (document.title, document.text[:160])
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        deduped.append(document)
    return deduped
