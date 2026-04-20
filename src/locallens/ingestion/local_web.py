from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from xml.etree import ElementTree
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
import requests

from locallens.schemas import CityRecord, SourceDocument
from locallens.utils import count_words, now_iso_date, slugify


HEADERS = {
    "User-Agent": "LocalLens/0.1 (+course project; curated local knowledge ingestion)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LOCAL_WEB_TOPIC_KEYWORDS = {
    "hidden_gems": {"hidden", "underrated", "secret", "locals", "favorite", "non-touristy"},
    "newcomer_advice": {"first-time", "first timer", "new", "moving", "newcomer", "before you go", "know before"},
    "neighborhood_vibe": {"neighborhood", "district", "area", "walkable", "where to stay", "guide"},
    "local_customs": {"custom", "etiquette", "respect", "tips", "know before", "visiting", "dos and don'ts"},
    "local_opinion": {"locals", "favorite", "worth it", "must-do", "recommend", "insider"},
    "scenic": {"sunset", "view", "viewpoint", "scenic", "rooftop", "waterfront"},
    "outdoors": {"park", "trail", "hiking", "beach", "outdoors", "nature"},
    "nightlife": {"nightlife", "bar", "cocktail", "club", "late night", "music"},
    "family": {"family", "kids", "children", "play", "all ages"},
    "activities": {"things to do", "itinerary", "attractions", "must-see", "weekend"},
}

DISCOVERY_KEYWORDS = sorted(
    {keyword for keywords in LOCAL_WEB_TOPIC_KEYWORDS.values() for keyword in keywords},
    key=len,
    reverse=True,
)

MAX_DISCOVERED_LINKS = 40
MAX_SECOND_HOP_LINKS = 24
MAX_DOCS_PER_CITY = 24
MAX_CRAWLED_DOCS_PER_CITY = 80
MAX_DOCS_PER_TOPIC = 6
MAX_DOCS_PER_DOMAIN = 10
MIN_DOC_WORDS = 80
REQUEST_TIMEOUT_SECONDS = 12
MAX_SITEMAP_URLS = 80
FALLBACK_MAX_SNIPPET_WORDS = 110
FALLBACK_TARGET_TOPICS = {
    "newcomer_advice",
    "neighborhood_vibe",
    "local_customs",
    "local_opinion",
    "scenic",
    "outdoors",
    "nightlife",
    "family",
    "activities",
}
FALLBACK_TOPIC_RULES = {
    "newcomer_advice": {
        "source_topics": {"orientation", "transit", "timing", "lodging", "etiquette"},
        "title_keywords": {
            "understand",
            "district",
            "stay safe",
            "get around",
            "transport",
            "know before",
            "moving",
            "first-time",
            "first time",
            "tips",
        },
        "body_keywords": {
            "know before",
            "first-time",
            "moving",
            "tips",
            "public transit",
            "walkable",
            "neighborhood",
            "safety",
        },
    },
    "neighborhood_vibe": {
        "source_topics": {"lodging", "orientation", "activities"},
        "title_keywords": {"district", "neighborhood", "area", "stay", "sleep", "quarter"},
        "body_keywords": {"neighborhood", "district", "walkable", "quiet", "busy", "local"},
    },
    "local_customs": {
        "source_topics": {"etiquette", "orientation", "food"},
        "title_keywords": {"custom", "culture", "etiquette", "respect", "know before", "tips"},
        "body_keywords": {"custom", "respect", "etiquette", "tip", "local", "visitor"},
    },
    "local_opinion": {
        "source_topics": {"activities", "food", "lodging", "orientation"},
        "title_keywords": {"favorite", "locals", "recommend", "worth", "best"},
        "body_keywords": {"locals", "favorite", "recommend", "worth it", "hidden gem"},
    },
    "scenic": {
        "source_topics": {"activities"},
        "title_keywords": {"sunset", "view", "scenic", "waterfront", "photo"},
        "body_keywords": {"sunset", "view", "scenic", "waterfront", "overlook"},
    },
    "outdoors": {
        "source_topics": {"activities"},
        "title_keywords": {"park", "trail", "beach", "outdoor", "nature", "hike"},
        "body_keywords": {"park", "trail", "beach", "outdoor", "nature", "hike"},
    },
    "nightlife": {
        "source_topics": {"activities", "food"},
        "title_keywords": {"nightlife", "bar", "cocktail", "club", "music", "late night"},
        "body_keywords": {"nightlife", "bar", "cocktail", "club", "music", "late night"},
    },
    "family": {
        "source_topics": {"activities"},
        "title_keywords": {"family", "kids", "children", "all ages", "play"},
        "body_keywords": {"family", "kids", "children", "all ages", "playground"},
    },
    "activities": {
        "source_topics": {"activities", "orientation"},
        "title_keywords": {"things to do", "weekend", "must-see", "attractions"},
        "body_keywords": {"things to do", "weekend", "must-see", "attractions"},
    },
}


@dataclass(slots=True)
class ScoredLocalDocument:
    document: SourceDocument
    quality_score: float
    topic: str
    domain: str


def fetch_city_local_knowledge(city: CityRecord) -> list[SourceDocument]:
    seed_urls = [url for url in [city.official_url, *city.curated_urls] if url]
    if not seed_urls:
        return []

    seed_hosts = {
        urlparse(url).netloc.lower().removeprefix("www.")
        for url in seed_urls
        if url
    }
    scored_documents: list[ScoredLocalDocument] = []
    seen_urls: set[str] = set()
    candidates: list[tuple[float, str, str]] = []
    for seed_url in seed_urls:
        candidates.extend(_discover_sitemap_links(city, seed_url))
        root_html = _fetch_html(seed_url)
        if not root_html:
            continue
        root_doc = _extract_document(city, seed_url, root_html, discovered_from=seed_url)
        if root_doc is not None:
            scored_documents.append(
                _score_document(
                    city,
                    root_doc,
                    discovery_score=2.0,
                    seed_hosts=seed_hosts,
                )
            )
            seen_urls.add(seed_url)
        candidates.extend(_discover_candidate_links(city, seed_url, root_html))

    expanded_candidates = list(sorted(candidates, reverse=True))
    second_hop_pages = 0
    for discovery_score, topic, candidate_url in expanded_candidates:
        if candidate_url in seen_urls:
            continue
        html = _fetch_html(candidate_url)
        if not html:
            continue
        document = _extract_document(
            city,
            candidate_url,
            html,
            preferred_topic=topic,
            discovered_from=seed_urls[0],
        )
        if document is None:
            continue
        scored_documents.append(
            _score_document(
                city,
                document,
                discovery_score=discovery_score,
                seed_hosts=seed_hosts,
            )
        )
        seen_urls.add(candidate_url)
        if second_hop_pages < 6:
            second_hop_pages += 1
            candidates.extend(
                _discover_candidate_links(city, candidate_url, html)[:MAX_SECOND_HOP_LINKS]
            )
        if len(scored_documents) >= MAX_CRAWLED_DOCS_PER_CITY:
            break

    return _select_top_documents(scored_documents)


def synthesize_local_knowledge_fallback(
    city: CityRecord,
    *,
    guide_docs: list[SourceDocument],
    background_docs: list[SourceDocument],
    forum_docs: list[SourceDocument],
) -> list[SourceDocument]:
    source_pool = [*forum_docs, *guide_docs, *background_docs]
    synthesized: list[SourceDocument] = []
    seen_doc_ids: set[str] = set()
    for topic in FALLBACK_TARGET_TOPICS:
        candidates = _rank_fallback_candidates(topic, source_pool)
        selected_candidates = []
        for score, document in candidates:
            if document.doc_id in seen_doc_ids:
                continue
            selected_candidates.append((score, document))
            seen_doc_ids.add(document.doc_id)
            if len(selected_candidates) >= 3:
                break
        if not selected_candidates:
            continue

        snippets: list[str] = []
        source_doc_ids: list[str] = []
        source_titles: list[str] = []
        source_urls: list[str] = []
        for _, document in selected_candidates:
            snippet = _fallback_snippet(document.text, topic)
            if not snippet:
                continue
            snippets.append(f"{document.title}: {snippet}")
            source_doc_ids.append(document.doc_id)
            source_titles.append(document.title)
            if document.source_url:
                source_urls.append(document.source_url)
        if len(snippets) < 2:
            continue
        text = "\n\n".join(snippets).strip()
        if count_words(text) < 120:
            continue
        synthesized.append(
            SourceDocument(
                doc_id=f"{city.slug}-fallback-{topic}",
                title=f"{city.name} Local Guide: {topic.replace('_', ' ').title()}",
                source_url=source_urls[0] if source_urls else "",
                location=city.name,
                topic=topic,
                source_type="local_guide",
                timestamp=now_iso_date(),
                text=text,
                metadata={
                    "provider": "fallback_local_synthesis",
                    "fallback_generated": True,
                    "source_doc_ids": source_doc_ids,
                    "source_titles": source_titles,
                    "quality_score": 0.0,
                },
            )
        )
    return synthesized


def _discover_candidate_links(city: CityRecord, base_url: str, html: str) -> list[tuple[float, str, str]]:
    parsed_base = urlparse(base_url)
    host = parsed_base.netloc.lower().removeprefix("www.")
    soup = BeautifulSoup(html, "html.parser")
    topic_best: dict[str, tuple[float, str]] = {}
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        text = anchor.get_text(" ", strip=True).strip()
        if not href or href.startswith("#"):
            continue
        absolute_url = _canonicalize_url(urljoin(base_url, href))
        parsed = urlparse(absolute_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc.lower().removeprefix("www.") != host:
            continue
        if absolute_url == _canonicalize_url(base_url):
            continue
        if _is_noise_url(absolute_url):
            continue
        haystack = " ".join([parsed.path.replace("-", " "), text]).lower()
        topic_scores: dict[str, float] = defaultdict(float)
        for topic, keywords in LOCAL_WEB_TOPIC_KEYWORDS.items():
            for keyword in keywords:
                if keyword in haystack:
                    topic_scores[topic] += 1.0 + (0.2 if keyword in parsed.path.lower() else 0.0)
        if city.name.lower() in haystack:
            for topic in list(topic_scores):
                topic_scores[topic] += 0.1
        if not topic_scores:
            continue
        topic, score = max(topic_scores.items(), key=lambda item: item[1])
        best = topic_best.get(absolute_url)
        if best is None or score > best[0]:
            topic_best[absolute_url] = (score, topic)

    ranked = sorted(
        [(score, topic, url) for url, (score, topic) in topic_best.items()],
        reverse=True,
    )
    return ranked[:MAX_DISCOVERED_LINKS]


def _discover_sitemap_links(city: CityRecord, seed_url: str) -> list[tuple[float, str, str]]:
    sitemap_urls = _candidate_sitemaps(seed_url)
    discovered: dict[str, tuple[float, str]] = {}
    for sitemap_url in sitemap_urls:
        xml_text = _fetch_text(sitemap_url)
        if not xml_text:
            continue
        for link in _extract_sitemap_urls(xml_text)[:MAX_SITEMAP_URLS]:
            if _is_noise_url(link):
                continue
            parsed = urlparse(link)
            haystack = parsed.path.replace("-", " ").lower()
            topic_scores: dict[str, float] = defaultdict(float)
            for topic, keywords in LOCAL_WEB_TOPIC_KEYWORDS.items():
                for keyword in keywords:
                    if keyword in haystack:
                        topic_scores[topic] += 1.0
            if city.name.lower() in haystack:
                for topic in list(topic_scores):
                    topic_scores[topic] += 0.1
            if not topic_scores:
                continue
            topic, score = max(topic_scores.items(), key=lambda item: item[1])
            best = discovered.get(link)
            if best is None or score > best[0]:
                discovered[link] = (score + 0.5, topic)
    return sorted(
        [(score, topic, url) for url, (score, topic) in discovered.items()],
        reverse=True,
    )[:MAX_DISCOVERED_LINKS]


def _extract_document(
    city: CityRecord,
    page_url: str,
    html: str,
    *,
    preferred_topic: str = "",
    discovered_from: str = "",
) -> SourceDocument | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _page_title(soup, city)
    text_blocks = _extract_text_blocks(soup)
    text = "\n\n".join(text_blocks).strip()
    if count_words(text) < MIN_DOC_WORDS:
        return None
    topic = preferred_topic or _infer_topic_from_page(title, page_url, text)
    if not topic:
        return None
    parsed = urlparse(page_url)
    domain = parsed.netloc.lower().removeprefix("www.")
    return SourceDocument(
        doc_id=f"{city.slug}-local-web-{slugify(title)[:40] or slugify(domain)}",
        title=f"{city.name} Local Guide: {title}",
        source_url=page_url,
        location=city.name,
        topic=topic,
        source_type="local_guide",
        timestamp=now_iso_date(),
        text=text,
        metadata={
            "provider": "curated_web",
            "source_domain": domain,
            "discovered_from": discovered_from,
            "city_kind": city.kind,
            "section_title": title,
            "curation_tier": "curated_domain",
            "quality_score": 0.0,
        },
    )


def _score_document(
    city: CityRecord,
    document: SourceDocument,
    *,
    discovery_score: float,
    seed_hosts: set[str],
) -> ScoredLocalDocument:
    domain = str(document.metadata.get("source_domain", "")).lower()
    title = document.title.lower()
    text = document.text.lower()
    word_count = count_words(document.text)
    score = discovery_score

    if domain in seed_hosts:
        score += 1.2
    if city.name.lower() in title:
        score += 0.4
    if city.name.lower() in text[:2000]:
        score += 0.4

    topic_keywords = LOCAL_WEB_TOPIC_KEYWORDS.get(document.topic, set())
    keyword_hits = sum(1 for keyword in topic_keywords if keyword in text or keyword in title)
    score += min(keyword_hits * 0.35, 2.0)

    if 180 <= word_count <= 1800:
        score += 1.0
    elif 120 <= word_count <= 2600:
        score += 0.6
    elif word_count < 120:
        score -= 1.0
    elif word_count > 4500:
        score -= 0.5

    paragraph_like_blocks = document.text.count("\n\n") + 1
    if paragraph_like_blocks >= 6:
        score += 0.5
    elif paragraph_like_blocks <= 2:
        score -= 0.4

    local_signals = {
        "locals",
        "favorite",
        "neighborhood",
        "walkable",
        "tips",
        "recommend",
        "hidden",
        "guide",
        "family",
        "outdoor",
    }
    local_hits = sum(1 for signal in local_signals if signal in text or signal in title)
    score += min(local_hits * 0.15, 1.0)

    low_signal_fragments = {
        "privacy policy",
        "terms of use",
        "contact us",
        "subscribe",
        "cookie",
        "sign up",
        "newsletter",
    }
    low_signal_hits = sum(1 for fragment in low_signal_fragments if fragment in text or fragment in title)
    score -= low_signal_hits * 0.8

    if title.endswith(" - home") or title == city.name.lower():
        score -= 0.5

    document.metadata["quality_score"] = round(score, 3)
    document.metadata["discovery_score"] = round(discovery_score, 3)
    document.metadata["word_count"] = word_count
    return ScoredLocalDocument(
        document=document,
        quality_score=score,
        topic=document.topic,
        domain=domain,
    )


def _select_top_documents(scored_documents: list[ScoredLocalDocument]) -> list[SourceDocument]:
    if not scored_documents:
        return []

    deduped: list[ScoredLocalDocument] = []
    seen_signatures: set[tuple[str, str]] = set()
    for scored in sorted(scored_documents, key=lambda item: item.quality_score, reverse=True):
        signature = (scored.document.source_url, scored.document.title)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        deduped.append(scored)

    topic_counts: dict[str, int] = defaultdict(int)
    domain_counts: dict[str, int] = defaultdict(int)
    selected: list[SourceDocument] = []
    for scored in deduped:
        if topic_counts[scored.topic] >= MAX_DOCS_PER_TOPIC:
            continue
        if domain_counts[scored.domain] >= MAX_DOCS_PER_DOMAIN:
            continue
        selected.append(scored.document)
        topic_counts[scored.topic] += 1
        domain_counts[scored.domain] += 1
        if len(selected) >= MAX_DOCS_PER_CITY:
            break
    return selected


def _rank_fallback_candidates(
    topic: str,
    source_pool: list[SourceDocument],
) -> list[tuple[float, SourceDocument]]:
    rule = FALLBACK_TOPIC_RULES[topic]
    ranked: list[tuple[float, SourceDocument]] = []
    for document in source_pool:
        score = 0.0
        title = document.title.lower()
        text = document.text.lower()
        if document.source_type == "forum_digest":
            score += 1.5
        elif document.source_type == "guide":
            score += 1.0
        elif document.source_type == "background":
            score += 0.6
        if document.topic in rule["source_topics"]:
            score += 1.3
        score += sum(0.55 for keyword in rule["title_keywords"] if keyword in title)
        score += sum(0.18 for keyword in rule["body_keywords"] if keyword in text[:5000])
        if topic == "local_opinion" and document.source_type == "forum_digest":
            score += 0.6
        if score >= 1.5:
            ranked.append((score, document))
    return sorted(ranked, key=lambda item: item[0], reverse=True)


def _fallback_snippet(text: str, topic: str) -> str:
    rule = FALLBACK_TOPIC_RULES[topic]
    paragraphs = [part.strip() for part in text.split("\n\n") if part.strip()]
    best = ""
    best_score = -1.0
    for paragraph in paragraphs[:12]:
        lowered = paragraph.lower()
        score = 0.0
        score += sum(1.0 for keyword in rule["body_keywords"] if keyword in lowered)
        if len(paragraph.split()) >= 18:
            score += 0.4
        if score > best_score:
            best_score = score
            best = paragraph
    if not best and paragraphs:
        best = paragraphs[0]
    words = best.split()
    return " ".join(words[:FALLBACK_MAX_SNIPPET_WORDS]).strip()


def _fetch_html(url: str) -> str:
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception:
        return ""
    content_type = str(response.headers.get("Content-Type", "")).lower()
    if "html" not in content_type and "<html" not in response.text[:300].lower():
        return ""
    return response.text


def _fetch_text(url: str) -> str:
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception:
        return ""
    return response.text


def _page_title(soup: BeautifulSoup, city: CityRecord) -> str:
    heading = soup.find(["h1", "h2"])
    if heading:
        title = heading.get_text(" ", strip=True).strip()
        if title:
            return title
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(" ", strip=True).strip()
        if title:
            return title.replace(" | ", " - ")
    return f"{city.name} local guide"


def _extract_text_blocks(soup: BeautifulSoup) -> list[str]:
    container = soup.find("main") or soup.find("article") or soup.body or soup
    blocks: list[str] = []
    for element in container.descendants:
        if not isinstance(element, Tag):
            continue
        if element.name not in {"p", "li", "h2", "h3"}:
            continue
        text = element.get_text(" ", strip=True).strip()
        if len(text.split()) < 8:
            continue
        if text.lower().startswith(("cookie", "privacy", "subscribe")):
            continue
        blocks.append(text)
    deduped: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        if block in seen:
            continue
        seen.add(block)
        deduped.append(block)
    return deduped[:40]


def _infer_topic_from_page(title: str, page_url: str, text: str) -> str:
    haystack = " ".join([title.lower(), page_url.lower(), text[:3000].lower()])
    topic_scores: dict[str, float] = defaultdict(float)
    for topic, keywords in LOCAL_WEB_TOPIC_KEYWORDS.items():
        for keyword in keywords:
            if keyword in haystack:
                topic_scores[topic] += 1.0
    if not topic_scores:
        return ""
    return max(topic_scores.items(), key=lambda item: item[1])[0]


def _canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean_path = parsed.path.rstrip("/") or "/"
    return parsed._replace(query="", fragment="", path=clean_path).geturl()


def _candidate_sitemaps(seed_url: str) -> list[str]:
    parsed = urlparse(seed_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    return [f"{root}/sitemap.xml", f"{root}/sitemap_index.xml"]


def _extract_sitemap_urls(xml_text: str) -> list[str]:
    urls: list[str] = []
    try:
        root = ElementTree.fromstring(xml_text)
    except Exception:
        return urls
    for element in root.iter():
        if element.tag.endswith("loc") and element.text:
            urls.append(element.text.strip())
    return urls


def _is_noise_url(url: str) -> bool:
    lowered = url.lower()
    noisy_fragments = [
        "/tag/",
        "/tags/",
        "/category/",
        "/author/",
        "/feed",
        "/page/",
        "/search",
        "/privacy",
        "/terms",
        "/cookie",
        "/contact",
        "/about",
        "/wp-json",
        ".jpg",
        ".png",
        ".pdf",
    ]
    return any(fragment in lowered for fragment in noisy_fragments)
