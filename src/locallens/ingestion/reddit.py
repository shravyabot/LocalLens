from __future__ import annotations

import requests

from locallens.schemas import CityRecord, SourceDocument
from locallens.taxonomy import REDDIT_TOPIC_QUERIES
from locallens.utils import now_iso_date, slugify

SEARCH_TIMEOUT_SECONDS = 5
COMMENT_TIMEOUT_SECONDS = 5
MAX_SUBREDDITS_PER_CITY = 2
MAX_QUERIES_PER_TOPIC = 2
MAX_DOCS_PER_TOPIC = 2


def fetch_city_threads(city: CityRecord, *, user_agent: str) -> list[SourceDocument]:
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    documents: list[SourceDocument] = []
    seen_post_ids: set[str] = set()

    for topic, queries in REDDIT_TOPIC_QUERIES.items():
        topic_collected = 0
        for query in queries[:MAX_QUERIES_PER_TOPIC]:
            search_query = f"\"{city.name}\" {query}"
            for subreddit in city.reddit_subreddits[:MAX_SUBREDDITS_PER_CITY]:
                for post in _search_subreddit(subreddit, search_query, headers=headers):
                    post_id = str(post.get("id", "")).strip()
                    if not post_id or post_id in seen_post_ids:
                        continue
                    if not _relevant(post, city, query=query):
                        continue
                    comments = _fetch_comments(str(post.get("permalink", "")), headers=headers)
                    document = _to_document(city, topic, post, comments)
                    if document is None:
                        continue
                    seen_post_ids.add(post_id)
                    documents.append(document)
                    topic_collected += 1
                    if topic_collected >= MAX_DOCS_PER_TOPIC:
                        break
                if topic_collected >= MAX_DOCS_PER_TOPIC:
                    break
            if topic_collected >= MAX_DOCS_PER_TOPIC:
                break
    return documents


def _search_subreddit(
    subreddit: str,
    query: str,
    *,
    headers: dict[str, str],
) -> list[dict[str, object]]:
    response = requests.get(
        f"https://www.reddit.com/r/{subreddit}/search.json",
        params={
            "q": query,
            "restrict_sr": 1,
            "sort": "top",
            "t": "year",
            "limit": 8,
            "type": "link",
            "raw_json": 1,
        },
        headers=headers,
        timeout=SEARCH_TIMEOUT_SECONDS,
    )
    if not response.ok:
        return []
    payload = response.json()
    return [item.get("data", {}) for item in payload.get("data", {}).get("children", [])]


def _fetch_comments(permalink: str, *, headers: dict[str, str]) -> list[str]:
    if not permalink:
        return []
    response = requests.get(
        f"https://www.reddit.com{permalink}.json",
        params={"limit": 6, "sort": "top", "raw_json": 1},
        headers=headers,
        timeout=COMMENT_TIMEOUT_SECONDS,
    )
    if not response.ok:
        return []
    payload = response.json()
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    comments: list[str] = []
    for child in payload[1].get("data", {}).get("children", []):
        if child.get("kind") != "t1":
            continue
        text = str(child.get("data", {}).get("body", "")).strip()
        if len(text) >= 40:
            comments.append(text)
        if len(comments) == 5:
            break
    return comments


def _relevant(post: dict[str, object], city: CityRecord, *, query: str) -> bool:
    combined = " ".join(
        [
            str(post.get("title", "")),
            str(post.get("selftext", "")),
            str(post.get("subreddit", "")),
        ]
    ).lower()
    city_terms = [city.name.lower(), *[alias.lower() for alias in city.aliases]]
    if not any(term in combined for term in city_terms):
        return False
    query_terms = [token for token in query.lower().split() if len(token) >= 4]
    return any(token in combined for token in query_terms)


def _to_document(
    city: CityRecord,
    topic: str,
    post: dict[str, object],
    comments: list[str],
) -> SourceDocument | None:
    title = str(post.get("title", "")).strip()
    permalink = str(post.get("permalink", "")).strip()
    if not title or not permalink:
        return None
    body_parts = [
        f"Local discussion about {city.name}.",
        f"Thread title: {title}",
    ]
    selftext = str(post.get("selftext", "")).strip()
    if selftext:
        body_parts.append(f"Original post: {selftext}")
    if comments:
        body_parts.append("Top comments:")
        body_parts.extend(f"- {comment}" for comment in comments)
    text = "\n\n".join(body_parts).strip()
    if len(text.split()) < 40:
        return None
    return SourceDocument(
        doc_id=f"{city.slug}-{topic}-reddit-{slugify(title)[:32]}",
        title=f"{city.name} Reddit: {title}",
        source_url=f"https://www.reddit.com{permalink}",
        location=city.name,
        topic=topic,
        source_type="forum_digest",
        timestamp=now_iso_date(),
        text=text,
        metadata={
            "provider": "reddit",
            "subreddit": str(post.get("subreddit", "")),
            "score": post.get("score"),
            "city_kind": city.kind,
        },
    )
