from __future__ import annotations

import re

from rank_bm25 import BM25Okapi

from locallens.schemas import ChunkRecord, SearchResult


TOKEN_RE = re.compile(r"[A-Za-z0-9']+")


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def match_filters(chunk: ChunkRecord, filters: dict[str, str] | None) -> bool:
    if not filters:
        return True
    if filters.get("location") and chunk.location != filters["location"]:
        return False
    if filters.get("topic") and chunk.topic != filters["topic"]:
        return False
    return True


class BM25Retriever:
    def __init__(self, chunks: list[ChunkRecord]) -> None:
        self.chunks = chunks
        self.corpus_tokens = [tokenize(_chunk_text(chunk)) for chunk in chunks]
        self.model = BM25Okapi(self.corpus_tokens) if self.corpus_tokens else None

    def search(
        self,
        query: str,
        *,
        top_k: int,
        filters: dict[str, str] | None = None,
    ) -> list[SearchResult]:
        if not self.model:
            return []
        scores = self.model.get_scores(tokenize(query))
        ranked = sorted(
            enumerate(scores),
            key=lambda item: item[1],
            reverse=True,
        )
        results: list[SearchResult] = []
        for index, score in ranked:
            chunk = self.chunks[index]
            if not match_filters(chunk, filters):
                continue
            if score <= 0:
                continue
            results.append(
                SearchResult(
                    chunk=chunk,
                    bm25_score=float(score),
                    final_score=float(score),
                )
            )
            if len(results) == top_k:
                break
        return results


def _chunk_text(chunk: ChunkRecord) -> str:
    return " ".join(
        [
            chunk.title,
            chunk.location,
            chunk.topic,
            chunk.source_type.replace("_", " "),
            chunk.passage_text,
        ]
    )

