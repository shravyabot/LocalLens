from __future__ import annotations

from locallens.retrieval.bm25 import tokenize
from locallens.schemas import SearchResult


SOURCE_TYPE_BOOST = {
    "official_faq": 0.10,
    "guide": 0.06,
    "forum_digest": 0.03,
    "background": 0.01,
}


class HeuristicReranker:
    def rerank(self, query: str, results: list[SearchResult]) -> list[SearchResult]:
        query_tokens = set(tokenize(query))
        query_lower = query.lower()
        for result in results:
            overlap = len(query_tokens.intersection(tokenize(result.chunk.passage_text)))
            overlap_score = overlap / max(len(query_tokens), 1)
            metadata_score = 0.0
            if result.chunk.location.lower() in query_lower:
                metadata_score += 0.08
            if result.chunk.topic in query_tokens:
                metadata_score += 0.05
            if "provider" in result.chunk.metadata:
                metadata_score += 0.01
            result.rerank_score = (
                result.rrf_score
                + overlap_score
                + SOURCE_TYPE_BOOST.get(result.chunk.source_type, 0.01)
                + metadata_score
            )
            result.final_score = result.rerank_score
        results.sort(key=lambda item: item.final_score, reverse=True)
        return results

