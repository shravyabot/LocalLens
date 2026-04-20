from __future__ import annotations

from locallens.config import Settings
from locallens.retrieval.bm25 import BM25Retriever
from locallens.retrieval.dense import DenseRetriever
from locallens.retrieval.rerank import HeuristicReranker
from locallens.schemas import ChunkRecord, SearchResult


class HybridRetriever:
    def __init__(self, chunks: list[ChunkRecord], settings: Settings) -> None:
        self.chunks = chunks
        self.settings = settings
        self.bm25 = BM25Retriever(chunks)
        self.dense = DenseRetriever(chunks, settings)
        self.reranker = HeuristicReranker()

    def search(
        self,
        query: str,
        *,
        top_k: int | None = None,
        candidate_k: int | None = None,
        filters: dict[str, str] | None = None,
    ) -> list[SearchResult]:
        top_k = top_k or self.settings.top_k
        candidate_k = candidate_k or self.settings.candidate_k

        bm25_results = self.bm25.search(query, top_k=candidate_k, filters=filters)
        dense_results = self.dense.search(query, top_k=candidate_k, filters=filters)

        merged: dict[str, SearchResult] = {}
        self._accumulate_rrf(merged, bm25_results, "bm25_score")
        self._accumulate_rrf(merged, dense_results, "dense_score")
        reranked = self.reranker.rerank(query, list(merged.values()))
        return reranked[:top_k]

    @staticmethod
    def _accumulate_rrf(
        merged: dict[str, SearchResult],
        results: list[SearchResult],
        score_field: str,
        *,
        rrf_k: int = 60,
    ) -> None:
        for rank, result in enumerate(results, start=1):
            existing = merged.get(result.chunk.chunk_id)
            if existing is None:
                existing = SearchResult(chunk=result.chunk)
                merged[result.chunk.chunk_id] = existing
            setattr(existing, score_field, getattr(result, score_field))
            existing.rrf_score += 1.0 / (rrf_k + rank)
            existing.final_score = existing.rrf_score

