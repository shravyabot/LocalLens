from __future__ import annotations

from locallens.schemas import ChunkRecord, SourceDocument
from locallens.utils import count_words, paragraphs


def chunk_document(
    document: SourceDocument,
    *,
    max_words: int,
    overlap_words: int,
) -> list[ChunkRecord]:
    parts = paragraphs(document.text)
    chunks: list[ChunkRecord] = []
    current_parts: list[str] = []
    current_words = 0

    def flush() -> None:
        nonlocal current_parts, current_words
        if not current_parts:
            return
        passage_text = "\n".join(current_parts).strip()
        chunks.append(
            ChunkRecord(
                chunk_id=f"{document.doc_id}-chunk-{len(chunks) + 1:03d}",
                doc_id=document.doc_id,
                title=document.title,
                source_url=document.source_url,
                location=document.location,
                topic=document.topic,
                source_type=document.source_type,
                timestamp=document.timestamp,
                passage_text=passage_text,
                passage_index=len(chunks) + 1,
                metadata=document.metadata,
            )
        )
        overlap_parts = _tail_for_overlap(current_parts, overlap_words=overlap_words)
        current_parts = overlap_parts
        current_words = sum(count_words(part) for part in current_parts)

    for part in parts:
        part_words = count_words(part)
        if current_words and current_words + part_words > max_words:
            flush()
        current_parts.append(part)
        current_words += part_words

    flush()
    return chunks


def chunk_documents(
    documents: list[SourceDocument],
    *,
    max_words: int,
    overlap_words: int,
) -> list[ChunkRecord]:
    chunks: list[ChunkRecord] = []
    for document in documents:
        chunks.extend(
            chunk_document(
                document,
                max_words=max_words,
                overlap_words=overlap_words,
            )
        )
    return chunks


def _tail_for_overlap(parts: list[str], *, overlap_words: int) -> list[str]:
    if overlap_words <= 0:
        return []
    kept: list[str] = []
    words_kept = 0
    for part in reversed(parts):
        kept.insert(0, part)
        words_kept += count_words(part)
        if words_kept >= overlap_words:
            break
    return kept

