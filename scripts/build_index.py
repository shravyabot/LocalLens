from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from locallens.config import get_settings
from locallens.retrieval.dense import build_dense_embeddings
from locallens.storage import connect, load_chunks


def main() -> None:
    settings = get_settings(ROOT)
    conn = connect(settings.database_path)
    chunks = load_chunks(conn)
    _, ids, backend = build_dense_embeddings(settings, chunks)
    target = settings.qdrant_path if backend.startswith("qdrant:") else settings.embedding_matrix_path
    print(
        f"Built dense index for {len(ids)} chunks using backend {backend} "
        f"at {target}"
    )


if __name__ == "__main__":
    main()
