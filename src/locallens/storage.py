from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from locallens.schemas import ChunkRecord, PlaceRecord, SourceDocument
from locallens.utils import ensure_parent


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS source_documents (
    doc_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    source_url TEXT,
    location TEXT NOT NULL,
    topic TEXT NOT NULL,
    source_type TEXT NOT NULL,
    timestamp TEXT,
    text TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    title TEXT NOT NULL,
    source_url TEXT,
    location TEXT NOT NULL,
    topic TEXT NOT NULL,
    source_type TEXT NOT NULL,
    timestamp TEXT,
    passage_text TEXT NOT NULL,
    passage_index INTEGER NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS places (
    place_id TEXT PRIMARY KEY,
    location TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    source_provider TEXT NOT NULL,
    source_url TEXT,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    address TEXT,
    neighborhood TEXT,
    rating REAL,
    review_count INTEGER,
    price_level TEXT,
    cuisine_json TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    description TEXT,
    image_url TEXT,
    review_snippets_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_docs_location_topic ON source_documents(location, topic);
CREATE INDEX IF NOT EXISTS idx_chunks_location_topic ON chunks(location, topic);
CREATE INDEX IF NOT EXISTS idx_places_location_category ON places(location, category);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    ensure_parent(db_path)
    # Streamlit may reuse cached resources across rerun threads, so the
    # connection needs to allow cross-thread access within this local app.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def replace_documents(conn: sqlite3.Connection, documents: list[SourceDocument]) -> None:
    conn.execute("DELETE FROM source_documents")
    conn.executemany(
        """
        INSERT INTO source_documents
        (doc_id, title, source_url, location, topic, source_type, timestamp, text, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                document.doc_id,
                document.title,
                document.source_url,
                document.location,
                document.topic,
                document.source_type,
                document.timestamp,
                document.text,
                json.dumps(document.metadata, ensure_ascii=False),
            )
            for document in documents
        ],
    )
    conn.commit()


def replace_chunks(conn: sqlite3.Connection, chunks: list[ChunkRecord]) -> None:
    conn.execute("DELETE FROM chunks")
    conn.executemany(
        """
        INSERT INTO chunks
        (chunk_id, doc_id, title, source_url, location, topic, source_type, timestamp, passage_text, passage_index, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                chunk.chunk_id,
                chunk.doc_id,
                chunk.title,
                chunk.source_url,
                chunk.location,
                chunk.topic,
                chunk.source_type,
                chunk.timestamp,
                chunk.passage_text,
                chunk.passage_index,
                json.dumps(chunk.metadata, ensure_ascii=False),
            )
            for chunk in chunks
        ],
    )
    conn.commit()


def replace_places(conn: sqlite3.Connection, places: list[PlaceRecord]) -> None:
    conn.execute("DELETE FROM places")
    conn.executemany(
        """
        INSERT INTO places
        (place_id, location, name, category, source_provider, source_url, latitude, longitude, address, neighborhood,
         rating, review_count, price_level, cuisine_json, tags_json, description, image_url, review_snippets_json, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                place.place_id,
                place.location,
                place.name,
                place.category,
                place.source_provider,
                place.source_url,
                place.latitude,
                place.longitude,
                place.address,
                place.neighborhood,
                place.rating,
                place.review_count,
                place.price_level,
                json.dumps(place.cuisine, ensure_ascii=False),
                json.dumps(place.tags, ensure_ascii=False),
                place.description,
                place.image_url,
                json.dumps(place.review_snippets, ensure_ascii=False),
                json.dumps(place.metadata, ensure_ascii=False),
            )
            for place in places
        ],
    )
    conn.commit()


def load_documents(conn: sqlite3.Connection) -> list[SourceDocument]:
    rows = conn.execute("SELECT * FROM source_documents ORDER BY location, topic, doc_id").fetchall()
    return [
        SourceDocument(
            doc_id=row["doc_id"],
            title=row["title"],
            source_url=row["source_url"],
            location=row["location"],
            topic=row["topic"],
            source_type=row["source_type"],
            timestamp=row["timestamp"],
            text=row["text"],
            metadata=json.loads(row["metadata_json"]),
        )
        for row in rows
    ]


def load_chunks(conn: sqlite3.Connection) -> list[ChunkRecord]:
    rows = conn.execute("SELECT * FROM chunks ORDER BY location, topic, chunk_id").fetchall()
    return [
        ChunkRecord(
            chunk_id=row["chunk_id"],
            doc_id=row["doc_id"],
            title=row["title"],
            source_url=row["source_url"],
            location=row["location"],
            topic=row["topic"],
            source_type=row["source_type"],
            timestamp=row["timestamp"],
            passage_text=row["passage_text"],
            passage_index=row["passage_index"],
            metadata=json.loads(row["metadata_json"]),
        )
        for row in rows
    ]


def load_places(conn: sqlite3.Connection) -> list[PlaceRecord]:
    rows = conn.execute("SELECT * FROM places ORDER BY location, category, name").fetchall()
    return [
        PlaceRecord(
            place_id=row["place_id"],
            location=row["location"],
            name=row["name"],
            category=row["category"],
            source_provider=row["source_provider"],
            source_url=row["source_url"],
            latitude=row["latitude"],
            longitude=row["longitude"],
            address=row["address"] or "",
            neighborhood=row["neighborhood"] or "",
            rating=row["rating"],
            review_count=row["review_count"],
            price_level=row["price_level"] or "",
            cuisine=json.loads(row["cuisine_json"]),
            tags=json.loads(row["tags_json"]),
            description=row["description"] or "",
            image_url=row["image_url"] or "",
            review_snippets=json.loads(row["review_snippets_json"]),
            metadata=json.loads(row["metadata_json"]),
        )
        for row in rows
    ]
