# LocalLens

LocalLens is a local-first RAG assistant for travelers and recent movers. It combines open travel guides, local forum threads, structured place records, and optional Google Places enrichment to answer questions like:

- `What should I know before moving to Seattle?`
- `Where is a good sunset spot in San Francisco?`
- `Best taco place in San Jose over 4.5?`
- `Can I rely on public transit in Chicago?`

The project is designed around the CS 6120 final-project rubric:

- Streamlit frontend
- 10k+ entry database
- local model serving with Ollama
- clickable citations to source passages
- reproducible ingestion/build pipeline
- Dockerized setup
- writeup and demo guidance

## Architecture

```mermaid
flowchart LR
    A["Open data sources\nWikivoyage, Wikipedia, Reddit, OSM,\noptional Google Places"] --> B["Normalize into SQLite\nsource_documents + places"]
    B --> C["Chunk long documents"]
    C --> D["Dense embeddings\nall-MiniLM-L6-v2"]
    C --> E["BM25 lexical index"]
    D --> F["Hybrid retrieval + RRF"]
    E --> F
    G["Structured place search\nratings/category/location filters"] --> H["Narrative answer composer"]
    F --> H
    I["Local Ollama model"] --> H
    H --> J["Streamlit UI\nanswer, why, tips, place cards, citations"]
```

## Data Sources

Default open-source pipeline:

- `Wikivoyage`: sectioned city/park guide content for activities, transit, food, safety, lodging
- `Wikipedia`: summaries and thumbnail images for orientation and visual context
- `Reddit`: city-subreddit travel/local threads with top comments
- `OpenStreetMap / Overpass`: structured places for restaurants, parks, museums, attractions, hotels, viewpoints, and transit nodes

Optional enrichment:

- `Google Places API`: live ratings, review counts, and place metadata for queries like `best taco place in san jose over 4.5`
- `NPS API`: optional national-park metadata if you add a key

## Project Layout

```text
LocalLens/
├── app.py
├── artifacts/
├── data/
│   ├── processed/
│   └── raw/
├── docs/
├── scripts/
├── src/locallens/
├── .env.example
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Quickstart

### 1. Install

```bash
cd /Users/anusharavikumar/Desktop/LocalLens
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Minimum recommended `.env` values:

```bash
OLLAMA_BASE_URL=http://localhost:11434
LOCALLENS_OLLAMA_MODEL=llama3.1:8b-instruct-q4_K_M
LOCALLENS_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2
LOCALLENS_RERANK_MODEL=cross-encoder/ms-marco-MiniLM-L-6-v2
REDDIT_USER_AGENT=LocalLens/0.1
GOOGLE_MAPS_API_KEY=
```

### 3. Build the corpus

```bash
python3 scripts/build_corpus.py
python3 scripts/build_index.py
```

Notes:

- The nationwide build fetches travel guides, Wikipedia summaries, Reddit discussions, and OpenStreetMap place records across the LocalLens city catalog.
- On a fresh machine, the full build can take several minutes because it is downloading and normalizing external sources.
- For a quicker local development pass, you can skip Reddit:

```bash
python3 scripts/build_corpus.py --skip-reddit
python3 scripts/build_index.py
```

### 4. Run the app

```bash
PYTHONPATH=src streamlit run app.py
```

## Local Model Setup

LocalLens expects a local generation model served through Ollama.

```bash
ollama pull llama3.1:8b-instruct-q4_K_M
ollama serve
```

The retrieval embeddings and reranker run locally through Python models:

- `sentence-transformers/all-MiniLM-L6-v2`
- `cross-encoder/ms-marco-MiniLM-L-6-v2`

## Google Places Support

If you want rating-sensitive place answers such as `best taco place in san jose over 4.5`, add a `GOOGLE_MAPS_API_KEY`.

The app still works without it, but rating-aware place search is much better with Google Places enabled.

See [docs/gcp_free_tier.md](/Users/anusharavikumar/Desktop/LocalLens/docs/gcp_free_tier.md) for current Google Cloud and Maps free-tier notes.

## Docker

### Build

```bash
docker build -t locallens .
```

### Run

```bash
docker run --rm -p 8501:8501 \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  locallens
```

### Compose

```bash
docker compose up --build
```

## Deployment Notes

### Local demo

The easiest path for presentation day is:

1. Run Ollama locally on the same machine.
2. Build the LocalLens SQLite database and embeddings ahead of time.
3. Launch Streamlit and expose the machine's IP address on your local network or a VM.

### Free or low-cost GCP path

If you want to use Google Cloud:

1. Use a small Compute Engine VM for the Streamlit app, SQLite database, and embedding artifacts.
2. Keep Ollama on:
   - the same VM if you provision enough CPU/RAM, or
   - a separate machine you control.
3. Restrict Google Maps keys if you enable Places enrichment.

See [docs/gcp_free_tier.md](/Users/anusharavikumar/Desktop/LocalLens/docs/gcp_free_tier.md) for the current free-tier notes and official links.

## Reproducibility Notes

- `data/processed/locallens.db` is the main SQLite database.
- `artifacts/chunk_embeddings.npy` stores the dense index.
- All ingestion is done with code in `src/locallens/ingestion/`.
- If you change sources or city coverage, rerun:

```bash
python3 scripts/build_corpus.py
python3 scripts/build_index.py
```

## Submission Checklist

- Code and README
- Docker setup
- Streamlit demo
- Writeup PDF from [docs/PROJECT_WRITEUP.tex](/Users/anusharavikumar/Desktop/LocalLens/docs/PROJECT_WRITEUP.tex)
- Slide/demo notes from [docs/demo_script.md](/Users/anusharavikumar/Desktop/LocalLens/docs/demo_script.md)
- GCP/free-tier notes from [docs/gcp_free_tier.md](/Users/anusharavikumar/Desktop/LocalLens/docs/gcp_free_tier.md)
