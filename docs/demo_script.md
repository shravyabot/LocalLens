# LocalLens Demo Script

## 1. Elevator Pitch

LocalLens is a local-first RAG assistant for travelers and recent movers. Instead of generic travel content, it combines city guides, local forum discussions, and structured place data to explain the practical tradeoffs people usually learn only after living somewhere for a while.

## 2. Demo Flow

1. Show the corpus stats in the hero banner.
2. Mention that the model is local through Ollama and that citations are clickable.
2. Ask a general narrative question:
   `What should I know before moving to Seattle?`
3. Ask a local activity question:
   `Where is a good sunset spot in San Francisco?`
4. Ask a place-discovery question:
   `Best taco place in San Jose over 4.5?`
5. Open one or two clickable citations.
6. Point out the local model note in Retrieval Notes.
7. Show one recommended place card and explain why it was ranked highly.

## 3. Corner Cases

- Ask with no location:
  `Where should I go for a long weekend if I like walkable neighborhoods?`
- Ask a safety/logistics question:
  `What local safety issues should I know in New Orleans?`
- Ask a transit question:
  `Can I rely on public transit in Chicago?`

## 4. Talking Points

- Hybrid retrieval over narrative documents plus structured places
- Local generation via Ollama
- Clickable passage citations
- Optional Google Places enrichment for rating-sensitive place search
- SQLite + offline embeddings for reproducibility
- Cross-country city coverage rather than a single-city demo

## 5. Questions To Be Ready For

- `How many total records and passages are in the database?`
- `Which sources are static corpus data versus live enrichment?`
- `What happens when evidence is thin or a city is missing?`
- `How would you scale this beyond the current set of cities?`
