# GCP and Google Maps Free-Tier Notes

These notes reflect the official Google Cloud and Google Maps pages I checked on April 18, 2026.

## Google Cloud

- New Google Cloud customers get `$300` in free trial credit for about `90 days`.
- Compute Engine has an always-free `e2-micro` VM option.
- The always-free `e2-micro` applies in these U.S. regions:
  - `us-west1` (Oregon)
  - `us-central1` (Iowa)
  - `us-east1` (South Carolina)
- The always-free Compute Engine offer also includes:
  - `30 GB-months` of standard persistent disk
  - `1 GB` of outbound data transfer per month from North America
- GPUs are **not** included in the always-free tier.

Official sources:

- [Google Cloud Free Program](https://cloud.google.com/free?hl=en-US)
- [Compute Engine Free Tier details](https://cloud.google.com/free/docs/compute-getting-started)

## Google Maps Platform

- Google Maps Platform now advertises free monthly calls per SKU instead of the older flat `$200` monthly credit model.
- The pricing page currently says you can get started with:
  - up to `10K` free calls per SKU per month for many Essentials products
  - up to `5K` free calls per SKU per month for many Pro products
  - up to `1K` free calls per SKU per month for many Enterprise products
- The Places API `searchText` endpoint can be used for rating-sensitive local place search.

Official sources:

- [Google Maps Platform pricing](https://mapsplatform.google.com/pricing/?hl=en-US)
- [Places API `searchText`](https://developers.google.com/maps/documentation/places/web-service/reference/rest/v1/places/searchText)

## Practical Recommendation for LocalLens

For class demo purposes:

1. Use your own machine or lab metal for the local LLM if possible.
2. Use free or trial GCP only for hosting the Streamlit app and the retrieval/database layer.
3. If you want rating-aware restaurant/place answers, enable Google Maps Places with a restricted API key.
4. If you stay inside the free monthly Maps limits and the `e2-micro` VM limits, you can keep costs very low.

## Suggested Deployment Path

1. Build the LocalLens Docker image locally.
2. Deploy the Streamlit app on a small GCP VM, or on Cloud Run if you keep the app stateless.
3. Point `OLLAMA_BASE_URL` either to:
   - a local machine you control on the same network, or
   - a separate GPU/CPU host running Ollama.
4. Restrict your Google Maps API key by API type and IP/domain.

