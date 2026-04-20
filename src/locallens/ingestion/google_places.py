from __future__ import annotations

from urllib.parse import quote

import requests

from locallens.schemas import CityRecord, PlaceRecord
from locallens.utils import now_iso_date, slugify


SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"


class GooglePlacesClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()

    def available(self) -> bool:
        return bool(self.api_key)

    def search_places(
        self,
        *,
        city: CityRecord,
        query: str,
        category: str,
        limit: int = 5,
    ) -> list[PlaceRecord]:
        if not self.available():
            return []
        response = requests.post(
            SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": self.api_key,
                "X-Goog-FieldMask": ",".join(
                    [
                        "places.id",
                        "places.displayName",
                        "places.formattedAddress",
                        "places.googleMapsUri",
                        "places.rating",
                        "places.userRatingCount",
                        "places.priceLevel",
                        "places.primaryTypeDisplayName",
                        "places.location",
                    ]
                ),
            },
            json={"textQuery": f"{query} in {city.name}", "pageSize": limit},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        places: list[PlaceRecord] = []
        for place in payload.get("places", []):
            name = str(place.get("displayName", {}).get("text", "")).strip()
            place_id = str(place.get("id", "")).strip()
            if not place_id or not name:
                continue
            places.append(
                PlaceRecord(
                    place_id=f"gplaces-{place_id}",
                    location=city.name,
                    name=name,
                    category=category,
                    source_provider="google_places",
                    source_url=str(place.get("googleMapsUri", "")),
                    latitude=float(place.get("location", {}).get("latitude", city.latitude)),
                    longitude=float(place.get("location", {}).get("longitude", city.longitude)),
                    address=str(place.get("formattedAddress", "")),
                    rating=float(place["rating"]) if place.get("rating") is not None else None,
                    review_count=int(place["userRatingCount"]) if place.get("userRatingCount") is not None else None,
                    price_level=str(place.get("priceLevel", "")),
                    tags=[
                        str(place.get("primaryTypeDisplayName", {}).get("text", "")).strip(),
                    ],
                    description=f"{name} matched the query '{query}' in {city.name}.",
                    metadata={"provider": "google_places", "fetched_at": now_iso_date()},
                )
            )
        return places

