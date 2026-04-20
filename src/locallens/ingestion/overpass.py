from __future__ import annotations

from collections import Counter

import requests

from locallens.schemas import CityRecord, PlaceRecord
from locallens.utils import slugify


OVERPASS_ENDPOINTS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
]

MAX_PLACES_PER_CITY = 800
MAX_RADIUS_METERS_CITY = 7000
MAX_RADIUS_METERS_PARK = 10000

MAX_PLACES_PER_CATEGORY = {
    "restaurant": 180,
    "hotel": 90,
    "museum": 90,
    "attraction": 120,
    "viewpoint": 80,
    "park": 120,
    "transit": 80,
    "trail": 90,
    "beach": 60,
    "zoo": 40,
    "venue": 80,
    "market": 60,
    "equestrian": 40,
}

QUERY_GROUPS = [
    """
    [out:json][timeout:45];
    (
      node(around:{radius_m},{lat},{lon})[tourism~"hotel|hostel|motel|museum|attraction|gallery|viewpoint|zoo|aquarium|theme_park|artwork"];
      way(around:{radius_m},{lat},{lon})[tourism~"hotel|hostel|motel|museum|attraction|gallery|viewpoint|zoo|aquarium|theme_park|artwork"];
    );
    out center tags;
    """,
    """
    [out:json][timeout:45];
    (
      node(around:{radius_m},{lat},{lon})[leisure~"park|garden|nature_reserve|dog_park|marina|sports_centre|stadium|water_park|horse_riding"];
      way(around:{radius_m},{lat},{lon})[leisure~"park|garden|nature_reserve|dog_park|marina|sports_centre|stadium|water_park|horse_riding"];
    );
    out center tags;
    """,
    """
    [out:json][timeout:45];
    (
      node(around:{radius_m},{lat},{lon})[public_transport];
      node(around:{radius_m},{lat},{lon})[railway=station];
      node(around:{radius_m},{lat},{lon})[amenity=bus_station];
    );
    out center tags;
    """,
    """
    [out:json][timeout:45];
    (
      node(around:{radius_m},{lat},{lon})[amenity~"theatre|cinema|arts_centre|marketplace|library"];
      way(around:{radius_m},{lat},{lon})[amenity~"theatre|cinema|arts_centre|marketplace|library"];
    );
    out center tags;
    """,
    """
    [out:json][timeout:45];
    (
      node(around:{radius_m},{lat},{lon})[natural~"beach|peak|cave_entrance"];
      way(around:{radius_m},{lat},{lon})[natural~"beach|peak|cave_entrance"];
      node(around:{radius_m},{lat},{lon})[tourism=information][information=trailhead];
      way(around:{radius_m},{lat},{lon})[tourism=information][information=trailhead];
      node(around:{radius_m},{lat},{lon})[sport=equestrian];
      way(around:{radius_m},{lat},{lon})[sport=equestrian];
    );
    out center tags;
    """,
    """
    [out:json][timeout:45];
    (
      node(around:{radius_m},{lat},{lon})[amenity~"restaurant|cafe|bar|fast_food"];
      way(around:{radius_m},{lat},{lon})[amenity~"restaurant|cafe|bar|fast_food"];
    );
    out center tags;
    """,
]


def fetch_city_places(city: CityRecord) -> list[PlaceRecord]:
    places: list[PlaceRecord] = []
    seen_names: set[tuple[str, str]] = set()
    category_counts: Counter[str] = Counter()
    radius_m = int(city.radius_km * 1000)
    radius_m = min(
        radius_m,
        MAX_RADIUS_METERS_PARK if city.kind == "park" else MAX_RADIUS_METERS_CITY,
    )
    for query in QUERY_GROUPS:
        try:
            payload = _run_query(
                query.format(radius_m=radius_m, lat=city.latitude, lon=city.longitude)
            )
        except Exception:
            continue
        for element in payload.get("elements", []):
            tags = element.get("tags", {})
            name = str(tags.get("name", "")).strip()
            if not name:
                continue
            category = _category_from_tags(tags)
            if category_counts[category] >= MAX_PLACES_PER_CATEGORY.get(category, 60):
                continue
            key = (category, name.lower())
            if key in seen_names:
                continue
            seen_names.add(key)
            lat = float(element.get("lat") or element.get("center", {}).get("lat") or city.latitude)
            lon = float(element.get("lon") or element.get("center", {}).get("lon") or city.longitude)
            cuisine = [part.strip() for part in str(tags.get("cuisine", "")).split(";") if part.strip()]
            tags_list = unique_tags(tags)
            place = PlaceRecord(
                place_id=f"osm-{element.get('type', 'node')}-{element.get('id')}",
                location=city.name,
                name=name,
                category=category,
                source_provider="openstreetmap",
                source_url=_osm_url(element),
                latitude=lat,
                longitude=lon,
                address=_address_from_tags(tags),
                neighborhood=str(tags.get("addr:suburb", "") or tags.get("addr:neighbourhood", "")).strip(),
                cuisine=cuisine,
                tags=tags_list,
                description=_place_description(city.name, name, category, tags, cuisine),
                metadata={"provider": "openstreetmap", "city_kind": city.kind},
            )
            places.append(place)
            category_counts[category] += 1
            if len(places) >= MAX_PLACES_PER_CITY:
                return places
    return places


def _run_query(query: str) -> dict[str, object]:
    last_error: Exception | None = None
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            response = requests.get(
                endpoint,
                params={"data": query},
                headers={"User-Agent": "LocalLens/0.1 (+course project; local travel RAG ingestion)"},
                timeout=8,
            )
            response.raise_for_status()
            return response.json()
        except Exception as error:
            last_error = error
            continue
    if last_error:
        raise last_error
    return {"elements": []}


def unique_tags(tags: dict[str, object]) -> list[str]:
    values = [
        str(tags.get("amenity", "")),
        str(tags.get("tourism", "")),
        str(tags.get("leisure", "")),
        str(tags.get("public_transport", "")),
        str(tags.get("railway", "")),
        str(tags.get("sport", "")),
        str(tags.get("natural", "")),
        str(tags.get("information", "")),
        str(tags.get("cuisine", "")),
    ]
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        for part in value.split(";"):
            item = part.strip()
            if item and item not in seen:
                seen.add(item)
                output.append(item)
    return output


def _category_from_tags(tags: dict[str, object]) -> str:
    amenity = str(tags.get("amenity", ""))
    tourism = str(tags.get("tourism", ""))
    leisure = str(tags.get("leisure", ""))
    sport = str(tags.get("sport", ""))
    natural = str(tags.get("natural", ""))
    information = str(tags.get("information", ""))
    if amenity in {"restaurant", "cafe", "bar", "fast_food"}:
        return "restaurant"
    if amenity == "bus_station":
        return "transit"
    if amenity in {"theatre", "cinema", "arts_centre", "library"}:
        return "venue"
    if amenity == "marketplace":
        return "market"
    if tourism in {"hotel", "hostel", "motel"}:
        return "hotel"
    if tourism in {"museum", "gallery"}:
        return "museum"
    if tourism in {"zoo", "aquarium"}:
        return "zoo"
    if tourism in {"attraction", "viewpoint"}:
        return "attraction" if tourism == "attraction" else "viewpoint"
    if tourism in {"theme_park", "artwork"}:
        return "attraction"
    if natural == "beach":
        return "beach"
    if natural == "peak":
        return "viewpoint"
    if natural == "cave_entrance":
        return "attraction"
    if tourism == "information" and information == "trailhead":
        return "trail"
    if sport == "equestrian" or leisure == "horse_riding":
        return "equestrian"
    if leisure in {"sports_centre", "stadium"}:
        return "venue"
    if leisure == "marina":
        return "attraction"
    if leisure == "water_park":
        return "attraction"
    if leisure in {"park", "garden", "nature_reserve", "dog_park"}:
        return "park"
    if str(tags.get("public_transport", "")).strip() or str(tags.get("railway", "")).strip():
        return "transit"
    return "attraction"


def _address_from_tags(tags: dict[str, object]) -> str:
    parts = [
        str(tags.get("addr:housenumber", "")).strip(),
        str(tags.get("addr:street", "")).strip(),
        str(tags.get("addr:city", "")).strip(),
    ]
    return " ".join(part for part in parts if part).strip()


def _osm_url(element: dict[str, object]) -> str:
    element_type = str(element.get("type", "node"))
    element_id = element.get("id")
    return f"https://www.openstreetmap.org/{element_type}/{element_id}"


def _place_description(
    city_name: str,
    place_name: str,
    category: str,
    tags: dict[str, object],
    cuisine: list[str],
) -> str:
    if category == "restaurant":
        cuisine_text = ", ".join(cuisine[:3]) if cuisine else "local food"
        return f"{place_name} is a {category} listing in {city_name} associated with {cuisine_text}."
    if category == "park":
        return f"{place_name} is a park or outdoor space in {city_name}."
    if category == "trail":
        return f"{place_name} is a trail or trailhead associated with outdoor recreation in {city_name}."
    if category == "beach":
        return f"{place_name} is a beach or waterfront natural feature in {city_name}."
    if category == "hotel":
        return f"{place_name} is a lodging option recorded for {city_name}."
    if category == "viewpoint":
        return f"{place_name} is a scenic viewpoint or overlook in {city_name}."
    if category == "museum":
        return f"{place_name} is a museum or gallery listing in {city_name}."
    if category == "zoo":
        return f"{place_name} is a zoo or aquarium listing in {city_name}."
    if category == "venue":
        return f"{place_name} is a venue for performances, films, events, or civic culture in {city_name}."
    if category == "market":
        return f"{place_name} is a market or marketplace listing in {city_name}."
    if category == "equestrian":
        return f"{place_name} is an equestrian or horse-riding related place in or near {city_name}."
    return f"{place_name} is a transit or attraction record in {city_name}."
