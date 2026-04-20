from __future__ import annotations

TOPICS = [
    "orientation",
    "activities",
    "food",
    "lodging",
    "transit",
    "timing",
    "safety",
    "etiquette",
    "budget",
    "family",
    "nightlife",
    "outdoors",
    "hidden_gems",
    "local_customs",
    "newcomer_advice",
    "neighborhood_vibe",
    "scenic",
    "local_opinion",
]


WIKIVOYAGE_TOPIC_MAP = {
    "understand": "orientation",
    "history": "orientation",
    "climate": "timing",
    "get in": "transit",
    "get around": "transit",
    "see": "activities",
    "do": "activities",
    "buy": "budget",
    "eat": "food",
    "drink": "food",
    "sleep": "lodging",
    "stay safe": "safety",
    "stay healthy": "safety",
    "respect": "etiquette",
    "cope": "etiquette",
    "connect": "orientation",
    "go next": "orientation",
}


REDDIT_TOPIC_QUERIES = {
    "activities": [
        "hidden gems",
        "sunset spot",
        "things to do",
        "weekend itinerary",
        "best hikes",
        "day trips",
        "horse riding",
        "favorite museums",
    ],
    "hidden_gems": [
        "hidden gems",
        "underrated spots",
        "locals only places",
        "secret spots",
        "non touristy things to do",
    ],
    "food": ["best tacos", "best local food", "restaurants locals recommend", "coffee shops", "brunch spots"],
    "lodging": ["where to stay", "neighborhood to stay", "hotel area"],
    "transit": ["public transit", "getting around", "parking advice", "airport transfer"],
    "safety": ["safety tips", "what to avoid", "moving advice"],
    "etiquette": ["local norms", "things visitors should know", "what is considered rude", "customs"],
    "local_customs": ["local customs", "unwritten rules", "social norms", "gift etiquette", "visitor etiquette"],
    "newcomer_advice": ["just moved here", "things to know before moving", "new resident advice", "new in town"],
    "neighborhood_vibe": ["best neighborhoods for", "neighborhood vibe", "quiet neighborhood", "walkable areas"],
    "nightlife": ["best bars", "nightlife", "late night spots", "music venues"],
    "outdoors": ["best hikes", "parks locals love", "scenic drives", "outdoor weekend plans"],
    "local_opinion": ["locals recommend", "what locals actually do", "tourist trap or worth it"],
}


PLACE_CATEGORY_QUERIES = {
    "restaurant": ["restaurant", "food", "eat", "tacos", "pizza", "coffee", "brunch"],
    "park": ["park", "garden", "green space", "picnic", "playground", "dog park"],
    "museum": ["museum", "gallery", "art", "exhibit"],
    "viewpoint": ["viewpoint", "overlook", "sunset", "skyline", "scenic spot", "photo spot"],
    "trail": ["trail", "trailhead", "hike", "hiking", "walk", "nature walk", "scenic walk"],
    "beach": ["beach", "shore", "coast", "waterfront"],
    "zoo": ["zoo", "aquarium", "animals"],
    "venue": ["theater", "theatre", "cinema", "concert", "show", "music venue", "live music", "club", "bar", "cocktail", "brewery"],
    "market": ["market", "marketplace", "farmer market", "shopping", "bazaar", "food hall"],
    "equestrian": ["horse riding", "horseback", "equestrian", "stable", "stables", "trail ride"],
    "hotel": ["hotel", "stay", "lodging"],
    "transit": ["train", "station", "transit", "bus"],
    "attraction": ["attraction", "landmark", "tourist spot", "must see"],
}


ACTIVITY_TYPE_QUERIES = {
    "food_drink": ["food", "eat", "restaurant", "bar", "coffee", "brunch", "tacos", "cocktail"],
    "outdoors": ["hike", "trail", "park", "outdoor", "beach", "nature", "picnic", "camping"],
    "nightlife": ["nightlife", "bar", "club", "late night", "live music", "after dark", "cocktail", "brewery"],
    "arts_culture": ["museum", "gallery", "art", "theater", "theatre", "concert", "show", "culture"],
    "family": ["family", "kid", "kids", "children", "stroller", "playground"],
    "shopping_markets": ["shopping", "market", "boutique", "farmer market", "mall"],
    "wellness": ["spa", "wellness", "yoga", "relaxing", "sauna"],
    "sports_recreation": ["sports", "recreation", "climbing", "pickleball", "horse riding", "equestrian"],
    "scenic": ["sunset", "viewpoint", "overlook", "scenic", "photo spot", "skyline"],
    "day_trip": ["day trip", "weekend drive", "road trip", "under 2 hours", "within 2 hours"],
    "hidden_gems": ["hidden gem", "underrated", "locals only", "secret spot", "non touristy"],
    "newcomer_advice": ["just moved", "new here", "newcomer", "before moving", "settling in"],
    "local_customs": ["etiquette", "custom", "norm", "unwritten rule", "considered rude", "gift etiquette"],
    "neighborhood_vibe": ["neighborhood", "walkable", "quiet", "lively", "good area", "vibe"],
}
