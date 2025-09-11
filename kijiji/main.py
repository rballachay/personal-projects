import os
import asyncio
from typing import List, Dict
import pandas as pd
import torch 

from kijiji.graphql_query import (
    get_place_id_for_city_async,
    get_location_id_async,
    fetch_kijiji_listings_async
)

from kijiji.db_connector import ListingManager, EmbeddingManager
from kijiji.embed_desc import EmbeddingTransformer
from kijiji.config import load_config


async def fetch_city_listings(
    city: str,
    keyword: str,
    radius: int,
    category: int,
    google_places_api_key: str,
    kijiji_graphql_url: str
) -> List[Dict]:
    """Fetch listings for a given city."""
    display_name, google_place_id, location = await get_place_id_for_city_async(
        city, google_places_api_key
    )
    location_id = await get_location_id_async(google_place_id, kijiji_graphql_url)

    listings = await fetch_kijiji_listings_async(
        keyword,
        category,
        location_id,
        location["latitude"],
        location["longitude"],
        radius,
        display_name,
        kijiji_graphql_url,
    )
    return listings


async def main():
    config = load_config('kijiji/config.json')
    google_places_api_key = os.getenv("GOOGLE_PLACES_API_KEY")

    # Managers
    listing_mgr = ListingManager(config["DB_PATH"])
    embedding_mgr = EmbeddingManager(config["DB_PATH"])
    embedder = EmbeddingTransformer(device="cuda" if torch.cuda.is_available() else "cpu")

    # Config
    cities = config["CITIES"]
    keyword = config["DEFAULT_KEYWORD"]
    category = config["DEFAULT_CATEGORY"]
    radius = config["DEFAULT_RADIUS"]
    kijiji_graphql_url = config["KIJIJI_GRAPHQL_URL"]

    for city in cities:
        print(f"Fetching listings for {city}...")
        listings = await fetch_city_listings(
            city, keyword, radius, category, google_places_api_key, kijiji_graphql_url
        )
        listings = listings.to_dict(orient="records") if isinstance(listings, pd.DataFrame) else listings

        if listings:
            listing_mgr.insert_many(listings)
            print(f"Inserted {len(listings)} listings for {city}.")
        else:
            print(f"No listings found for {city}.")

    # --- Embedding step ---
    all_listings = listing_mgr.get_all_listings()
    print(f"Processing embeddings for {len(all_listings)} listings...")

    # Filter out listings that already have embeddings
    existing_embeddings = {eid for eid, _ in embedding_mgr.get_all_embeddings()}
    listings_to_embed = [
        l for l in all_listings if l["id"] not in existing_embeddings and l.get("description")
    ]

    if listings_to_embed:
        texts = [l["description"] for l in listings_to_embed]
        ids = [l["id"] for l in listings_to_embed]

        embeddings = embedder.encode_texts(texts, batch_size=32)
        id_emb_pairs = list(zip(ids, embeddings))
        embedding_mgr.insert_many(id_emb_pairs)

        print(f"Created embeddings for {len(id_emb_pairs)} listings.")
    else:
        print("All listings already have embeddings.")


if __name__ == "__main__":
    asyncio.run(main())
