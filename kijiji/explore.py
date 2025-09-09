import asyncio
import json
import pandas as pd
from playwright.async_api import async_playwright
import os
import httpx

DEFAULT_CITY = 'edmonton'
DEFAULT_KEYWORD = 'skis'
GRAPHQL_URL = "https://www.kijiji.ca/anvil/api"
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
HARDCODED_LIMIT = 500
DEFAULT_CATEGORY = 10 # this corresponds to Buy/Sell category
DEFAULT_RADIUS = 50 # this is the default in kijiji, change to what you like 

async def get_place_id_for_city_async(city_name: str, country_code: str = "CA") -> str:
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": "places.displayName,places.name,places.location"
    }
    payload = {"textQuery": f"{city_name}, {country_code}"}

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload)
        data = response.json()
        if "places" in data and len(data["places"]) > 0:
            display_name = data["places"][0].get("displayName").get("text")
            place_id = data["places"][0].get("name").replace('places/','')
            location = data["places"][0].get("location")
            return display_name, place_id, location
        else:
            raise ValueError(f"No place found for {city_name}, {country_code}")


async def get_location_id(place_id: str) -> int:
    """
    Given a Google-style place ID, returns Kijiji's internal location ID.
    """
    payload = {
        "operationName": "GetLocationFromPlace",
        "variables": {
            "placeId": place_id,
            "sessionToken":"SOME_SESSION_TOKEN"
        },
        "query": """
        query GetLocationFromPlace($placeId: String!, $sessionToken: String) {
          locationFromPlace(placeId: $placeId, sessionToken: $sessionToken) {
            location {
              id
              coordinates { latitude longitude }
              name { en_CA fr_CA }
              __typename
            }
          }
        }
        """
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()


        response = await page.request.post(
            GRAPHQL_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )

        data = await response.json()
        await browser.close()

    # Extract the location ID
    location = data.get("data", {}).get("locationFromPlace", {}).get("location", {})
    location_id = location.get("id")
    return location_id

async def get_seo_url_async(keywords: str, location_id: int, latitude: float, longitude: float, radius: float = 50, address: str = "") -> str:
    """
    Async call to Kijiji's GetSeoUrl GraphQL endpoint.
    Returns the SEO-friendly search URL.
    """
    url = "https://www.kijiji.ca/anvil/api"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }

    payload = {
        "operationName": "GetSeoUrl",
        "variables": {
            "input": {
                "searchQuery": {
                    "keywords": keywords,
                    "location": {
                        "id": location_id,
                        "area": {
                            "latitude": latitude,
                            "longitude": longitude,
                            "radius": radius,
                            "address": address
                        }
                    },
                    "view": "LIST"
                },
                "pagination": {
                    "offset": 0,
                    "limit": 40
                }
            }
        },
        "query": """
        query GetSeoUrl($input: SearchUrlInput!) {
            searchUrl(input: $input)
        }
        """
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=payload, timeout=15.0)
        data = response.json()

    try:
        seo_url = data["data"]["searchUrl"]
        return seo_url
    except KeyError:
        raise ValueError(f"No SEO URL returned for keywords={keywords}, location_id={location_id}")


async def fetch_kijiji_listings(
    keywords: str,
    category_id: int,
    location_id: int,
    latitude: float,
    longitude: float,
    radius: float,
    address: str
) -> pd.DataFrame:
    """
    Fetch listings from Kijiji using GraphQL via Playwright.

    Args:
        keywords (str): Search keywords, e.g., 'skis'.
        category_id (int): Kijiji category ID for the search.
        location_id (int): Kijiji location ID.
        latitude (float): Latitude of the search area center.
        longitude (float): Longitude of the search area center.
        radius (float): Search radius in kilometers.
        address (str): Address string used for the search.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the following columns:
            - id: Listing ID
            - title: Listing title
            - url: Listing URL
            - description: Listing description
            - imageUrls: List of image URLs
            - price: Listing price (float or None)
            - location: Listing location name
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        all_listings = []
        offset = 0
        limit = 40

        while True:
            search_payload = {
                "operationName": "SearchResultsPage",
                "variables": {
                    "by": {
                        "query": {
                            "oneListingPerUser": True,
                            "categoryId": category_id,
                            "location": {
                                "id": location_id,
                                "area": {
                                    "latitude": latitude,
                                    "longitude": longitude,
                                    "radius": radius,
                                    "address": address,
                                },
                            },
                            "keywords": keywords
                        }
                    },
                    "pagination": {"offset": offset, "limit": limit}
                },
                "query": """
                query SearchResultsPage($by: SearchResultsPageInputBy!, $pagination: PaginationInputV2!) {
                  searchResultsPage(by: $by, pagination: $pagination) {
                    results {
                      mainListings(pagination: $pagination) {
                        id
                        title
                        url
                        description
                        imageUrls
                        price {
                          ... on StandardAmountPrice { amount }
                          ... on AutosDealerAmountPrice { amount }
                          ... on AmountPrice { amount }
                        }
                        location { name }
                      }
                    }
                  }
                }
                """
            }

            resp = await page.request.post(
                "https://www.kijiji.ca/anvil/api",
                data=json.dumps(search_payload),
                headers={"Content-Type": "application/json"}
            )
            data = await resp.json()
            
            listings = data["data"]["searchResultsPage"]["results"]["mainListings"]

            if not listings:
                break

            all_listings.extend(listings)
            offset += limit

        await browser.close()

        # Convert to pandas DataFrame
        df = pd.DataFrame([{
            "id": l["id"],
            "title": l["title"],
            "url": l["url"],
            "description": l["description"],
            "imageUrls": [img["url"] for img in l.get("images", [])],
            "price": l["price"]["amount"] if l.get("price") else None,
            "location": l.get("location", {}).get("name")
        } for l in all_listings])

        return df
    
    
async def main(city=DEFAULT_CITY, keyword=DEFAULT_KEYWORD, 
               radius=DEFAULT_RADIUS, category=DEFAULT_CATEGORY):
    
    display_name, google_place_id, location = await get_place_id_for_city_async(city)
    location_id = await get_location_id(google_place_id)

    # run the retrieval, now that we have the necessary params
    listings = await fetch_kijiji_listings(keyword, category, location_id, 
                                           location['latitude'], location['longitude'], radius, display_name)
    listings.to_csv('example_listing.csv')

if __name__ == "__main__":
    asyncio.run(main())
