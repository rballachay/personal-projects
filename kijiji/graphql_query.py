import json
import pandas as pd
from playwright.async_api import async_playwright
import httpx

async def get_place_id_for_city_async(city_name: str, google_places_api_key:str, country_code: str = "CA") -> str:
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": google_places_api_key,
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


async def get_location_id_async(place_id: str, kijiji_graphql_url:str) -> int:
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
            kijiji_graphql_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )

        data = await response.json()
        await browser.close()

    # Extract the location ID
    location = data.get("data", {}).get("locationFromPlace", {}).get("location", {})
    location_id = location.get("id")
    return location_id

async def fetch_kijiji_listings_async(
    keywords: str,
    category_id: int,
    location_id: int,
    latitude: float,
    longitude: float,
    radius: float,
    address: str,
    kijiji_graphql_url:str="https://www.kijiji.ca/anvil/api"
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
                kijiji_graphql_url,
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
            "imageUrls": [img for img in l.get("imageUrls", [])],
            "price": l["price"]["amount"] if l.get("price") else None,
            "location": l.get("location", {}).get("name")
        } for l in all_listings])

        return df
