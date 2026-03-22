from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from urllib.parse import quote
from bs4 import BeautifulSoup
from httpx import AsyncClient, Timeout
import asyncio

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev ke liye
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔥 Global HTTP Client (connection pooling = FAST)
client = AsyncClient(
    timeout=Timeout(10.0),
    headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }
)

BASE_URL = "https://9animetv.to"

# =========================
# 🔹 SERVICE FUNCTIONS
# =========================

async def fetch_html(url: str):
    """Reusable fetch function with error handling"""
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")


async def fetch_json(url: str):
    """Reusable JSON fetch"""
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"JSON fetch failed: {str(e)}")


async def search_anime_service(query: str):
    query = quote(query.strip())
    url = f"{BASE_URL}/search?keyword={query}"

    html = await fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    container = soup.find_all("div", class_="flw-item item-qtip")

    if not container:
        return []

    results = []
    for item in container:
        try:
            img = item.find("img")
            results.append({
                "anime_id": item.get("data-id"),
                "poster": img.get("data-src"),
                "title": img.get("alt")
            })
        except Exception:
            continue  # skip broken elements

    return results


async def get_episode_service(anime_id: int):
    url = f"{BASE_URL}/ajax/episode/list/{anime_id}"

    data = await fetch_json(url)
    html = data.get("html")

    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    episodes = soup.find_all("a", class_="item ep-item")

    result = []
    for ep in episodes:
        try:
            result.append({
                "episode_number": ep.get("data-number"),
                "episode_id": ep.get("data-id"),
            })
        except Exception:
            continue

    return result


# =========================
# 🔹 ROUTES
# =========================

@app.get("/search/{query}")
async def search(query: str):
    results = await search_anime_service(query)

    if not results:
        raise HTTPException(status_code=404, detail="No results found")

    return {
        "results_found": len(results),
        "results": results
    }


@app.get("/anime/episode/{anime_id}")
async def get_episodes(anime_id: int):
    data = await get_episode_service(anime_id)

    if not data:
        raise HTTPException(status_code=404, detail="No episodes found")

    return {"episode_data": data}


# =========================
# 🔹 CLEANUP (IMPORTANT)
# =========================

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()