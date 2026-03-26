import asyncio
import time
import logging
from typing import List, Dict, Any
from urllib.parse import quote
from collections import OrderedDict, defaultdict

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from httpx import AsyncClient, Timeout, Limits
from selectolax.lexbor import LexborHTMLParser
import orjson

# =========================
# 🔹 CONFIGURATION & LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fast-api-scraper")

class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_INDENT_2)

app = FastAPI(
    title="Hyper-Optimized 9Anime Scraper",
    version="4.0.0",
    default_response_class=ORJSONResponse
)

# =========================
# 🔹 MIDDLEWARE
# =========================
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

request_counts = defaultdict(int)

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    ip = request.client.host
    request_counts[ip] += 1

    if request_counts[ip] > 200:
        return JSONResponse({"error": "Too many requests"}, status_code=429)

    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_time:.3f}"
    return response

# =========================
# 🔹 HTTP CLIENT
# =========================
client = AsyncClient(
    http2=True,
    timeout=Timeout(5.0, connect=2.0),
    limits=Limits(max_connections=200, max_keepalive_connections=50),
    headers={
        "User-Agent": "Mozilla/5.0",
        "Accept-Encoding": "gzip, deflate, br",
    }
)

BASE_URL_9ANIME = "https://9animetv.to"

# =========================
# 🔹 CACHE (LRU + TTL)
# =========================
class LRUCache:
    def __init__(self, ttl=300, max_size=1000):
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size

    def get(self, key):
        if key in self.cache:
            value, ts = self.cache.pop(key)
            if time.monotonic() - ts < self.ttl:
                self.cache[key] = (value, ts)
                return value
        return None

    def set(self, key, value):
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[key] = (value, time.monotonic())

search_cache = LRUCache(600)
episode_cache = LRUCache(300)
suggest_cache = LRUCache(600)
home_cache = LRUCache(300)

# =========================
# 🔹 FETCH WITH RETRY
# =========================
async def fetch_with_retry(url: str, is_json=False, retries=2):
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json() if is_json else resp.text
        except Exception as e:
            if attempt == retries:
                logger.error(f"Fetch failed {url}: {e}")
                raise HTTPException(502, "Upstream error")
            await asyncio.sleep(0.1 * (2 ** attempt))
    return None

# =========================
# 🔹 SEARCH
# =========================
async def search_anime_logic(query: str):
    cached = search_cache.get(query)
    if cached:
        return cached

    url = f"{BASE_URL_9ANIME}/search?keyword={quote(query)}"
    html = await fetch_with_retry(url)

    parser = LexborHTMLParser(html)
    items = parser.css("div.flw-item.item-qtip")

    results = []
    for item in items:
        img = item.css_first("img")
        if img:
            attrs = img.attributes
            results.append({
                "anime_id": item.attributes.get("data-id"),
                "poster": attrs.get("data-src") or attrs.get("src"),
                "title": attrs.get("alt")
            })

    search_cache.set(query, results)
    return results

# =========================
# 🔹 EPISODES
# =========================
async def get_episodes_logic(anime_id: str):
    cached = episode_cache.get(anime_id)
    if cached:
        return cached

    url = f"{BASE_URL_9ANIME}/ajax/episode/list/{anime_id}"
    data = await fetch_with_retry(url, is_json=True)

    parser = LexborHTMLParser(data["html"])
    items = parser.css("a.item.ep-item")

    results = []
    for ep in items:
        attrs = ep.attributes
        results.append({
            "episode_number": attrs.get("data-number"),
            "episode_id": attrs.get("data-id"),
            "title": attrs.get("title")
        })

    episode_cache.set(anime_id, results)
    return results

# =========================
# 🔹 SUGGESTIONS
# =========================
async def get_suggestions_logic(query: str):
    cached = suggest_cache.get(query)
    if cached:
        return cached

    url = f"{BASE_URL_9ANIME}/ajax/search/suggest?keyword={quote(query)}"
    data = await fetch_with_retry(url, is_json=True)

    result = data.get("result", [])
    suggest_cache.set(query, result)
    return result

# =========================
# 🔹 HOME THUMBNAILS (YOUR LOGIC UPGRADED)
# =========================
async def get_home_thumbnails_logic():
    cached = home_cache.get("home")
    if cached:
        return cached

    html = await fetch_with_retry(f"{BASE_URL_9ANIME}/home")
    parser = LexborHTMLParser(html)

    images = parser.css("img.film-poster-img")

    thumbnails = []
    hero_thumbnails = []

    for img in images:
        attrs = img.attributes
        if attrs.get("data-src"):
            thumbnails.append(attrs.get("data-src"))
        if attrs.get("src"):
            hero_thumbnails.append(attrs.get("src"))

    result = {
        "thumbnails": thumbnails,
        "hero_thumbnails": hero_thumbnails,
        "count": len(thumbnails)
    }

    home_cache.set("home", result)
    return result

# =========================
# 🔹 ROUTES
# =========================
@app.get("/search/{query}")
async def search(query: str):
    return {"results": await search_anime_logic(query)}

@app.get("/suggest/{query}")
async def suggest(query: str):
    return {"results": await get_suggestions_logic(query)}

@app.get("/anime/episode/{anime_id}")
async def episodes(anime_id: str):
    return {"episodes": await get_episodes_logic(anime_id)}

@app.get("/home/thumbnails")
async def home():
    return await get_home_thumbnails_logic()

@app.get("/health")
async def health():
    return {"status": "ok"}

# =========================
# 🔹 BACKGROUND WARMING
# =========================
async def warm_cache():
    titles = ["Naruto", "One Piece", "Bleach"]
    tasks = []

    for t in titles:
        tasks.append(search_anime_logic(t))
        tasks.append(get_suggestions_logic(t))

    tasks.append(get_home_thumbnails_logic())

    await asyncio.gather(*tasks, return_exceptions=True)

# =========================
# 🔹 LIFECYCLE
# =========================
@app.on_event("startup")
async def startup():
    asyncio.create_task(warm_cache())

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()

# =========================
# 🔹 RUN
# =========================
if __name__ == "__main__":
    import uvloop
    import uvicorn

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    uvicorn.run(app, host="0.0.0.0", port=8000)
