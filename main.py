import asyncio
import time
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from httpx import AsyncClient, Timeout, Limits
from selectolax.lexbor import LexborHTMLParser

# =========================
# 🔹 CONFIGURATION & LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fast-api-scraper")

app = FastAPI(
    title="Advanced 9Anime Scraper",
    description="High-performance scraping API with sub-second latency targets.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# 🔹 INFRASTRUCTURE (GLOBAL CLIENT)
# =========================
# Advanced HTTP Client with HTTP/2, Connection Pooling, and Brotli support
client = AsyncClient(
    http2=True,
    timeout=Timeout(5.0, connect=2.0),
    limits=Limits(max_connections=100, max_keepalive_connections=20),
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
)

BASE_URL_9ANIME = "https://9animetv.to"

# =========================
# 🔹 IN-MEMORY CACHE (LRU-LIKE)
# =========================
class SimpleCache:
    def __init__(self, ttl: int = 300):
        self.cache = {}
        self.ttl = ttl

    def get(self, key: str):
        if key in self.cache:
            val, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return val
            else:
                del self.cache[key]
        return None

    def set(self, key: str, value: Any):
        self.cache[key] = (value, time.time())

# Initialize caches for different endpoints
search_cache = SimpleCache(ttl=600)  # 10 mins
episode_cache = SimpleCache(ttl=300) # 5 mins

# =========================
# 🔹 MIDDLEWARE FOR LATENCY TRACKING
# =========================
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_time:.2f}"
    return response

# =========================
# 🔹 SERVICE FUNCTIONS (OPTIMIZED)
# =========================

async def fetch_with_retry(url: str, is_json: bool = False, retries: int = 1):
    """Fetch with HTTP/2 and minimal retries for stability"""
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json() if is_json else resp.text
            if resp.status_code == 404:
                return None
        except Exception as e:
            if attempt == retries:
                logger.error(f"Failed to fetch {url}: {str(e)}")
                raise HTTPException(status_code=502, detail="Upstream service unavailable")
            await asyncio.sleep(0.1 * (attempt + 1))
    return None

async def search_anime_logic(query: str) -> List[Dict[str, str]]:
    cached = search_cache.get(query)
    if cached: return cached

    encoded_query = quote(query.strip())
    url = f"{BASE_URL_9ANIME}/search?keyword={encoded_query}"
    
    html = await fetch_with_retry(url)
    if not html: return []

    # Using Lexbor (C-based parser) for ultra-fast HTML parsing
    parser = LexborHTMLParser(html)
    items = parser.css("div.flw-item.item-qtip")
    
    results = []
    for item in items:
        img = item.css_first("img")
        if img:
            results.append({
                "anime_id": item.attributes.get("data-id"),
                "poster": img.attributes.get("data-src") or img.attributes.get("src"),
                "title": img.attributes.get("alt")
            })
    
    search_cache.set(query, results)
    return results

async def get_episodes_logic(anime_id: str) -> List[Dict[str, str]]:
    cached = episode_cache.get(anime_id)
    if cached: return cached

    url = f"{BASE_URL_9ANIME}/ajax/episode/list/{anime_id}"
    data = await fetch_with_retry(url, is_json=True)
    
    if not data or "html" not in data:
        return []

    parser = LexborHTMLParser(data["html"])
    episodes = parser.css("a.item.ep-item")
    
    results = []
    for ep in episodes:
        results.append({
            "episode_number": ep.attributes.get("data-number"),
            "episode_id": ep.attributes.get("data-id"),
            "title": ep.attributes.get("title")
        })
    
    episode_cache.set(anime_id, results)
    return results

# =========================
# 🔹 ROUTES
# =========================

@app.get("/search/{query}")
async def search(query: str):
    start_time = time.perf_counter()
    results = await search_anime_logic(query)
    latency_ms = (time.perf_counter() - start_time) * 1000
    
    return {
        "status": "success",
        "latency_ms": f"{latency_ms:.2f}",
        "results_found": len(results),
        "results": results
    }

@app.get("/anime/episode/{anime_id}")
async def get_episodes(anime_id: str):
    start_time = time.perf_counter()
    data = await get_episodes_logic(anime_id)
    latency_ms = (time.perf_counter() - start_time) * 1000
    
    if not data:
        raise HTTPException(status_code=404, detail="No episodes found")
        
    return {
        "status": "success",
        "latency_ms": f"{latency_ms:.2f}",
        "episode_count": len(data),
        "episode_data": data
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

# =========================
# 🔹 LIFECYCLE
# =========================

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
