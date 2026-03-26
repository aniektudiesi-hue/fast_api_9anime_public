import asyncio
import time
import logging
import hashlib
from typing import List, Dict, Any, Optional
from urllib.parse import quote
from collections import OrderedDict, defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from httpx import AsyncClient, Timeout, Limits, HTTPStatusError
from selectolax.lexbor import LexborHTMLParser
import orjson

# =========================
# 🔹 LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("ro-anime-api")


# =========================
# 🔹 ORJSON RESPONSE
# =========================
class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)


# =========================
# 🔹 LRU + TTL CACHE
# =========================
class LRUCache:
    """Thread-safe LRU cache with per-entry TTL and hit/miss stats."""

    def __init__(self, ttl: int = 300, max_size: int = 1000):
        self.cache: OrderedDict = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            value, ts = self.cache.pop(key)
            if time.monotonic() - ts < self.ttl:
                self.cache[key] = (value, ts)   # move to end (most recent)
                self.hits += 1
                return value
            # expired — don't re-insert
        self.misses += 1
        return None

    def set(self, key: str, value: Any) -> None:
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)      # evict oldest
        self.cache[key] = (value, time.monotonic())

    def delete(self, key: str) -> None:
        self.cache.pop(key, None)

    def clear(self) -> None:
        self.cache.clear()

    @property
    def size(self) -> int:
        return len(self.cache)

    def stats(self) -> Dict:
        total = self.hits + self.misses
        return {
            "size": self.size,
            "max_size": self.max_size,
            "ttl_seconds": self.ttl,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(self.hits / total, 3) if total else 0.0,
        }


# =========================
# 🔹 RATE LIMITER (sliding window)
# =========================
class SlidingWindowRateLimiter:
    """Per-IP sliding window rate limiter — more accurate than simple counters."""

    def __init__(self, max_requests: int = 200, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self._buckets: Dict[str, List[float]] = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now = time.monotonic()
        window_start = now - self.window
        timestamps = self._buckets[ip]

        # Purge old entries
        self._buckets[ip] = [t for t in timestamps if t > window_start]

        if len(self._buckets[ip]) >= self.max_requests:
            return False

        self._buckets[ip].append(now)
        return True

    def cleanup(self) -> None:
        """Drop IPs with no recent activity (call periodically)."""
        now = time.monotonic()
        cutoff = now - self.window
        stale = [ip for ip, ts in self._buckets.items() if not any(t > cutoff for t in ts)]
        for ip in stale:
            del self._buckets[ip]


# =========================
# 🔹 CACHE & LIMITER INSTANCES
# =========================
search_cache  = LRUCache(ttl=600,  max_size=1000)
episode_cache = LRUCache(ttl=300,  max_size=500)
suggest_cache = LRUCache(ttl=600,  max_size=1000)
home_cache    = LRUCache(ttl=180,  max_size=10)   # home changes frequently
stream_cache  = LRUCache(ttl=120,  max_size=500)

rate_limiter = SlidingWindowRateLimiter(max_requests=200, window_seconds=60)

BASE_URL = "https://9animetv.to"


# =========================
# 🔹 HTTP CLIENT (singleton)
# =========================
_client: Optional[AsyncClient] = None

def get_client() -> AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = AsyncClient(
            http2=True,
            timeout=Timeout(8.0, connect=3.0),
            limits=Limits(max_connections=300, max_keepalive_connections=80),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": BASE_URL,
            },
            follow_redirects=True,
        )
    return _client


# =========================
# 🔹 LIFESPAN (replaces deprecated on_event)
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    logger.info("Starting up — warming cache...")
    asyncio.create_task(warm_cache())
    asyncio.create_task(periodic_cleanup())
    yield
    # ── Shutdown ──
    logger.info("Shutting down — closing HTTP client...")
    client = get_client()
    await client.aclose()


# =========================
# 🔹 APP
# =========================
app = FastAPI(
    title="RO-Anime API",
    version="5.0.0",
    description="High-performance async scraper for 9AnimeTV",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=512)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],       # this API is read-only
    allow_headers=["*"],
)


# =========================
# 🔹 MIDDLEWARE
# =========================
@app.middleware("http")
async def request_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"

    # Rate limit
    if not rate_limiter.is_allowed(ip):
        return ORJSONResponse(
            {"error": "Too many requests", "retry_after": 60},
            status_code=429,
            headers={"Retry-After": "60"},
        )

    start = time.perf_counter()
    response = await call_next(request)
    ms = (time.perf_counter() - start) * 1000

    response.headers["X-Process-Time-Ms"] = f"{ms:.2f}"
    response.headers["X-Powered-By"] = "RO-Anime/5.0"
    return response


# =========================
# 🔹 FETCH HELPERS
# =========================
async def fetch_html(url: str, retries: int = 2) -> str:
    client = get_client()
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
        except HTTPStatusError as e:
            logger.warning(f"HTTP {e.response.status_code} for {url}")
            if e.response.status_code in (403, 404):
                raise HTTPException(e.response.status_code, "Upstream returned error")
            if attempt == retries:
                raise HTTPException(502, "Upstream unavailable")
        except Exception as e:
            if attempt == retries:
                logger.error(f"Fetch failed ({url}): {e}")
                raise HTTPException(502, "Upstream unavailable")
        await asyncio.sleep(0.15 * (2 ** attempt))


async def fetch_json(url: str, retries: int = 2) -> Any:
    client = get_client()
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
        except HTTPStatusError as e:
            if e.response.status_code in (403, 404):
                raise HTTPException(e.response.status_code, "Upstream returned error")
            if attempt == retries:
                raise HTTPException(502, "Upstream unavailable")
        except Exception as e:
            if attempt == retries:
                logger.error(f"JSON fetch failed ({url}): {e}")
                raise HTTPException(502, "Upstream unavailable")
        await asyncio.sleep(0.15 * (2 ** attempt))


# =========================
# 🔹 PARSERS / BUSINESS LOGIC
# =========================
def _parse_search_results(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    results = []
    for item in parser.css("div.flw-item.item-qtip"):
        img = item.css_first("img")
        if not img:
            continue
        attrs = img.attributes
        title = attrs.get("alt", "").strip()
        poster = attrs.get("data-src") or attrs.get("src") or ""
        anime_id = item.attributes.get("data-id", "")
        if title and anime_id:
            results.append({"anime_id": anime_id, "poster": poster, "title": title})
    return results


def _parse_episodes(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    episodes = []
    for ep in parser.css("a.item.ep-item"):
        attrs = ep.attributes
        ep_num   = attrs.get("data-number", "")
        ep_id    = attrs.get("data-id", "")
        ep_title = attrs.get("title", f"Episode {ep_num}").strip()
        if ep_num and ep_id:
            episodes.append({
                "episode_number": ep_num,
                "episode_id": ep_id,
                "title": ep_title,
            })
    return episodes


def _parse_home(html: str) -> Dict:
    parser = LexborHTMLParser(html)
    thumbnails, hero_thumbnails = [], []

    for img in parser.css("img.film-poster-img"):
        attrs = img.attributes
        if attrs.get("data-src"):
            thumbnails.append(attrs["data-src"])
        if attrs.get("src"):
            hero_thumbnails.append(attrs["src"])

    # Also grab featured/trending titles from the home page
    trending = []
    for item in parser.css("div.flw-item"):
        img = item.css_first("img")
        if img:
            attrs = img.attributes
            title = attrs.get("alt", "").strip()
            poster = attrs.get("data-src") or attrs.get("src") or ""
            anime_id = item.attributes.get("data-id", "")
            if title and anime_id:
                trending.append({"anime_id": anime_id, "poster": poster, "title": title})

    return {
        "thumbnails": thumbnails,
        "hero_thumbnails": hero_thumbnails,
        "trending": trending,
        "count": len(thumbnails),
    }


# =========================
# 🔹 SERVICE LAYER
# =========================
async def search_service(query: str) -> List[Dict]:
    key = f"search:{query.lower().strip()}"
    cached = search_cache.get(key)
    if cached is not None:
        return cached
    html = await fetch_html(f"{BASE_URL}/search?keyword={quote(query)}")
    results = _parse_search_results(html)
    search_cache.set(key, results)
    return results


async def episodes_service(anime_id: str) -> List[Dict]:
    cached = episode_cache.get(anime_id)
    if cached is not None:
        return cached
    data = await fetch_json(f"{BASE_URL}/ajax/episode/list/{anime_id}")
    episodes = _parse_episodes(data.get("html", ""))
    episode_cache.set(anime_id, episodes)
    return episodes


async def suggest_service(query: str) -> List:
    key = f"suggest:{query.lower().strip()}"
    cached = suggest_cache.get(key)
    if cached is not None:
        return cached
    data = await fetch_json(f"{BASE_URL}/ajax/search/suggest?keyword={quote(query)}")
    result = data.get("result", [])
    suggest_cache.set(key, result)
    return result


async def home_service() -> Dict:
    cached = home_cache.get("home")
    if cached is not None:
        return cached
    html = await fetch_html(f"{BASE_URL}/home")
    result = _parse_home(html)
    home_cache.set("home", result)
    return result


async def stream_url_service(episode_id: str, sub: str = "sub") -> Dict:
    """
    Returns the streaming embed URL for a given episode.
    sub = 'sub' | 'dub'
    """
    key = f"stream:{episode_id}:{sub}"
    cached = stream_cache.get(key)
    if cached is not None:
        return cached

    # Try to get the actual server list for this episode
    data = await fetch_json(f"{BASE_URL}/ajax/episode/servers?episodeId={episode_id}")
    servers = data.get("html", "")
    parser = LexborHTMLParser(servers)

    # Pick first available server
    server_items = parser.css("div.item.server-item")
    server_id = None
    for s in server_items:
        if sub in s.attributes.get("data-type", "").lower():
            server_id = s.attributes.get("data-id")
            break

    if not server_id:
        # Fallback to megaplay direct embed
        url = f"https://megaplay.buzz/stream/s-2/{episode_id}/{sub}"
        result = {"url": url, "source": "fallback"}
        stream_cache.set(key, result)
        return result

    # Get actual source URL from server
    src_data = await fetch_json(f"{BASE_URL}/ajax/episode/sources?id={server_id}")
    url = src_data.get("link", f"https://megaplay.buzz/stream/s-2/{episode_id}/{sub}")
    result = {"url": url, "source": "9anime", "server_id": server_id}
    stream_cache.set(key, result)
    return result


# =========================
# 🔹 ROUTES
# =========================
@app.get("/search/{query}", summary="Search anime by keyword")
async def search(query: str):
    if len(query.strip()) < 1:
        raise HTTPException(400, "Query must not be empty")
    results = await search_service(query)
    return {"results": results, "count": len(results)}


@app.get("/suggest/{query}", summary="Autocomplete suggestions")
async def suggest(query: str):
    if len(query.strip()) < 2:
        return {"results": []}
    results = await suggest_service(query)
    return {"results": results}


@app.get("/anime/episode/{anime_id}", summary="Get episode list for an anime")
async def episodes(anime_id: str):
    eps = await episodes_service(anime_id)
    return {"episodes": eps, "count": len(eps)}


@app.get("/anime/stream/{episode_id}", summary="Get stream URL for an episode")
async def stream(
    episode_id: str,
    type: str = Query(default="sub", pattern="^(sub|dub)$")
):
    result = await stream_url_service(episode_id, type)
    return result


@app.get("/home/thumbnails", summary="Home page thumbnails and trending")
async def home():
    return await home_service()


@app.get("/cache/stats", summary="Cache hit/miss statistics")
async def cache_stats():
    return {
        "search":  search_cache.stats(),
        "episode": episode_cache.stats(),
        "suggest": suggest_cache.stats(),
        "home":    home_cache.stats(),
        "stream":  stream_cache.stats(),
    }


@app.delete("/cache/flush", summary="Flush all caches (use carefully)")
async def flush_cache():
    for c in (search_cache, episode_cache, suggest_cache, home_cache, stream_cache):
        c.clear()
    return {"status": "flushed"}


@app.get("/health", summary="Health check")
async def health():
    client = get_client()
    return {
        "status": "ok",
        "version": "5.0.0",
        "client_closed": client.is_closed,
        "cache_sizes": {
            "search":  search_cache.size,
            "episode": episode_cache.size,
            "suggest": suggest_cache.size,
            "home":    home_cache.size,
            "stream":  stream_cache.size,
        },
    }


# =========================
# 🔹 BACKGROUND TASKS
# =========================
async def warm_cache():
    """Pre-warm caches with popular queries so first real user gets a cache hit."""
    titles = ["Naruto", "One Piece", "Bleach", "Jujutsu Kaisen", "Demon Slayer"]
    tasks = []
    for t in titles:
        tasks.append(search_service(t))
        tasks.append(suggest_service(t))
    tasks.append(home_service())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = sum(1 for r in results if isinstance(r, Exception))
    logger.info(f"Cache warm complete — {len(tasks) - errors}/{len(tasks)} succeeded")


async def periodic_cleanup():
    """Runs every 5 minutes to purge stale rate-limiter buckets."""
    while True:
        await asyncio.sleep(300)
        rate_limiter.cleanup()
        logger.debug("Rate limiter cleanup done")


# =========================
# 🔹 ENTRY POINT
# =========================
if __name__ == "__main__":
    import uvicorn

    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("Using uvloop event loop")
    except ImportError:
        logger.info("uvloop not found — using default asyncio loop")

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1,           # single worker for shared in-memory caches
        log_level="info",
        access_log=True,
    )
