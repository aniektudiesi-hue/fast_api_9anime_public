import asyncio
import time
import logging
import random
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
# 🔹 BROWSER FINGERPRINT PROFILES
# Real Chrome/Edge/Firefox UA + matching sec-ch-ua headers
# Rotating these prevents fingerprint-based blocks
# =========================
BROWSER_PROFILES = [
    {
        # Chrome 124 on Windows
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        # Chrome 123 on macOS
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    },
    {
        # Edge 124 on Windows
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
        ),
        "sec-ch-ua": '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        # Firefox 125 on Windows
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
            "Gecko/20100101 Firefox/125.0"
        ),
        "sec-ch-ua": "",          # Firefox doesn't send sec-ch-ua
        "sec-ch-ua-mobile": "",
        "sec-ch-ua-platform": "",
    },
    {
        # Chrome 122 on Linux
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "sec-ch-ua": '"Chromium";v="122", "Google Chrome";v="122", "Not(A:Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    },
]

# Static headers that never change (full browser simulation)
STATIC_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "DNT": "1",
}

STATIC_HEADERS_JSON = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "X-Requested-With": "XMLHttpRequest",
    "DNT": "1",
}


def _build_headers(for_json: bool = False, profile_idx: Optional[int] = None) -> Dict[str, str]:
    """Assemble full browser-grade headers with a chosen (or random) UA profile."""
    idx = profile_idx if profile_idx is not None else random.randint(0, len(BROWSER_PROFILES) - 1)
    profile = BROWSER_PROFILES[idx]
    base = STATIC_HEADERS_JSON.copy() if for_json else STATIC_HEADERS.copy()
    base["User-Agent"] = profile["User-Agent"]
    # Only add sec-ch-ua headers when non-empty (Firefox omits them)
    for k in ("sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"):
        if profile.get(k):
            base[k] = profile[k]
    return base


# =========================
# 🔹 LRU + TTL CACHE
# =========================
class LRUCache:
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
                self.cache[key] = (value, ts)
                self.hits += 1
                return value
        self.misses += 1
        return None

    def set(self, key: str, value: Any) -> None:
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
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
# 🔹 RATE LIMITER
# =========================
class SlidingWindowRateLimiter:
    def __init__(self, max_requests: int = 200, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self._buckets: Dict[str, List[float]] = defaultdict(list)

    def is_allowed(self, ip: str) -> bool:
        now = time.monotonic()
        window_start = now - self.window
        self._buckets[ip] = [t for t in self._buckets[ip] if t > window_start]
        if len(self._buckets[ip]) >= self.max_requests:
            return False
        self._buckets[ip].append(now)
        return True

    def cleanup(self) -> None:
        now = time.monotonic()
        cutoff = now - self.window
        stale = [ip for ip, ts in self._buckets.items() if not any(t > cutoff for t in ts)]
        for ip in stale:
            del self._buckets[ip]


# =========================
# 🔹 INSTANCES
# =========================
search_cache  = LRUCache(ttl=600,  max_size=1000)
episode_cache = LRUCache(ttl=300,  max_size=500)
suggest_cache = LRUCache(ttl=600,  max_size=1000)
home_cache    = LRUCache(ttl=180,  max_size=10)
stream_cache  = LRUCache(ttl=120,  max_size=500)

rate_limiter = SlidingWindowRateLimiter(max_requests=200, window_seconds=60)

BASE_URL = "https://9animetv.to"


# =========================
# 🔹 HTTP CLIENT POOL
# Multiple clients so we can rotate per retry (different TCP fingerprint)
# =========================
_clients: List[AsyncClient] = []
_client_idx = 0

def _make_client() -> AsyncClient:
    return AsyncClient(
        http2=True,
        timeout=Timeout(12.0, connect=5.0),
        limits=Limits(max_connections=200, max_keepalive_connections=60),
        follow_redirects=True,
        verify=True,          # keep TLS verification — dropping it is suspicious
    )

def get_client(rotate: bool = False) -> AsyncClient:
    """Return a client from the pool, optionally rotating to the next one."""
    global _client_idx
    if len(_clients) < 3:
        while len(_clients) < 3:
            _clients.append(_make_client())
    if rotate:
        _client_idx = (_client_idx + 1) % len(_clients)
    client = _clients[_client_idx]
    if client.is_closed:
        _clients[_client_idx] = _make_client()
        client = _clients[_client_idx]
    return client


# =========================
# 🔹 LIFESPAN
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — warming cache...")
    asyncio.create_task(warm_cache())
    asyncio.create_task(periodic_cleanup())
    yield
    logger.info("Shutting down — closing HTTP clients...")
    for c in _clients:
        if not c.is_closed:
            await c.aclose()


# =========================
# 🔹 APP
# =========================
app = FastAPI(
    title="RO-Anime API",
    version="6.0.0",
    description="High-reliability async scraper for 9AnimeTV",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=512)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# =========================
# 🔹 MIDDLEWARE
# =========================
@app.middleware("http")
async def request_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
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
    response.headers["X-Powered-By"] = "RO-Anime/6.0"
    return response


# =========================
# 🔹 CORE FETCH — RETRY UNTIL SUCCESS
#
# Strategy:
#   • First 3 attempts: exponential backoff (0.3s → 0.6s → 1.2s) + jitter
#   • Attempts 4-6:     rotate UA profile + rotate HTTP client
#   • Attempt 7+:       longer pause (3-6s) before retrying — avoids ban
#   • 403/404:          fail immediately (no point retrying)
#   • No hard cap:      keeps trying until `max_attempts` (default 15)
#     which practically = ~100% success for transient failures
# =========================
async def fetch_html(
    url: str,
    max_attempts: int = 15,
    referer: str = BASE_URL,
) -> str:
    last_exc = None
    for attempt in range(max_attempts):
        rotate = attempt >= 3          # rotate UA+client after 3 failures
        client = get_client(rotate=rotate)
        profile_idx = attempt % len(BROWSER_PROFILES)
        headers = _build_headers(for_json=False, profile_idx=profile_idx)
        headers["Referer"] = referer

        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            if attempt > 0:
                logger.info(f"✅ fetch_html succeeded on attempt {attempt + 1}: {url}")
            return resp.text

        except HTTPStatusError as e:
            status = e.response.status_code
            logger.warning(f"HTTP {status} on attempt {attempt + 1} for {url}")
            if status in (403, 404):
                raise HTTPException(status, "Upstream returned error")
            last_exc = e

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed ({url}): {type(e).__name__}: {e}")
            last_exc = e

        # ── Backoff ──
        if attempt < 3:
            delay = (0.3 * (2 ** attempt)) + random.uniform(0, 0.2)
        elif attempt < 7:
            delay = 1.5 + random.uniform(0, 0.5)
        else:
            delay = 3.0 + random.uniform(0, 3.0)   # long pause for stubborn blocks

        logger.info(f"Retrying in {delay:.2f}s (attempt {attempt + 2}/{max_attempts})...")
        await asyncio.sleep(delay)

    logger.error(f"All {max_attempts} attempts failed for {url}: {last_exc}")
    raise HTTPException(502, f"Upstream unavailable after {max_attempts} attempts")


async def fetch_json(
    url: str,
    max_attempts: int = 15,
    referer: str = BASE_URL,
) -> Any:
    last_exc = None
    for attempt in range(max_attempts):
        rotate = attempt >= 3
        client = get_client(rotate=rotate)
        profile_idx = attempt % len(BROWSER_PROFILES)
        headers = _build_headers(for_json=True, profile_idx=profile_idx)
        headers["Referer"] = referer

        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            if attempt > 0:
                logger.info(f"✅ fetch_json succeeded on attempt {attempt + 1}: {url}")
            return resp.json()

        except HTTPStatusError as e:
            status = e.response.status_code
            logger.warning(f"HTTP {status} on attempt {attempt + 1} for {url}")
            if status in (403, 404):
                raise HTTPException(status, "Upstream returned error")
            last_exc = e

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed ({url}): {type(e).__name__}: {e}")
            last_exc = e

        if attempt < 3:
            delay = (0.3 * (2 ** attempt)) + random.uniform(0, 0.2)
        elif attempt < 7:
            delay = 1.5 + random.uniform(0, 0.5)
        else:
            delay = 3.0 + random.uniform(0, 3.0)

        logger.info(f"Retrying in {delay:.2f}s (attempt {attempt + 2}/{max_attempts})...")
        await asyncio.sleep(delay)

    logger.error(f"All {max_attempts} attempts failed for {url}: {last_exc}")
    raise HTTPException(502, f"Upstream unavailable after {max_attempts} attempts")


# =========================
# 🔹 PARSERS
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
    html = await fetch_html(
        f"{BASE_URL}/search?keyword={quote(query)}",
        referer=f"{BASE_URL}/home",
    )
    results = _parse_search_results(html)
    search_cache.set(key, results)
    return results


async def episodes_service(anime_id: str) -> List[Dict]:
    cached = episode_cache.get(anime_id)
    if cached is not None:
        return cached
    data = await fetch_json(
        f"{BASE_URL}/ajax/episode/list/{anime_id}",
        referer=f"{BASE_URL}/watch/{anime_id}",
    )
    episodes = _parse_episodes(data.get("html", ""))
    episode_cache.set(anime_id, episodes)
    return episodes


async def suggest_service(query: str) -> List:
    key = f"suggest:{query.lower().strip()}"
    cached = suggest_cache.get(key)
    if cached is not None:
        return cached
    data = await fetch_json(
        f"{BASE_URL}/ajax/search/suggest?keyword={quote(query)}",
        referer=f"{BASE_URL}/home",
    )
    result = data.get("result", [])
    suggest_cache.set(key, result)
    return result


async def home_service() -> Dict:
    cached = home_cache.get("home")
    if cached is not None:
        return cached
    html = await fetch_html(f"{BASE_URL}/home", referer=BASE_URL)
    result = _parse_home(html)
    home_cache.set("home", result)
    return result


async def stream_url_service(episode_id: str, sub: str = "sub") -> Dict:
    key = f"stream:{episode_id}:{sub}"
    cached = stream_cache.get(key)
    if cached is not None:
        return cached

    data = await fetch_json(
        f"{BASE_URL}/ajax/episode/servers?episodeId={episode_id}",
        referer=f"{BASE_URL}/watch/{episode_id}",
    )
    servers = data.get("html", "")
    parser = LexborHTMLParser(servers)

    server_id = None
    for s in parser.css("div.item.server-item"):
        if sub in s.attributes.get("data-type", "").lower():
            server_id = s.attributes.get("data-id")
            break

    if not server_id:
        url = f"https://megaplay.buzz/stream/s-2/{episode_id}/{sub}"
        result = {"url": url, "source": "fallback"}
        stream_cache.set(key, result)
        return result

    src_data = await fetch_json(
        f"{BASE_URL}/ajax/episode/sources?id={server_id}",
        referer=f"{BASE_URL}/watch/{episode_id}",
    )
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


@app.delete("/cache/flush", summary="Flush all caches")
async def flush_cache():
    for c in (search_cache, episode_cache, suggest_cache, home_cache, stream_cache):
        c.clear()
    return {"status": "flushed"}


@app.get("/health", summary="Health check")
async def health():
    return {
        "status": "ok",
        "version": "6.0.0",
        "active_clients": len([c for c in _clients if not c.is_closed]),
        "browser_profiles": len(BROWSER_PROFILES),
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
    titles = ["Naruto", "One Piece", "Bleach", "Jujutsu Kaisen", "Demon Slayer"]
    tasks = [search_service(t) for t in titles] + [suggest_service(t) for t in titles]
    tasks.append(home_service())
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = sum(1 for r in results if isinstance(r, Exception))
    logger.info(f"Cache warm complete — {len(tasks) - errors}/{len(tasks)} succeeded")


async def periodic_cleanup():
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
        workers=1,
        log_level="info",
        access_log=True,
    )
