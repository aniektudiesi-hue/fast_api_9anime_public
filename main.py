"""
RO-Anime API v9.1 - "Unified + VOD Edition"
============================================
v9.0 fully preserved. Added:
- /stream/v2/{episode_id} — fetches sub+dub netmagcdn M3U8 from nine.mewcdn.online
- Removed all Cloud Vision API references (none existed in v9.0)
- VOD fetcher uses shared HttpClient for connection reuse & semaphore control
"""

import asyncio
import logging
import random
import time
from collections import deque, OrderedDict
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import msgspec
import uvloop
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from httpx import AsyncClient, Limits, Timeout
from selectolax.lexbor import LexborHTMLParser

# ──────────────────────────────────────────────
# CONFIGURATION & CONSTANTS
# ──────────────────────────────────────────────
BASE_URL        = "https://9animetv.to"
MEW_BASE        = "https://nine.mewcdn.online"
RAPID_CLOUD     = "https://rapid-cloud.co"
UPSTREAM_CONCURRENCY = 15
CACHE_MAX_SIZE  = 5000
PARSER_WORKERS  = 4

TTL = {
    "search": 600,
    "episode": 300,
    "suggest": 600,
    "home": 180,
    "stream": 120,
    "stream_v2": 90,   # VOD URLs rotate, keep short
}

# ──────────────────────────────────────────────
# STRUCTURED LOGGING
# ──────────────────────────────────────────────
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return msgspec.json.encode(log_record).decode()

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("ro-anime-v9")

# ──────────────────────────────────────────────
# MODELS
# ──────────────────────────────────────────────
class AnimeBase(msgspec.Struct):
    anime_id: str
    title: str
    poster: str

class Episode(msgspec.Struct):
    episode_number: str
    episode_id: str
    title: str

class StreamInfo(msgspec.Struct):
    url: str
    source: str
    server_id: Optional[str] = None

class VodSource(msgspec.Struct):
    m3u8: Optional[str]
    subtitles: List[Dict]

class VodResponse(msgspec.Struct):
    episode_id: str
    sub: Optional[VodSource]
    dub: Optional[VodSource]

# ──────────────────────────────────────────────
# BROWSER PROFILES
# ──────────────────────────────────────────────
BROWSER_PROFILES = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "sec-ch-ua": '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "sec-ch-ua": "",
        "sec-ch-ua-mobile": "",
        "sec-ch-ua-platform": "",
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="122", "Google Chrome";v="122", "Not(A:Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    },
]

# Mobile profile specifically for rapid-cloud (matches original traffic capture)
MOBILE_UA = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36"

# ──────────────────────────────────────────────
# SMART CACHE (O(1) LRU + SWR)
# ──────────────────────────────────────────────
class SmartCache:
    def __init__(self, max_size: int = 5000):
        self._store: OrderedDict[str, Tuple[Any, float, float]] = OrderedDict()
        self._max = max_size
        self.hits = self.misses = self.swr_hits = 0

    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        entry = self._store.get(key)
        if entry is None:
            self.misses += 1
            return None, False
        value, expires_at, stale_at = entry
        now = time.monotonic()
        if now > stale_at:
            self._store.pop(key, None)
            self.misses += 1
            return None, False
        self._store.move_to_end(key)
        if now > expires_at:
            self.swr_hits += 1
            return value, True
        self.hits += 1
        return value, False

    def set(self, key: str, value: Any, ttl: int) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        elif len(self._store) >= self._max:
            self._store.popitem(last=False)
        now = time.monotonic()
        self._store[key] = (value, now + ttl, now + (ttl * 2))

    def stats(self) -> Dict:
        total = self.hits + self.misses + self.swr_hits
        return {
            "size": len(self._store),
            "hits": self.hits,
            "swr_hits": self.swr_hits,
            "misses": self.misses,
            "hit_rate": round((self.hits + self.swr_hits) / total, 3) if total else 0.0,
        }

cache = SmartCache(max_size=CACHE_MAX_SIZE)

# ──────────────────────────────────────────────
# RATE LIMITER
# ──────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_req: int = 200, window: int = 60) -> None:
        self._max = max_req
        self._window = window
        self._buckets: Dict[str, deque] = {}

    def is_allowed(self, ip: str) -> bool:
        now = time.monotonic()
        cutoff = now - self._window
        if ip not in self._buckets:
            self._buckets[ip] = deque()
        dq = self._buckets[ip]
        while dq and dq[0] <= cutoff:
            dq.popleft()
        if len(dq) >= self._max:
            return False
        dq.append(now)
        return True

rate_limiter = RateLimiter()

# ──────────────────────────────────────────────
# CIRCUIT BREAKER
# ──────────────────────────────────────────────
class CircuitBreaker:
    def __init__(self, threshold: int = 10, recovery_time: int = 30):
        self.threshold = threshold
        self.recovery_time = recovery_time
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = 0

    def allow(self) -> bool:
        if self.state == "OPEN":
            if time.monotonic() - self.last_failure_time > self.recovery_time:
                self.state = "HALF-OPEN"
                return True
            return False
        return True

    def record_success(self):
        self.failures = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.monotonic()
        if self.failures >= self.threshold:
            self.state = "OPEN"
            logger.error(f"Circuit Breaker OPENED after {self.failures} failures")

breaker = CircuitBreaker()

# ──────────────────────────────────────────────
# HTTP CLIENT & CONCURRENCY
# ──────────────────────────────────────────────
class HttpClient:
    def __init__(self):
        self.client: Optional[AsyncClient] = None
        self.semaphore = asyncio.Semaphore(UPSTREAM_CONCURRENCY)
        self.inflight: Dict[str, asyncio.Event] = {}
        self.inflight_results: Dict[str, Any] = {}
        self.inflight_errors: Dict[str, Exception] = {}

    async def start(self):
        self.client = AsyncClient(
            http2=True,
            timeout=Timeout(12.0, connect=5.0),
            limits=Limits(max_connections=200, max_keepalive_connections=50),
            follow_redirects=True,
        )

    async def stop(self):
        if self.client:
            await self.client.aclose()

    async def fetch(self, url: str, referer: str = BASE_URL, is_json: bool = False, extra_headers: Optional[Dict] = None) -> Any:
        if not breaker.allow():
            raise HTTPException(503, "Upstream circuit open")

        if url in self.inflight:
            event = self.inflight[url]
            await event.wait()
            if url in self.inflight_errors:
                raise self.inflight_errors[url]
            return self.inflight_results[url]

        event = asyncio.Event()
        self.inflight[url] = event

        try:
            result = await self._do_fetch(url, referer, is_json, extra_headers)
            self.inflight_results[url] = result
            breaker.record_success()
            return result
        except Exception as e:
            self.inflight_errors[url] = e
            breaker.record_failure()
            raise
        finally:
            event.set()
            async def _cleanup():
                await asyncio.sleep(0.1)
                self.inflight.pop(url, None)
                self.inflight_results.pop(url, None)
                self.inflight_errors.pop(url, None)
            asyncio.create_task(_cleanup())

    async def _do_fetch(self, url: str, referer: str, is_json: bool, extra_headers: Optional[Dict]) -> Any:
        for attempt in range(12):
            profile = BROWSER_PROFILES[attempt % len(BROWSER_PROFILES)]
            headers = {
                "User-Agent": profile["User-Agent"],
                "Referer": referer,
                "Accept-Encoding": "gzip, deflate, br",
            }
            if profile.get("sec-ch-ua"):
                headers["sec-ch-ua"] = profile["sec-ch-ua"]
                headers["sec-ch-ua-mobile"] = profile["sec-ch-ua-mobile"]
                headers["sec-ch-ua-platform"] = profile["sec-ch-ua-platform"]
            if extra_headers:
                headers.update(extra_headers)

            try:
                async with self.semaphore:
                    resp = await self.client.get(url, headers=headers)
                resp.raise_for_status()
                return resp.json() if is_json else resp.text
            except Exception:
                if attempt == 11:
                    raise
                if attempt < 3:
                    delay = (0.3 * (2 ** attempt)) + random.uniform(0, 0.2)
                elif attempt < 7:
                    delay = 1.5 + random.uniform(0, 0.5)
                else:
                    delay = 3.0 + random.uniform(0, 3.0)
                await asyncio.sleep(delay)

http_client = HttpClient()

# ──────────────────────────────────────────────
# PARSING (Offloaded to ThreadPool)
# ──────────────────────────────────────────────
executor = ThreadPoolExecutor(max_workers=PARSER_WORKERS)

def parse_search(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    results = []
    for item in parser.css("div.flw-item.item-qtip"):
        img = item.css_first("img")
        if not img: continue
        anime_id = item.attributes.get("data-id", "")
        if anime_id:
            results.append({
                "anime_id": anime_id,
                "title": img.attributes.get("alt", "").strip(),
                "poster": img.attributes.get("data-src") or img.attributes.get("src") or "",
            })
    return results

def parse_episodes(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    episodes = []
    for ep in parser.css("a.item.ep-item"):
        num = ep.attributes.get("data-number", "")
        eid = ep.attributes.get("data-id", "")
        if num and eid:
            episodes.append({
                "episode_number": num,
                "episode_id": eid,
                "title": ep.attributes.get("title", f"Episode {num}").strip(),
            })
    return episodes

def parse_home(html: str) -> Dict:
    parser = LexborHTMLParser(html)
    thumbs: List[str] = []
    heroes: List[str] = []
    trending: List[Dict] = []

    for img in parser.css("img.film-poster-img"):
        a = img.attributes
        if a.get("data-src"): thumbs.append(a["data-src"])
        if a.get("src"): heroes.append(a["src"])

    for item in parser.css("div.flw-item"):
        img = item.css_first("img")
        if not img: continue
        a = img.attributes
        title = a.get("alt", "").strip()
        anime_id = item.attributes.get("data-id", "")
        if title and anime_id:
            trending.append({
                "anime_id": anime_id,
                "title": title,
                "poster": a.get("data-src") or a.get("src") or "",
            })

    return {"thumbnails": thumbs, "hero_thumbnails": heroes, "trending": trending, "count": len(thumbs)}

def parse_server_id(html: str, sub: str) -> Optional[str]:
    for s in LexborHTMLParser(html).css("div.item.server-item"):
        if sub in s.attributes.get("data-type", "").lower():
            return s.attributes.get("data-id")
    return None

# Inline BS4-free parser for mew server list using selectolax
def parse_mew_server_ids(html: str) -> List[str]:
    parser = LexborHTMLParser(html)
    return [
        node.attributes.get("data-id")
        for node in parser.css("div.item.server-item")
        if node.attributes.get("data-id")
    ]

# ──────────────────────────────────────────────
# SERVICE LAYER — v9.0 routes (unchanged)
# ──────────────────────────────────────────────
async def get_search(query: str) -> List[Dict]:
    key = f"search:{query.lower().strip()}"
    val, is_stale = cache.get(key)
    if val and not is_stale: return val

    async def refresh():
        try:
            html = await http_client.fetch(f"{BASE_URL}/search?keyword={quote(query)}")
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(executor, parse_search, html)
            cache.set(key, results, TTL["search"])
            return results
        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_episodes(anime_id: str) -> List[Dict]:
    key = f"ep:{anime_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale: return val

    async def refresh():
        try:
            data = await http_client.fetch(f"{BASE_URL}/ajax/episode/list/{anime_id}", is_json=True)
            loop = asyncio.get_running_loop()
            eps = await loop.run_in_executor(executor, parse_episodes, data.get("html", ""))
            cache.set(key, eps, TTL["episode"])
            return eps
        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_home() -> Dict:
    key = "home"
    val, is_stale = cache.get(key)
    if val and not is_stale: return val

    async def refresh():
        try:
            html = await http_client.fetch(f"{BASE_URL}/home")
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(executor, parse_home, html)
            cache.set(key, res, TTL["home"])
            return res
        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_suggest(query: str) -> List:
    key = f"sug:{query.lower().strip()}"
    val, is_stale = cache.get(key)
    if val and not is_stale: return val

    async def refresh():
        try:
            data = await http_client.fetch(f"{BASE_URL}/ajax/search/suggest?keyword={quote(query)}", is_json=True)
            result = data.get("result", [])
            cache.set(key, result, TTL["suggest"])
            return result
        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()



async def get_stream(episode_id: str, sub: str = "sub") -> Dict:
    key = f"stream:{episode_id}:{sub}"
    val, is_stale = cache.get(key)

    # ✅ Return fresh cache
    if val and not is_stale:
        return val

    async def refresh():
        try:
            servers_data = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/servers?episodeId={episode_id}",
                is_json=True
            )

            loop = asyncio.get_running_loop()
            server_id = await loop.run_in_executor(
                executor,
                parse_server_id,
                servers_data.get("html", ""),
                sub
            )

            fallback = f"https://megaplay.buzz/stream/s-2/{episode_id}/{sub}"

            if not server_id:
                result = {
                    "url": fallback,
                    "source": "fallback"
                }
            else:
                src = await http_client.fetch(
                    f"{BASE_URL}/ajax/episode/sources?id={server_id}",
                    is_json=True
                )

                url = src.get("link")

                # 🔥 UPDATED LOGIC
                if not url:
                    sources = src.get("sources", [])

                    if sources:
                        if sub == "sub":
                            url = sources[0].get("file")   # first stream
                        else:
                            url = sources[-1].get("file")  # last stream
                    else:
                        url = fallback

                result = {
                    "url": url,
                    "source": "9anime",
                    "server_id": server_id
                }

            cache.set(key, result, TTL["stream"])
            return result

        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    # ✅ Stale → return old + refresh in background
    if is_stale and val:
        asyncio.create_task(refresh())
        return val  # FIXED

    # ✅ No cache → fetch fresh
    return await refresh()

# ──────────────────────────────────────────────
# SERVICE LAYER — NEW VOD STREAM (v9.1)
# ──────────────────────────────────────────────
def _get_mew_date() -> str:
    now = datetime.now(timezone.utc)
    # Windows-safe date formatting
    return quote(f"{now.month}/{now.day}/{now.year} {now.hour:02d}:{now.minute:02d}", safe='')

async def _fetch_vod_for_type(episode_id: str, audio_type: str, date: str) -> Optional[Dict]:
    """
    Fetch netmagcdn VOD URL for a given episode + audio type.
    Reuses shared http_client for connection pooling + semaphore.
    """
    servers_url = f"{MEW_BASE}/ajax/episode/servers?episodeId={episode_id}&type={audio_type}-{date}"
    try:
        servers_data = await http_client.fetch(servers_url, referer=MEW_BASE, is_json=True)
    except Exception as e:
        logger.warning(f"[vod/{audio_type}] servers fetch failed: {e}")
        return None

    loop = asyncio.get_running_loop()
    server_ids: List[str] = await loop.run_in_executor(
        executor, parse_mew_server_ids, servers_data.get("html", "")
    )

    if not server_ids:
        logger.warning(f"[vod/{audio_type}] no server IDs for episode {episode_id}")
        return None

    for sid in server_ids:
        try:
            src_url = f"{MEW_BASE}/ajax/episode/sources?id={sid}&type={audio_type}-{date}"
            src_data = await http_client.fetch(src_url, referer=MEW_BASE, is_json=True)
            embed_link: str = src_data.get("link", "")

            if "rapid-cloud.co" not in embed_link and "megacloud" not in embed_link:
                continue

            embed_id = embed_link.split("/")[-1].split("?")[0]
            sources_url = f"{RAPID_CLOUD}/embed-2/v2/e-1/getSources?id={embed_id}"

            sources_data = await http_client.fetch(
                sources_url,
                referer=embed_link,
                is_json=True,
                extra_headers={
                    "Accept": "*/*",
                    "X-Requested-With": "XMLHttpRequest",
                    "User-Agent": MOBILE_UA,
                },
            )

            sources: List[Dict] = sources_data.get("sources", [])
            tracks: List[Dict] = sources_data.get("tracks", [])

            # Pick netmagcdn — no Cloudflare
            vod_url = next(
                (s["file"] for s in sources if "netmagcdn.com" in s.get("file", "")),
                None,
            )

            if vod_url:
                return {
                    "m3u8": vod_url,
                    "subtitles": [t for t in tracks if t.get("kind") == "captions"],
                }

        except Exception as e:
            logger.warning(f"[vod/{audio_type}] sid={sid} failed: {e}")
            continue

    return None


async def get_vod_stream(episode_id: str) -> Dict:
    key = f"vod:{episode_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            date = _get_mew_date()
            # sub + dub concurrently
            sub, dub = await asyncio.gather(
                _fetch_vod_for_type(episode_id, "sub", date),
                _fetch_vod_for_type(episode_id, "dub", date),
            )
            result = {"episode_id": episode_id, "sub": sub, "dub": dub}
            cache.set(key, result, TTL["stream_v2"])
            return result
        except Exception as e:
            logger.error(f"VOD stream refresh failed ep={episode_id}: {e}")
            return None

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

# ──────────────────────────────────────────────
# FASTAPI APP
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await http_client.start()
    logger.info("🚀 RO-Anime API v9.1 Online")
    yield
    await http_client.stop()
    executor.shutdown()

app = FastAPI(lifespan=lifespan, title="RO-Anime API v9.1")

app.add_middleware(CORSMiddleware, allow_origins=["*"])
app.add_middleware(GZipMiddleware, minimum_size=512)

@app.middleware("http")
async def middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(ip):
        return Response(
            msgspec.json.encode({"error": "Too many requests", "retry_after": 60}),
            status_code=429,
            media_type="application/json",
            headers={"Retry-After": "60"},
        )
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_time:.2f}"
    response.headers["X-Powered-By"] = "RO-Anime/9.1"
    return response

# ── v9.0 routes (untouched) ──
@app.get("/search/{query}")
async def route_search(query: str):
    results = await get_search(query)
    return Response(msgspec.json.encode({"results": results}), media_type="application/json")

@app.get("/suggest/{query}")
async def route_suggest(query: str):
    results = await get_suggest(query)
    return Response(msgspec.json.encode({"results": results}), media_type="application/json")

@app.get("/anime/episode/{anime_id}")
async def route_episodes(anime_id: str):
    eps = await get_episodes(anime_id)
    return Response(msgspec.json.encode({"episodes": eps}), media_type="application/json")

@app.get("/anime/stream/{episode_id}")
async def route_stream(episode_id: str, type: str = Query(default="sub", pattern="^(sub|dub)$")):
    res = await get_stream(episode_id, type)
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/home/thumbnails")
async def route_home():
    res = await get_home()
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/batch/search")
async def route_batch_search(q: List[str] = Query(...)):
    queries = list(dict.fromkeys(q[:20]))
    tasks = [get_search(title) for title in queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {t: (r if not isinstance(r, Exception) else {"error": str(r)}) for t, r in zip(queries, results)}
    return Response(msgspec.json.encode(out), media_type="application/json")

# ── v9.1 NEW endpoint ──
@app.get("/stream/v2/{episode_id}")
async def route_vod_stream(episode_id: str):
    """
    Returns netmagcdn M3U8 URLs for sub + dub.
    Source: nine.mewcdn.online → rapid-cloud.co getSources → netmagcdn filter
    Response:
    {
        "episode_id": "161805",
        "sub": { "m3u8": "https://vod.netmagcdn.com:2228/...", "subtitles": [...] },
        "dub": { "m3u8": "...", "subtitles": [...] }  // null if unavailable
    }
    """
    result = await get_vod_stream(episode_id)
    if not result or (result["sub"] is None and result["dub"] is None):
        raise HTTPException(status_code=404, detail="No VOD sources found for this episode")
    return Response(msgspec.json.encode(result), media_type="application/json")

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "9.1",
        "cache": cache.stats(),
        "circuit": breaker.state,
        "inflight": len(http_client.inflight),
    }

if __name__ == "__main__":
    import uvicorn
    uvloop.install()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
