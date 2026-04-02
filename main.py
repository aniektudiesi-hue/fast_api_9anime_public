"""
RO-Anime API v10.1 - "Sub-Only Unified Stream Edition"
=======================================================
Base: v10.0 infrastructure (superior parsers, backoff curve, parse_home with
      trending/hero, parse_search with flw-item.item-qtip, parse_episodes
      with a.item.ep-item).

Carried over from v10.0 unchanged:
  - All msgspec models: AnimeBase, Episode, StreamInfo, QualityInfo,
    Timestamps, Subtitle, ServerSource, EpisodeResponse
  - HttpClient with origin param, request coalescing, 12-attempt v9 backoff
  - Full Origin header in _do_fetch (BUG-6 fix)
  - get_date_param() helper
  - SmartCache O(1) LRU + SWR with BUG-8 fix
  - All SWR refresh() re-raise on exception (BUG-1 fix)
  - get_source_v2() with &type= and date param (BUG-4, BUG-5 fixes)
  - parse_server_ids() with data-type attribute filtering + fallback
  - TTL entry for "unified_stream"
  - Full HTTP middleware (rate limit, X-Process-Time-Ms, X-Powered-By)
  - All routes: /search, /suggest, /anime/episode, /anime/stream,
    /stream/v2, /api/v10/stream, /home/thumbnails, /batch/search, /health
  - if __name__ == "__main__" uvicorn entry point

Added in v10.1:
  - get_unified_stream() SUB-ONLY FILTER: after fetching each source from
    MEW_BASE the returned JSON "type" field is checked; any source whose type
    does not match stream_type is skipped — only matching CDN m3u8 URLs
    reach EpisodeResponse.servers
  - Quality ladder: if RapidCloud returns multiple entries in "sources",
    each gets its own QualityInfo instead of just the first
  - Subtitles collected from all servers (not first only) — deduped by file
  - /api/v10/stream route returns 404 with descriptive message when no
    matching sources are found (previously silently returned empty servers[])
  - Version bumped to 10.1 in logger name, lifespan log, X-Powered-By,
    FastAPI title
"""

import asyncio
import logging
import random
import re
import time
from collections import deque, OrderedDict
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote, urlparse

import msgspec
import uvloop
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from httpx import AsyncClient, HTTPStatusError, Limits, Timeout
from selectolax.lexbor import LexborHTMLParser

# ──────────────────────────────────────────────
# CONFIGURATION & CONSTANTS
# ──────────────────────────────────────────────
BASE_URL             = "https://9animetv.to"
MEW_BASE             = "https://nine.mewcdn.online"
RAPID_CLOUD_BASE     = "https://rapid-cloud.co"
UPSTREAM_CONCURRENCY = 15
CACHE_MAX_SIZE       = 5000
PARSER_WORKERS       = 4

TTL = {
    "search":         600,
    "episode":        300,
    "suggest":        600,
    "home":           180,
    "stream":         120,
    "stream_v2":      120,
    "unified_stream": 120,   # v10 addition
}

# ──────────────────────────────────────────────
# STRUCTURED LOGGING
# ──────────────────────────────────────────────
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level":     record.levelname,
            "name":      record.name,
            "message":   record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return msgspec.json.encode(log_record).decode()

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger("ro-anime-v10.1")

# ──────────────────────────────────────────────
# MODELS (msgspec for speed)
# ──────────────────────────────────────────────
class AnimeBase(msgspec.Struct):
    anime_id: str
    title:    str
    poster:   str

class Episode(msgspec.Struct):
    episode_number: str
    episode_id:     str
    title:          str

class StreamInfo(msgspec.Struct):
    url:       str
    source:    str
    server_id: Optional[str] = None

# ── v10 models ────────────────────────────────
class QualityInfo(msgspec.Struct):
    label:   str
    url:     str
    bitrate: Optional[int] = None

class Timestamps(msgspec.Struct):
    intro: List[float] = msgspec.field(default_factory=lambda: [0.0, 0.0])
    outro: List[float] = msgspec.field(default_factory=lambda: [0.0, 0.0])

class Subtitle(msgspec.Struct):
    file:    str
    label:   str
    default: bool = False

class ServerSource(msgspec.Struct):
    serverName: str
    m3u8Url:    str
    qualities:  List[QualityInfo]
    referer:    str

class EpisodeResponse(msgspec.Struct):
    episodeId:  str
    type:       str
    servers:    List[ServerSource]
    cdnDomain:  str
    subtitles:  List[Subtitle]       = msgspec.field(default_factory=list)
    timestamps: Optional[Timestamps] = None

# ──────────────────────────────────────────────
# BROWSER PROFILES
# ──────────────────────────────────────────────
BROWSER_PROFILES = [
    {
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "User-Agent":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"macOS"',
    },
    {
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "sec-ch-ua":          '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "sec-ch-ua":          "",
        "sec-ch-ua-mobile":   "",
        "sec-ch-ua-platform": "",
    },
    {
        "User-Agent":         "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "sec-ch-ua":          '"Chromium";v="122", "Google Chrome";v="122", "Not(A:Brand";v="24"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Linux"',
    },
]

# ──────────────────────────────────────────────
# SMART CACHE (O(1) LRU + SWR)
# ──────────────────────────────────────────────
class SmartCache:
    def __init__(self, max_size: int = 5000):
        self._store: OrderedDict[str, Tuple[Any, float, float]] = OrderedDict()
        self._max   = max_size
        self.hits   = self.misses = self.swr_hits = 0

    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        entry = self._store.get(key)
        if entry is None:
            self.misses += 1
            return None, False

        value, expires_at, stale_at = entry
        now = time.monotonic()

        # Fully expired — evict
        if now > stale_at:
            self._store.pop(key, None)
            self.misses += 1
            return None, False

        self._store.move_to_end(key)

        # BUG-8 FIX: fresh check first; is_stale only in the SWR window
        if now <= expires_at:
            self.hits += 1
            return value, False          # fresh

        # now is in (expires_at, stale_at) → SWR window
        self.swr_hits += 1
        return value, True

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
            "size":     len(self._store),
            "hits":     self.hits,
            "swr_hits": self.swr_hits,
            "misses":   self.misses,
            "hit_rate": round((self.hits + self.swr_hits) / total, 3) if total else 0.0,
        }

cache = SmartCache(max_size=CACHE_MAX_SIZE)

# ──────────────────────────────────────────────
# RATE LIMITER
# ──────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_req: int = 200, window: int = 60) -> None:
        self._max    = max_req
        self._window = window
        self._buckets: Dict[str, deque] = {}

    def is_allowed(self, ip: str) -> bool:
        now    = time.monotonic()
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
        self.threshold         = threshold
        self.recovery_time     = recovery_time
        self.failures          = 0
        self.state             = "CLOSED"
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
        self.state    = "CLOSED"

    def record_failure(self):
        self.failures          += 1
        self.last_failure_time  = time.monotonic()
        if self.failures >= self.threshold:
            self.state = "OPEN"
            logger.error(f"Circuit Breaker OPENED after {self.failures} failures")

breaker = CircuitBreaker()

# ──────────────────────────────────────────────
# HTTP CLIENT
# ──────────────────────────────────────────────
class HttpClient:
    def __init__(self):
        self.client:           Optional[AsyncClient]    = None
        self.semaphore                                   = asyncio.Semaphore(UPSTREAM_CONCURRENCY)
        self.inflight:         Dict[str, asyncio.Event] = {}
        self.inflight_results: Dict[str, Any]           = {}
        self.inflight_errors:  Dict[str, Exception]     = {}

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

    async def fetch(
        self,
        url:     str,
        referer: str  = BASE_URL,
        origin:  str  = "",        # v10 addition — needed by MEW_BASE & RapidCloud
        is_json: bool = False,
    ) -> Any:
        if not breaker.allow():
            raise HTTPException(503, "Upstream circuit open")

        # Request coalescing
        if url in self.inflight:
            event = self.inflight[url]
            await event.wait()
            if url in self.inflight_errors:
                raise self.inflight_errors[url]
            return self.inflight_results[url]

        event              = asyncio.Event()
        self.inflight[url] = event

        try:
            result = await self._do_fetch(url, referer, origin, is_json)
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

    async def _do_fetch(
        self,
        url:     str,
        referer: str,
        origin:  str,
        is_json: bool,
    ) -> Any:
        for attempt in range(12):
            profile = BROWSER_PROFILES[attempt % len(BROWSER_PROFILES)]
            # v10 BUG-6 FIX: include Origin header — required by MEW_BASE & RapidCloud
            headers = {
                **profile,
                "Referer":          referer,
                "Accept":           "*/*",
                "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
                "Connection":       "keep-alive",
                "Sec-Fetch-Dest":   "empty",
                "Sec-Fetch-Mode":   "cors",
                "Sec-Fetch-Site":   "cross-site",
            }
            if origin:
                headers["Origin"] = origin

            try:
                async with self.semaphore:
                    resp = await self.client.get(url, headers=headers)
                    resp.raise_for_status()
                    return resp.json() if is_json else resp.text
            except Exception as e:
                if attempt == 11:
                    raise
                # v9 backoff curve (better than v10's flat 0.2*attempt)
                if attempt < 3:
                    delay = (0.3 * (2 ** attempt)) + random.uniform(0, 0.2)
                elif attempt < 7:
                    delay = 1.5 + random.uniform(0, 0.5)
                else:
                    delay = 3.0 + random.uniform(0, 3.0)
                await asyncio.sleep(delay)

http_client = HttpClient()
executor    = ThreadPoolExecutor(max_workers=PARSER_WORKERS)

# ──────────────────────────────────────────────
# PARSERS  (v9 versions — more precise selectors)
# ──────────────────────────────────────────────
def parse_search(html: str) -> List[Dict]:
    """v9 parser: uses div.flw-item.item-qtip — precise, no false matches."""
    parser  = LexborHTMLParser(html)
    results = []
    for item in parser.css("div.flw-item.item-qtip"):
        img = item.css_first("img")
        if not img:
            continue
        anime_id = item.attributes.get("data-id", "")
        if anime_id:
            results.append({
                "anime_id": anime_id,
                "title":    img.attributes.get("alt", "").strip(),
                "poster":   img.attributes.get("data-src") or img.attributes.get("src") or "",
            })
    return results

def parse_suggest(html: str) -> List[Dict]:
    """Parse suggestion results from the AJAX suggest endpoint."""
    parser  = LexborHTMLParser(html)
    results = []
    for node in parser.css(".item"):
        title_node = node.css_first(".name")
        img_node   = node.css_first("img")
        if title_node and img_node:
            results.append({
                "anime_id": node.attributes.get("href", "").split("/")[-1],
                "title":    title_node.text().strip(),
                "poster":   img_node.attributes.get("data-src") or img_node.attributes.get("src", ""),
            })
    return results

def parse_episodes(html: str) -> List[Dict]:
    """v9 parser: uses a.item.ep-item — matches 9animetv episode anchor elements."""
    parser   = LexborHTMLParser(html)
    episodes = []
    for ep in parser.css("a.item.ep-item"):
        num = ep.attributes.get("data-number", "")
        eid = ep.attributes.get("data-id", "")
        if num and eid:
            episodes.append({
                "episode_number": num,
                "episode_id":     eid,
                "title":          ep.attributes.get("title", f"Episode {num}").strip(),
            })
    return episodes

def parse_home(html: str) -> List[Dict]:
    """
    v9 parser extended: returns a flat list of {anime_id, title, poster}
    from the trending/featured items on the home page.
    The frontend (player.html) expects a plain JSON array from /home/thumbnails.
    """
    parser  = LexborHTMLParser(html)
    results = []
    for item in parser.css("div.flw-item"):
        img = item.css_first("img")
        if not img:
            continue
        title    = img.attributes.get("alt", "").strip()
        anime_id = item.attributes.get("data-id", "")
        if title and anime_id:
            results.append({
                "anime_id": anime_id,
                "title":    title,
                "poster":   img.attributes.get("data-src") or img.attributes.get("src") or "",
            })
    return results

def parse_server_id(html: str, sub: str) -> Optional[str]:
    for s in LexborHTMLParser(html).css("div.item.server-item"):
        if sub in s.attributes.get("data-type", "").lower():
            return s.attributes.get("data-id")
    return None

def parse_vidcloud_ids(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    v9 approach: find first two Vidcloud server-items.
    Returns (sub_id, dub_id).
    """
    sub_id: Optional[str] = None
    dub_id: Optional[str] = None
    count = 0
    for node in LexborHTMLParser(html).css("div.item.server-item"):
        text = node.text(strip=True)
        if "Vidcloud" in text:
            data_id = node.attributes.get("data-id")
            if count == 0:
                sub_id = data_id
            elif count == 1:
                dub_id = data_id
                break
            count += 1
    return sub_id, dub_id

def parse_server_ids(html: str, stream_type: str = "sub") -> List[str]:
    """
    v10 helper: extract data-id values for the requested stream type only.
    The server list HTML contains items for both sub and dub; we must filter
    by data-type attribute so dub requests don't silently get sub server IDs.

    Each server item looks like:
      <div class="item server-item" data-id="12345" data-type="sub" ...>
    We parse with selectolax to respect that attribute correctly.
    Falls back to all IDs if no type-matched items are found (e.g. dub not available).
    """
    parser   = LexborHTMLParser(html)
    matched  = []
    fallback = []
    for node in parser.css("div.item.server-item"):
        data_id   = node.attributes.get("data-id", "").strip()
        data_type = node.attributes.get("data-type", "").lower().strip()
        if not data_id:
            continue
        fallback.append(data_id)
        # data-type may be "sub", "dub", "sub-1", "dub-1" etc.
        if stream_type in data_type:
            matched.append(data_id)

    # Return type-matched IDs; if none found fall back to all (preserves
    # old behaviour for edge-case pages with no data-type attributes).
    return matched if matched else fallback

# ──────────────────────────────────────────────
# V10 HELPER: DATE PARAM
# ──────────────────────────────────────────────
def get_date_param() -> str:
    """
    MEW_BASE /ajax/episode/servers and /ajax/episode/sources both require a
    URL-encoded datetime param in the format M/D/YYYY HH:MM (UTC).
    """
    now = datetime.now(timezone.utc)
    raw = f"{now.month}/{now.day}/{now.year} {now.hour:02d}:{now.minute:02d}"
    return raw.replace("/", "%2F").replace(" ", "%20").replace(":", "%3A")

# ──────────────────────────────────────────────
# SERVICE LAYER
# ──────────────────────────────────────────────

async def get_search(query: str) -> List[Dict]:
    key           = f"search:{query.lower().strip()}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            html    = await http_client.fetch(f"{BASE_URL}/search?keyword={quote(query)}")
            loop    = asyncio.get_running_loop()
            results = await loop.run_in_executor(executor, parse_search, html)
            cache.set(key, results, TTL["search"])
            return results
        except Exception as e:
            logger.error(f"search refresh failed for '{query}': {e}")
            raise  # BUG-1 FIX: re-raise so route returns proper HTTP error

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_suggest(query: str) -> List:
    key           = f"sug:{query.lower().strip()}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            data   = await http_client.fetch(
                f"{BASE_URL}/ajax/search/suggest?keyword={quote(query)}", is_json=True
            )
            # 9animetv returns HTML inside a JSON envelope; parse it
            html   = data.get("html", "")
            loop   = asyncio.get_running_loop()
            result = await loop.run_in_executor(executor, parse_suggest, html) if html else data.get("result", [])
            cache.set(key, result, TTL["suggest"])
            return result
        except Exception as e:
            logger.error(f"suggest refresh failed for '{query}': {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_episodes(anime_id: str) -> List[Dict]:
    key           = f"ep:{anime_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            data = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/list/{anime_id}", is_json=True
            )
            loop = asyncio.get_running_loop()
            eps  = await loop.run_in_executor(executor, parse_episodes, data.get("html", ""))
            cache.set(key, eps, TTL["episode"])
            return eps
        except Exception as e:
            logger.error(f"episodes refresh failed for '{anime_id}': {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_home() -> List[Dict]:
    """
    Returns a flat list of {anime_id, title, poster} objects.
    The frontend reads this as a plain JSON array and uses:
      - poster URLs   → hero slider images
      - full objects  → dynamic anime grid cards
    """
    key           = "home"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            html    = await http_client.fetch(f"{BASE_URL}/home")
            loop    = asyncio.get_running_loop()
            results = await loop.run_in_executor(executor, parse_home, html)
            cache.set(key, results, TTL["home"])
            return results
        except Exception as e:
            logger.error(f"home refresh failed: {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_stream(episode_id: str, sub: str = "sub") -> Dict:
    key           = f"stream:{episode_id}:{sub}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            servers_data = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/servers?episodeId={episode_id}", is_json=True
            )
            loop      = asyncio.get_running_loop()
            server_id = await loop.run_in_executor(
                executor, parse_server_id, servers_data.get("html", ""), sub
            )
            fallback = f"https://megaplay.buzz/stream/s-2/{episode_id}/{sub}"
            if not server_id:
                result = {"url": fallback, "source": "fallback"}
            else:
                src    = await http_client.fetch(
                    f"{BASE_URL}/ajax/episode/sources?id={server_id}", is_json=True
                )
                result = {"url": src.get("link", fallback), "source": "9anime", "server_id": server_id}
            cache.set(key, result, TTL["stream"])
            return result
        except Exception as e:
            logger.error(f"stream refresh failed for '{episode_id}': {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_source_v2(episode_id: str) -> Dict:
    """
    Fetches sub & dub m3u8 sources and subtitle tracks.
    Pipeline: server list → vidcloud IDs → source links → RapidCloud getSources.
    All parallel where possible.
    """
    mew_referer   = MEW_BASE
    rapid_referer = RAPID_CLOUD_BASE

    # BUG-4 FIX: &type= param required; BUG-2 FIX: origin= required
    date        = get_date_param()
    servers_url = f"{MEW_BASE}/ajax/episode/servers?episodeId={episode_id}&type=sub"
    try:
        servers_data = await http_client.fetch(
            servers_url, referer=mew_referer, origin=MEW_BASE, is_json=True
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[stream_v2] servers fetch failed for {episode_id}: {e}")
        raise HTTPException(502, "Failed to fetch episode server list")

    html = servers_data.get("html", "")
    if not html:
        raise HTTPException(502, "Empty server list response")

    loop           = asyncio.get_running_loop()
    sub_id, dub_id = await loop.run_in_executor(executor, parse_vidcloud_ids, html)

    if not sub_id or not dub_id:
        raise HTTPException(404, f"Vidcloud sub/dub server not found for episode {episode_id}")

    # BUG-5 FIX: date param required on source URLs too
    sub_src_url = f"{MEW_BASE}/ajax/episode/sources?id={sub_id}&type=sub-{date}"
    dub_src_url = f"{MEW_BASE}/ajax/episode/sources?id={dub_id}&type=dub-{date}"

    try:
        sub_src_data, dub_src_data = await asyncio.gather(
            http_client.fetch(sub_src_url, referer=mew_referer, origin=MEW_BASE, is_json=True),
            http_client.fetch(dub_src_url, referer=mew_referer, origin=MEW_BASE, is_json=True),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[stream_v2] source link fetch failed for {episode_id}: {e}")
        raise HTTPException(502, "Failed to fetch sub/dub source links")

    sub_link: str = sub_src_data.get("link", "")
    dub_link: str = dub_src_data.get("link", "")

    if not sub_link or not dub_link:
        raise HTTPException(502, "Missing embed link in source response")

    sub_embed_id = sub_link.rstrip("/").split("/")[-1].split("?")[0]
    dub_embed_id = dub_link.rstrip("/").split("/")[-1].split("?")[0]

    if not sub_embed_id or not dub_embed_id:
        raise HTTPException(502, "Could not parse embed IDs from source links")

    sub_sources_url = f"{RAPID_CLOUD_BASE}/embed-2/v2/e-1/getSources?id={sub_embed_id}"
    dub_sources_url = f"{RAPID_CLOUD_BASE}/embed-2/v2/e-1/getSources?id={dub_embed_id}"

    try:
        sub_sources_data, dub_sources_data = await asyncio.gather(
            http_client.fetch(sub_sources_url, referer=sub_link, origin=RAPID_CLOUD_BASE, is_json=True),
            http_client.fetch(dub_sources_url, referer=dub_link, origin=RAPID_CLOUD_BASE, is_json=True),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[stream_v2] RapidCloud fetch failed for {episode_id}: {e}")
        raise HTTPException(502, "Failed to fetch stream sources from RapidCloud")

    try:
        sub_file:   str  = sub_sources_data["sources"][0]["file"]
        sub_tracks: List = sub_sources_data.get("tracks", [])
        dub_file:   str  = dub_sources_data["sources"][0]["file"]
        dub_tracks: List = dub_sources_data.get("tracks", [])
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"[stream_v2] Unexpected source structure for {episode_id}: {e}")
        raise HTTPException(502, "Unexpected response structure from stream source")

    return {
        "episode_id": str(episode_id),
        "sub": {"m3u8": sub_file, "subtitles": sub_tracks},
        "dub": {"m3u8": dub_file, "subtitles": dub_tracks},
    }


# ──────────────────────────────────────────────
# UNIFIED STREAMING (v10.1) — what the player calls
# ──────────────────────────────────────────────
async def get_unified_stream(episode_id: str, stream_type: str = "sub") -> EpisodeResponse:
    """
    GET /api/v10/stream/{episode_id}?type=sub|dub

    Returns EpisodeResponse with:
      servers[]    — list of ServerSource objects — SUB-ONLY FILTERED (v10.1)
      subtitles[]  — Subtitle objects from RapidCloud tracks (all servers, deduped)
      timestamps   — Timestamps(intro, outro) stub — real values require a
                     separate AniSkip/AniList call (placeholder values provided)
      cdnDomain    — hostname of the primary m3u8 source

    v10.1 SUB-ONLY FILTER:
      After fetching each source link from MEW_BASE, the embed is resolved
      via RapidCloud getSources. The "sources" array in that response contains
      HLS m3u8 URLs. We also check the MEW source response "type" field — any
      server whose returned type does not match stream_type is skipped entirely
      so that dub CDN URLs never appear in a sub response and vice-versa.

    Bug fixes inherited from v10.0:
      BUG-1: all refresh() re-raise on exception
      BUG-2: origin=MEW_BASE on all MEW calls, origin=RAPID_CLOUD_BASE on RC calls
      BUG-3: bare except → except Exception
      BUG-4: &type= forwarded in server list URL
      BUG-5: date param forwarded to source URLs
      BUG-6: Origin header sent in _do_fetch when origin != ""
      BUG-8: SmartCache fresh/SWR logic corrected
    """
    key           = f"unified_stream:{episode_id}:{stream_type}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        date        = get_date_param()
        servers_url = (
            f"{MEW_BASE}/ajax/episode/servers"
            f"?episodeId={episode_id}&type={stream_type}-{date}"
        )
        try:
            # BUG-2 FIX: origin= required by nine.mewcdn.online
            servers_data = await http_client.fetch(
                servers_url,
                referer=MEW_BASE,
                origin=MEW_BASE,
                is_json=True,
            )
            html       = servers_data.get("html", "")
            server_ids = parse_server_ids(html, stream_type)

            all_servers:   List[ServerSource] = []
            all_subtitles: List[Subtitle]     = []
            seen_sub_files: set               = set()   # v10.1: dedup subtitles across servers
            main_cdn                          = "unknown"

            # Fetch up to 3 server sources in parallel
            tasks = []
            for s_id in server_ids[:3]:
                source_url = (
                    f"{MEW_BASE}/ajax/episode/sources"
                    f"?id={s_id}&type={stream_type}-{date}"
                )
                tasks.append(
                    http_client.fetch(
                        source_url,
                        referer=MEW_BASE,
                        origin=MEW_BASE,
                        is_json=True,
                    )
                )

            source_results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in source_results:
                if isinstance(res, Exception):
                    continue

                # ── v10.1 SUB-ONLY FILTER ────────────────────────────────
                # MEW returns a "type" field on each source response.
                # Skip any server whose type doesn't match what we asked for —
                # this prevents dub CDN URLs leaking into a sub response.
                returned_type = res.get("type", "").lower()
                # Strip trailing date suffix if present e.g. "sub-4%2F2%2F2025..."
                returned_type_base = returned_type.split("-")[0] if "-" in returned_type else returned_type
                if returned_type_base and returned_type_base != stream_type:
                    logger.debug(
                        f"[unified] Skipping server: returned type '{returned_type}' "
                        f"!= requested '{stream_type}'"
                    )
                    continue
                # ─────────────────────────────────────────────────────────

                link = res.get("link", "")
                if "rapid-cloud.co" in link or "megacloud" in link:
                    embed_id = link.rstrip("/").split("/")[-1].split("?")[0]
                    api_url  = f"{RAPID_CLOUD_BASE}/embed-2/v2/e-1/getSources?id={embed_id}"
                    try:
                        # BUG-3 FIX: except Exception (not bare except:)
                        api_res = await http_client.fetch(
                            api_url,
                            referer=link,
                            origin=RAPID_CLOUD_BASE,   # BUG-6 FIX
                            is_json=True,
                        )

                        # v10.1: collect subtitles from ALL servers, deduped by file URL
                        for sub in api_res.get("tracks", []):
                            if sub.get("kind") == "captions":
                                sub_file = sub.get("file", "")
                                if sub_file and sub_file not in seen_sub_files:
                                    seen_sub_files.add(sub_file)
                                    all_subtitles.append(Subtitle(
                                        file    = sub_file,
                                        label   = sub.get("label", "Unknown"),
                                        default = sub.get("default", False),
                                    ))

                        # v10.1: build QualityInfo for EVERY source entry, not just [0]
                        sources_list = api_res.get("sources", [])
                        qualities: List[QualityInfo] = []
                        first_m3u8 = ""
                        for s in sources_list:
                            m3u8 = s.get("file", "")
                            if m3u8:
                                if not first_m3u8:
                                    first_m3u8 = m3u8
                                    main_cdn   = urlparse(m3u8).hostname or "unknown"
                                qualities.append(QualityInfo(
                                    label   = s.get("label", "Auto"),
                                    url     = m3u8,
                                    bitrate = s.get("bitrate"),
                                ))

                        if first_m3u8:
                            all_servers.append(ServerSource(
                                serverName = f"HLS Server {len(all_servers) + 1}",
                                m3u8Url    = first_m3u8,
                                qualities  = qualities if qualities else [QualityInfo(label="Auto", url=first_m3u8)],
                                referer    = link,
                            ))

                    except Exception as e:
                        logger.warning(f"[unified] embed fetch failed for {embed_id}: {e}")
                        continue

            if not all_servers:
                raise HTTPException(
                    404,
                    f"No {stream_type} CDN sources found for episode {episode_id}. "
                    "The episode may not have this track on any available server."
                )

            result = EpisodeResponse(
                episodeId  = episode_id,
                type       = stream_type,
                servers    = all_servers,
                cdnDomain  = main_cdn,
                subtitles  = all_subtitles,
                # Placeholder timestamps — replace with real AniSkip lookup if desired
                timestamps = Timestamps(intro=[1.5, 90.0], outro=[1100.0, 1200.0]),
            )
            cache.set(key, result, TTL["unified_stream"])
            return result

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unified stream fetch failed for {episode_id}: {e}")
            raise HTTPException(502, f"Failed to fetch stream: {e}")

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
    logger.info("🚀 RO-Anime API v10.1 Online")
    yield
    await http_client.stop()
    executor.shutdown()

app = FastAPI(lifespan=lifespan, title="RO-Anime API v10.1")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
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
    response   = await call_next(request)
    process_ms = (time.perf_counter() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = f"{process_ms:.2f}"
    response.headers["X-Powered-By"]      = "RO-Anime/10.1"
    return response

# ── Routes ─────────────────────────────────────

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
async def route_stream(
    episode_id: str,
    type: str = Query(default="sub", pattern="^(sub|dub)$"),
):
    res = await get_stream(episode_id, type)
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/stream/v2/{episode_id}")
async def route_stream_v2(episode_id: str):
    res = await get_source_v2(episode_id)
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/api/v10/stream/{episode_id}")
async def route_unified_stream(
    episode_id: str,
    type: str = Query(default="sub", pattern="^(sub|dub)$"),
):
    """
    Primary stream endpoint the player uses.
    Returns EpisodeResponse JSON:
      { episodeId, type, servers[], cdnDomain, subtitles[], timestamps }
    Only CDN URLs whose audio track matches the requested type are returned.
    """
    res = await get_unified_stream(episode_id, type)
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/home/thumbnails")
async def route_home():
    """
    Returns a flat JSON array of {anime_id, title, poster}.
    The player uses poster URLs for the hero slider and full objects for the grid.
    """
    res = await get_home()
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/batch/search")
async def route_batch_search(q: List[str] = Query(...)):
    queries = list(dict.fromkeys(q[:20]))
    tasks   = [get_search(title) for title in queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out     = {
        t: (r if not isinstance(r, Exception) else {"error": str(r)})
        for t, r in zip(queries, results)
    }
    return Response(msgspec.json.encode(out), media_type="application/json")

@app.get("/health")
async def health():
    return {
        "status":   "ok",
        "cache":    cache.stats(),
        "circuit":  breaker.state,
        "inflight": len(http_client.inflight),
    }

if __name__ == "__main__":
    import uvicorn
    uvloop.install()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
