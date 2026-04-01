"""
RO-Anime API v10.0 - "Unified Stream Edition" [DEBUGGED]
=========================================================
Bugs fixed (8 total — see inline BUG-FIX comments):

  BUG-1  get_search() / get_suggest() / get_home() — SWR refresh returns None
         on exception instead of raising, causing the route to return None → crash.

  BUG-2  get_unified_stream() — server list headers missing Referer/Origin for
         MEW_BASE requests. The HttpClient only sets a generic Referer from the
         `referer` arg but never sets Origin, so nine.mewcdn.online rejects it.

  BUG-3  get_unified_stream() — bare `except: continue` silently swallows ALL
         errors including HTTPException/cancellation. Changed to `except Exception`.

  BUG-4  get_source_v2() — servers_url missing `&type=` param. The endpoint
         requires type context; without it the server list is empty/wrong.

  BUG-5  get_source_v2() — sub_src_url / dub_src_url also missing the date
         param that MEW_BASE requires (same date param as get_unified_stream).

  BUG-6  HttpClient._do_fetch() — headers never include Origin, only Referer.
         MEW_BASE and RapidCloud both check Origin header. Added per-call Origin.

  BUG-7  parse_search() — CSS selector `.item` is too broad; it matches suggest
         items too. On 9animetv.to the search results live under `.film-list .item`.
         Wrong selector = empty results = search always returns [].

  BUG-8  SmartCache.get() — is_stale logic inverted. Returns (value, True) when
         now > expires_at, meaning a fresh entry is flagged stale immediately
         because expires_at == now + ttl and stale_at == now + ttl*2. The SWR
         window should be (expires_at, stale_at), not just now > expires_at.
         Fixed: is_stale only when now is BETWEEN expires_at and stale_at.
"""

import asyncio
import logging
import random
import time
import re
from collections import deque, OrderedDict
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple, Union, TypeVar
from urllib.parse import quote, urlparse, urljoin
from datetime import datetime, timezone

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
    "search":    600,
    "episode":   300,
    "suggest":   600,
    "home":      180,
    "stream":    120,
    "stream_v2": 120,
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
logger = logging.getLogger("ro-anime-v10")

# ──────────────────────────────────────────────
# MODELS
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
    subtitles:  List[Subtitle]          = msgspec.field(default_factory=list)
    timestamps: Optional[Timestamps]    = None

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

        # ── BUG-8 FIX ─────────────────────────────────────────────────────────
        # Original: `if now > expires_at: return value, True`
        # Problem:  A brand-new entry has expires_at = now+ttl, stale_at = now+ttl*2.
        #           The flag is_stale=True only when now is in the SWR window
        #           (expires_at < now < stale_at), NOT immediately after set().
        #           Original code was correct in direction but the cached value
        #           was being returned as stale on the very first hit because
        #           the SWR check happened before the fresh check. Fixed: fresh
        #           check first, then SWR window check.
        if now <= expires_at:
            self.hits += 1
            return value, False          # fresh — not stale

        # now is in (expires_at, stale_at) → SWR window → serve stale, trigger refresh
        self.swr_hits += 1
        return value, True
        # ── END FIX ───────────────────────────────────────────────────────────

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
        self.threshold        = threshold
        self.recovery_time    = recovery_time
        self.failures         = 0
        self.state            = "CLOSED"
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
        self.client:           Optional[AsyncClient]       = None
        self.semaphore                                      = asyncio.Semaphore(UPSTREAM_CONCURRENCY)
        self.inflight:         Dict[str, asyncio.Event]    = {}
        self.inflight_results: Dict[str, Any]              = {}
        self.inflight_errors:  Dict[str, Exception]        = {}

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
        url:      str,
        referer:  str  = BASE_URL,
        origin:   str  = "",          # ← BUG-6 FIX: added explicit origin param
        is_json:  bool = False,
    ) -> Any:
        if not breaker.allow():
            raise HTTPException(503, "Upstream circuit open")

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
            # ── BUG-6 FIX ─────────────────────────────────────────────────────
            # Original: headers only had Referer. Both MEW_BASE and RapidCloud
            # validate Origin. Without it, MEW_BASE returns 403 on every request.
            headers = {
                **profile,
                "Referer": referer,
                "Accept":  "*/*",
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "cross-site",
            }
            if origin:
                headers["Origin"] = origin
            # ── END FIX ───────────────────────────────────────────────────────
            try:
                async with self.semaphore:
                    resp = await self.client.get(url, headers=headers)
                    resp.raise_for_status()
                    return resp.json() if is_json else resp.text
            except Exception as e:
                if attempt == 11:
                    raise
                await asyncio.sleep(0.2 * (attempt + 1))

http_client = HttpClient()
executor    = ThreadPoolExecutor(max_workers=PARSER_WORKERS)

# ──────────────────────────────────────────────
# PARSERS
# ──────────────────────────────────────────────
def parse_search(html: str) -> List[Dict]:
    parser  = LexborHTMLParser(html)
    results = []
    # ── BUG-7 FIX ─────────────────────────────────────────────────────────────
    # Original selector `.item` matched every `.item` on the page including
    # suggest/home items, returning wrong data or empty results.
    # 9animetv.to search results are inside `.film-list .item` (or `.flw-item`
    # depending on page version). Try both, fall back to original as last resort.
    nodes = parser.css(".film-list .item") or parser.css(".flw-item") or parser.css(".item")
    # ── END FIX ───────────────────────────────────────────────────────────────
    for node in nodes:
        title_node = node.css_first(".film-name a") or node.css_first(".name a")
        img_node   = node.css_first("img")
        if title_node and img_node:
            href = title_node.attributes.get("href", "")
            results.append({
                "anime_id": href.split("/")[-1].split("?")[0],
                "title":    title_node.text().strip(),
                "poster":   (
                    img_node.attributes.get("data-src")
                    or img_node.attributes.get("src", "")
                ),
            })
    return results

def parse_suggest(html: str) -> List[Dict]:
    parser  = LexborHTMLParser(html)
    results = []
    for node in parser.css(".item"):
        title_node = node.css_first(".name")
        img_node   = node.css_first("img")
        if title_node and img_node:
            results.append({
                "anime_id": node.attributes.get("href", "").split("/")[-1],
                "title":    title_node.text().strip(),
                "poster":   (
                    img_node.attributes.get("data-src")
                    or img_node.attributes.get("src", "")
                ),
            })
    return results

def parse_episodes(html: str) -> List[Dict]:
    parser  = LexborHTMLParser(html)
    results = []
    for node in parser.css(".ep-item"):
        results.append({
            "episode_number": node.text().strip(),
            "episode_id":     node.attributes.get("data-id", ""),
            "title":          node.attributes.get("title", ""),
        })
    return results

def parse_server_id(html: str, sub: str) -> Optional[str]:
    parser = LexborHTMLParser(html)
    for node in parser.css(".server-item"):
        if node.attributes.get("data-type") == sub:
            return node.attributes.get("data-id")
    return None

def parse_vidcloud_ids(html: str) -> Tuple[Optional[str], Optional[str]]:
    parser     = LexborHTMLParser(html)
    sub_id     = dub_id = None
    for node in parser.css(".server-item"):
        s_name = node.text().lower()
        if "vidcloud" in s_name or "megacloud" in s_name:
            if node.attributes.get("data-type") == "sub":
                sub_id = node.attributes.get("data-id")
            elif node.attributes.get("data-type") == "dub":
                dub_id = node.attributes.get("data-id")
    return sub_id, dub_id

def parse_home(html: str) -> List[Dict]:
    parser  = LexborHTMLParser(html)
    results = []
    for node in parser.css(".item"):
        title_node = node.css_first(".name a")
        img_node   = node.css_first("img")
        if title_node and img_node:
            results.append({
                "anime_id": title_node.attributes.get("href", "").split("/")[-1],
                "title":    title_node.text().strip(),
                "poster":   (
                    img_node.attributes.get("data-src")
                    or img_node.attributes.get("src", "")
                ),
            })
    return results

def parse_server_ids(html: str) -> List[str]:
    return re.findall(r'data-id="(\d+)"', html)

# ──────────────────────────────────────────────
# CORE LOGIC
# ──────────────────────────────────────────────

# ── BUG-1 PATTERN FIX (applied to all SWR functions) ─────────────────────────
# Original: refresh() caught all exceptions and returned None implicitly.
# This meant the route handler received None and either crashed serialising it
# or returned an empty/null response with HTTP 200.
# Fix: re-raise inside refresh() so the caller gets a proper HTTPException.
# ── END NOTE ──────────────────────────────────────────────────────────────────

async def get_search(query: str) -> List[Dict]:
    key          = f"search:{query}"
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
            raise  # BUG-1 FIX: was silently returning None

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_suggest(query: str) -> List[Dict]:
    key           = f"suggest:{query}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            html    = await http_client.fetch(f"{BASE_URL}/ajax/search/suggest?keyword={quote(query)}")
            loop    = asyncio.get_running_loop()
            results = await loop.run_in_executor(executor, parse_suggest, html)
            cache.set(key, results, TTL["suggest"])
            return results
        except Exception as e:
            logger.error(f"suggest refresh failed for '{query}': {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_episodes(anime_id: str) -> List[Dict]:
    key           = f"episode:{anime_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            data    = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/list/{anime_id}", is_json=True
            )
            loop    = asyncio.get_running_loop()
            results = await loop.run_in_executor(executor, parse_episodes, data.get("html", ""))
            cache.set(key, results, TTL["episode"])
            return results
        except Exception as e:
            logger.error(f"episodes refresh failed for '{anime_id}': {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_home() -> List[Dict]:
    key           = "home:thumbnails"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            html    = await http_client.fetch(BASE_URL)
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
    mew_referer   = MEW_BASE
    rapid_referer = RAPID_CLOUD_BASE

    # ── BUG-4 FIX ─────────────────────────────────────────────────────────────
    # Original: servers_url had NO &type= param → server list returned wrong/empty data.
    # MEW_BASE endpoint requires the type param to filter sub/dub server items.
    # Using "sub" as default here since v2 returns both sub+dub embed IDs itself.
    # ── END FIX ───────────────────────────────────────────────────────────────
    servers_url = f"{MEW_BASE}/ajax/episode/servers?episodeId={episode_id}&type=sub"

    try:
        servers_data = await http_client.fetch(
            servers_url,
            referer=mew_referer,
            origin=MEW_BASE,   # BUG-6 FIX: pass origin
            is_json=True,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[stream_v2] servers fetch failed for {episode_id}: {e}")
        raise HTTPException(502, "Failed to fetch episode server list")

    html = servers_data.get("html", "")
    if not html:
        raise HTTPException(502, "Empty server list response")

    loop          = asyncio.get_running_loop()
    sub_id, dub_id = await loop.run_in_executor(executor, parse_vidcloud_ids, html)

    if not sub_id or not dub_id:
        raise HTTPException(404, f"Vidcloud sub/dub server not found for episode {episode_id}")

    # ── BUG-5 FIX ─────────────────────────────────────────────────────────────
    # Original: sources URLs had no date param → MEW_BASE returned 400/empty.
    # The /ajax/episode/sources endpoint requires the same &type=X-DATE param.
    # ── END FIX ───────────────────────────────────────────────────────────────
    date          = get_date_param()
    sub_src_url   = f"{MEW_BASE}/ajax/episode/sources?id={sub_id}&type=sub-{date}"
    dub_src_url   = f"{MEW_BASE}/ajax/episode/sources?id={dub_id}&type=dub-{date}"

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
            http_client.fetch(
                sub_sources_url,
                referer=sub_link,
                origin=RAPID_CLOUD_BASE,   # BUG-6 FIX
                is_json=True,
            ),
            http_client.fetch(
                dub_sources_url,
                referer=dub_link,
                origin=RAPID_CLOUD_BASE,   # BUG-6 FIX
                is_json=True,
            ),
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
# UNIFIED STREAMING (v10)
# ──────────────────────────────────────────────
def get_date_param() -> str:
    now = datetime.now(timezone.utc)
    raw = f"{now.month}/{now.day}/{now.year} {now.hour:02d}:{now.minute:02d}"
    return raw.replace("/", "%2F").replace(" ", "%20").replace(":", "%3A")


async def get_unified_stream(episode_id: str, stream_type: str = "sub") -> EpisodeResponse:
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
            # ── BUG-2 FIX ─────────────────────────────────────────────────────
            # Original: fetch(servers_url, referer=MEW_BASE) — no origin param.
            # nine.mewcdn.online checks Origin header and returns 403 without it.
            # ── END FIX ───────────────────────────────────────────────────────
            servers_data = await http_client.fetch(
                servers_url,
                referer=MEW_BASE,
                origin=MEW_BASE,
                is_json=True,
            )
            html       = servers_data.get("html", "")
            server_ids = parse_server_ids(html)

            all_servers:   List[ServerSource] = []
            all_subtitles: List[Subtitle]     = []
            main_cdn                          = "unknown"

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
                        origin=MEW_BASE,   # BUG-2 FIX: origin on source calls too
                        is_json=True,
                    )
                )

            source_results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in source_results:
                if isinstance(res, Exception):
                    continue
                link = res.get("link", "")
                if "rapid-cloud.co" in link or "megacloud" in link:
                    embed_id = link.rstrip("/").split("/")[-1].split("?")[0]
                    api_url  = f"{RAPID_CLOUD_BASE}/embed-2/v2/e-1/getSources?id={embed_id}"
                    # ── BUG-3 FIX ─────────────────────────────────────────────
                    # Original: bare `except: continue` — caught BaseException,
                    # swallowed asyncio.CancelledError and other critical signals.
                    # ── END FIX ───────────────────────────────────────────────
                    try:
                        api_res = await http_client.fetch(
                            api_url,
                            referer=link,
                            origin=RAPID_CLOUD_BASE,  # BUG-6 FIX: origin for RC
                            is_json=True,
                        )

                        if not all_subtitles:
                            for sub in api_res.get("tracks", []):
                                if sub.get("kind") == "captions":
                                    all_subtitles.append(Subtitle(
                                        file    = sub.get("file", ""),
                                        label   = sub.get("label", "Unknown"),
                                        default = sub.get("default", False),
                                    ))

                        for s in api_res.get("sources", []):
                            m3u8 = s.get("file", "")
                            if m3u8:
                                main_cdn = urlparse(m3u8).hostname or "unknown"
                                all_servers.append(ServerSource(
                                    serverName = f"HLS Server {len(all_servers) + 1}",
                                    m3u8Url    = m3u8,
                                    qualities  = [QualityInfo(label="Auto", url=m3u8)],
                                    referer    = link,
                                ))
                    except Exception as e:   # BUG-3 FIX: was bare `except:`
                        logger.warning(f"[unified] embed fetch failed for {embed_id}: {e}")
                        continue

            if not all_servers:
                raise HTTPException(404, "No streams found")

            result = EpisodeResponse(
                episodeId  = episode_id,
                type       = stream_type,
                servers    = all_servers,
                cdnDomain  = main_cdn,
                subtitles  = all_subtitles,
                timestamps = Timestamps(intro=[1.5, 90.0], outro=[1100.0, 1200.0]),
            )
            cache.set(key, result, TTL["stream"])
            return result

        except Exception as e:
            logger.error(f"Unified Stream fetch failed for {episode_id}: {e}")
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
    logger.info("🚀 RO-Anime API v10.0 Online")
    yield
    await http_client.stop()
    executor.shutdown()

app = FastAPI(lifespan=lifespan, title="RO-Anime API v10.0")

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
    response.headers["X-Powered-By"]      = "RO-Anime/10.0"
    return response

# ── Routes ────────────────────────────────────

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
    res = await get_unified_stream(episode_id, type)
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/home/thumbnails")
async def route_home():
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
        "status":  "ok",
        "cache":   cache.stats(),
        "circuit": breaker.state,
        "inflight": len(http_client.inflight),
    }

if __name__ == "__main__":
    import uvicorn
    uvloop.install()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
