

import asyncio
import logging
import random
import time
import copy
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
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "application/json, text/javascript, */*; q=0.01" if is_json else "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            }
            if extra_headers:
                headers.update(extra_headers)

            try:
                async with self.semaphore:
                    resp = await self.client.get(url, headers=headers)
                    if resp.status_code == 403:
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        continue
                    resp.raise_for_status()
                    return resp.json() if is_json else resp.text
            except Exception as e:
                if attempt == 11: raise
                await asyncio.sleep(random.uniform(0.2, 0.5))

http_client = HttpClient()
executor = ThreadPoolExecutor(max_workers=PARSER_WORKERS)

# ──────────────────────────────────────────────
# PARSERS
# ──────────────────────────────────────────────
def parse_search(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    results = []
    for node in parser.css("div.item"):
        try:
            link = node.css_first("a.name")
            img = node.css_first("img")
            if link and img:
                results.append({
                    "anime_id": link.attributes.get("href", "").split("/")[-1],
                    "title": link.text(strip=True),
                    "poster": img.attributes.get("src", ""),
                })
        except: continue
    return results

def parse_episodes(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    eps = []
    for node in parser.css("a.ep-item"):
        try:
            eps.append({
                "episode_number": node.attributes.get("data-number", ""),
                "episode_id": node.attributes.get("data-id", ""),
                "title": node.attributes.get("title", ""),
            })
        except: continue
    return eps

def parse_home(html: str) -> List[Dict]:
    parser = LexborHTMLParser(html)
    results = []
    for node in parser.css("div.item"):
        try:
            link = node.css_first("a.name")
            img = node.css_first("img")
            if link and img:
                results.append({
                    "anime_id": link.attributes.get("href", "").split("/")[-1],
                    "title": link.text(strip=True),
                    "poster": img.attributes.get("src", ""),
                })
        except: continue
    return results

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
            res = await loop.run_in_executor(executor, parse_search, html)
            cache.set(key, res, TTL["search"])
            return res
        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_episodes(anime_id: str) -> List[Dict]:
    key = f"eps:{anime_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale: return val

    async def refresh():
        try:
            data = await http_client.fetch(f"{BASE_URL}/ajax/episode/list/{anime_id}", is_json=True)
            html = data.get("result", "")
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(executor, parse_episodes, html)
            cache.set(key, res, TTL["episode"])
            return res
        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_home() -> List[Dict]:
    key = "home:thumbnails"
    val, is_stale = cache.get(key)
    if val and not is_stale: return val

    async def refresh():
        try:
            html = await http_client.fetch(BASE_URL)
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

async def _fetch_source_from_sid(sid: str) -> Optional[Dict]:
    """Fetch VOD link and tracks for a single server ID."""
    try:
        src = await http_client.fetch(
            f"{BASE_URL}/ajax/episode/sources?id={sid}",
            is_json=True
        )
        link = src.get("link", "")
        if "rapid-cloud.co" in link or "megacloud" in link:
            embed_id = link.split("/")[-1].split("?")[0]
            sources_data = await http_client.fetch(
                f"https://rapid-cloud.co/embed-2/v2/e-1/getSources?id={embed_id}",
                headers={
                    "Referer": link,
                    "User-Agent": "Mozilla/5.0"
                },
                is_json=True
            )
            sources = sources_data.get("sources", [])
            tracks = sources_data.get("tracks", [])
            if sources:
                file = sources[0].get("file", "")
                if "vod" in file:
                    return {
                        "m3u8": file,
                        "subtitles": copy.deepcopy(tracks)
                    }
    except Exception as e:
        logger.debug(f"Failed to fetch source for sid {sid}: {e}")
    return None

async def get_stream(episode_id: str) -> dict:
    key = f"stream:{episode_id}"
    val, is_stale = cache.get(key)

    if val and not is_stale:
        return val

    async def refresh():
        try:
            servers_data = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/servers?episodeId={episode_id}",
                is_json=True
            )
            
            html_content = servers_data["html"]
            logger.info(f"DEBUG HTML: {html_content}")
            parser = LexborHTMLParser(html_content)
            
            # Group servers by type (sub/dub)
            # 9anime usually has divs with class 'servers-sub' and 'servers-dub'
            # Each containing '.item.server-item'
            sub_sids = []
            dub_sids = []
            
            sub_container = parser.css_first(".servers-sub")
            if sub_container:
                sub_sids = [n.attributes.get("data-id") for n in sub_container.css(".item.server-item") if n.attributes.get("data-id")]
            
            dub_container = parser.css_first(".servers-dub")
            if dub_container:
                dub_sids = [n.attributes.get("data-id") for n in dub_container.css(".item.server-item") if n.attributes.get("data-id")]
                
            # Fallback: if no containers, just take all and hope for the best (original logic but faster)
            if not sub_sids and not dub_sids:
                all_sids = [n.attributes.get("data-id") for n in parser.css(".item.server-item") if n.attributes.get("data-id")]
                sub_sids = all_sids
                dub_sids = all_sids

            # Fetch sources in parallel
            sub_tasks = [_fetch_source_from_sid(sid) for sid in sub_sids]
            dub_tasks = [_fetch_source_from_sid(sid) for sid in dub_sids]
            
            # Combine all tasks to fetch concurrently
            all_results = await asyncio.gather(*(sub_tasks + dub_tasks))
            
            sub_results = [r for r in all_results[:len(sub_tasks)] if r]
            dub_results = [r for r in all_results[len(sub_tasks):] if r]
            
            sub_data = sub_results[0] if sub_results else None
            dub_data = dub_results[0] if dub_results else None
            
            # Fallback logic
            fallback = f"https://megaplay.buzz/stream/s-2/{episode_id}/sub"
            
            if not sub_data:
                sub_data = {"m3u8": fallback, "subtitles": []}
            if not dub_data:
                dub_data = {"m3u8": sub_data["m3u8"] if sub_data else fallback, "subtitles": sub_data["subtitles"] if sub_data else []}

            result = {
                "episode_id": episode_id,
                "sub": sub_data,
                "dub": dub_data
            }

            cache.set(key, result, TTL["stream"])
            return result

        except Exception as e:
            logger.error(f"SWR Refresh failed for {key}: {e}")
            return {"episode_id": episode_id, "