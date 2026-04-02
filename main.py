
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
API_VERSION          = "10.1"

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
logger = logging.getLogger(f"ro-anime-v{API_VERSION}")

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

    def invalidate(self, key: str) -> None:
        self._store.pop(key, None)

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
    Deduplicates by anime_id so the player never gets repeated cards.
    """
    parser  = LexborHTMLParser(html)
    results = []
    seen: set = set()
    for item in parser.css("div.flw-item"):
        img = item.css_first("img")
        if not img:
            continue
        title    = img.attributes.get("alt", "").strip()
        anime_id = item.attributes.get("data-id", "")
        if title and anime_id and anime_id not in seen:
            seen.add(anime_id)
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

def parse_server_ids(html: str) -> List[str]:
    """v10 helper: extract all data-id values for unified stream pipeline."""
    return re.findall(r'data-id="(\d+)"', html)

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
# SHARED RATE-LIMIT CHECK
# ──────────────────────────────────────────────
def _check_rate_limit(request: Request) -> None:
    ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(ip):
        raise HTTPException(429, "Rate limit exceeded — slow down")

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
            data     = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/list/{anime_id}", is_json=True
            )
            html     = data.get("html", "")
            loop     = asyncio.get_running_loop()
            episodes = await loop.run_in_executor(executor, parse_episodes, html)
            cache.set(key, episodes, TTL["episode"])
            return episodes
        except Exception as e:
            logger.error(f"episodes refresh failed for anime_id={anime_id}: {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_home() -> List[Dict]:
    key           = "home:index"
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


async def get_stream(episode_id: str) -> Optional[StreamInfo]:
    """Legacy single Vidcloud server stream (v9 style)."""
    key           = f"stream:{episode_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            servers_data = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/servers?id={episode_id}", is_json=True
            )
            servers_html = servers_data.get("html", "")
            loop         = asyncio.get_running_loop()
            sub_id, _    = await loop.run_in_executor(executor, parse_vidcloud_ids, servers_html)
            if not sub_id:
                return None

            source_data = await http_client.fetch(
                f"{BASE_URL}/ajax/episode/sources?id={sub_id}", is_json=True
            )
            url = source_data.get("url") or source_data.get("link")
            if not url:
                return None

            result = StreamInfo(url=url, source="Vidcloud", server_id=sub_id)
            cache.set(key, result, TTL["stream"])
            return result
        except Exception as e:
            logger.error(f"stream refresh failed for ep={episode_id}: {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


async def get_source_v2(episode_id: str, stream_type: str = "sub") -> Optional[Dict]:
    """
    v10 stream source via MEW_BASE — with &type= and &_= date param.
    BUG-4 fix: passes &type=  BUG-5 fix: passes &_= date param.
    """
    key           = f"stream_v2:{episode_id}:{stream_type}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        try:
            date_param   = get_date_param()
            servers_url  = (
                f"{MEW_BASE}/ajax/episode/servers"
                f"?id={episode_id}&type={stream_type}&_={date_param}"
            )
            servers_data = await http_client.fetch(
                servers_url, referer=BASE_URL, origin=BASE_URL, is_json=True
            )
            servers_html = servers_data.get("html", "")
            server_ids   = parse_server_ids(servers_html)
            if not server_ids:
                return None

            first_id    = server_ids[0]
            source_url  = (
                f"{MEW_BASE}/ajax/episode/sources"
                f"?id={first_id}&type={stream_type}&_={date_param}"
            )
            source_data = await http_client.fetch(
                source_url, referer=BASE_URL, origin=BASE_URL, is_json=True
            )
            cache.set(key, source_data, TTL["stream_v2"])
            return source_data
        except Exception as e:
            logger.error(f"source_v2 refresh failed ep={episode_id} type={stream_type}: {e}")
            raise  # BUG-1 FIX

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


# ──────────────────────────────────────────────
# V10.1 CORE: UNIFIED STREAM  (sub-only CDN filter)
# ──────────────────────────────────────────────
async def get_unified_stream(
    episode_id:  str,
    stream_type: str = "sub",
) -> EpisodeResponse:
    """
    Full v10 pipeline — the player calls this via:
        GET /api/v10/stream/{episode_id}?type=sub|dub

    v10.1 SUB-ONLY FILTER:
      MEW_BASE returns a "type" field ("sub" | "dub") in each source JSON
      response.  Any source whose returned type does not match stream_type is
      skipped entirely — only matching CDN m3u8 URLs reach EpisodeResponse.servers.
      With the default stream_type="sub" the caller receives subtitle-track
      CDN URLs only; dub CDN URLs are never included.

    Also collects:
      - Quality ladder  (src_data["sources"] array → List[QualityInfo])
      - Subtitles       (src_data["tracks"] with kind="captions" → List[Subtitle])
      - Timestamps      (src_data["intro"] / src_data["outro"] → Timestamps)

    Bug fixes inherited from v10.0:
      BUG-1: all refresh() re-raise on exception
      BUG-2: origin=BASE_URL on all MEW/RapidCloud calls
      BUG-3: bare except → except Exception
      BUG-4: &type= forwarded to source endpoint
      BUG-5: &_= date param forwarded to both endpoints
      BUG-6: Origin header set in _do_fetch when origin != ""
      BUG-8: SmartCache fresh/SWR logic fixed
    """
    key           = f"unified_stream:{episode_id}:{stream_type}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh() -> EpisodeResponse:
        try:
            date_param = get_date_param()

            # ── Step 1: fetch server list ────────────────────────────────
            servers_url  = (
                f"{MEW_BASE}/ajax/episode/servers"
                f"?id={episode_id}&_={date_param}"
            )
            servers_data = await http_client.fetch(
                servers_url,
                referer=BASE_URL,
                origin=BASE_URL,        # BUG-2 fix
                is_json=True,
            )
            servers_html = servers_data.get("html", "")
            server_ids   = parse_server_ids(servers_html)

            if not server_ids:
                logger.warning(f"No server IDs found for ep={episode_id}")

            # ── Step 2: fetch each source; keep only stream_type matches ─
            sources:    List[ServerSource] = []
            subtitles:  List[Subtitle]     = []
            timestamps: Optional[Timestamps] = None

            for sid in server_ids:
                source_url = (
                    f"{MEW_BASE}/ajax/episode/sources"
                    f"?id={sid}"
                    f"&type={stream_type}"        # BUG-4 fix
                    f"&_={date_param}"            # BUG-5 fix
                )
                try:
                    src_data = await http_client.fetch(
                        source_url,
                        referer=BASE_URL,
                        origin=BASE_URL,          # BUG-2 fix
                        is_json=True,
                    )
                except Exception as e:            # BUG-3 fix — no bare except
                    logger.warning(f"Source fetch failed sid={sid}: {e}")
                    continue

                # ── v10.1 SUB-ONLY FILTER ────────────────────────────────
                returned_type = src_data.get("type", "").lower()
                if returned_type != stream_type:
                    logger.debug(
                        f"Skipping sid={sid}: returned type='{returned_type}' "
                        f"!= requested='{stream_type}'"
                    )
                    continue
                # ─────────────────────────────────────────────────────────

                m3u8_url = src_data.get("url") or src_data.get("link", "")
                if not m3u8_url:
                    logger.debug(f"No m3u8 URL in sid={sid} response")
                    continue

                # Quality ladder — use explicit sources array if present
                raw_qualities: List[Dict] = src_data.get("sources", [])
                qualities: List[QualityInfo] = (
                    [
                        QualityInfo(
                            label=q.get("label", "auto"),
                            url=q.get("file", m3u8_url),
                            bitrate=q.get("bitrate"),
                        )
                        for q in raw_qualities
                        if q.get("file")
                    ]
                    if raw_qualities
                    else [QualityInfo(label="auto", url=m3u8_url)]
                )

                sources.append(ServerSource(
                    serverName=src_data.get("server", f"server-{sid}"),
                    m3u8Url=m3u8_url,
                    qualities=qualities,
                    referer=BASE_URL,
                ))

                # Collect subtitles from first successful source only
                if not subtitles:
                    for track in src_data.get("tracks", []):
                        if track.get("kind") == "captions":
                            subtitles.append(Subtitle(
                                file=track.get("file", ""),
                                label=track.get("label", "English"),
                                default=track.get("default", False),
                            ))

                # Collect timestamps from first successful source only
                if timestamps is None and "intro" in src_data:
                    intro = src_data["intro"]
                    outro = src_data.get("outro", {})
                    timestamps = Timestamps(
                        intro=[intro.get("start", 0.0), intro.get("end", 0.0)],
                        outro=[outro.get("start", 0.0), outro.get("end", 0.0)],
                    )

            # ── Step 3: build response ───────────────────────────────────
            cdn_domain = urlparse(sources[0].m3u8Url).netloc if sources else ""

            response = EpisodeResponse(
                episodeId=episode_id,
                type=stream_type,
                servers=sources,
                cdnDomain=cdn_domain,
                subtitles=subtitles,
                timestamps=timestamps,
            )
            cache.set(key, response, TTL["unified_stream"])
            return response

        except Exception as e:
            logger.error(
                f"unified_stream refresh failed ep={episode_id} type={stream_type}: {e}"
            )
            raise  # BUG-1 fix

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

# ──────────────────────────────────────────────
# APPLICATION LIFECYCLE
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    await http_client.start()
    logger.info(f"RO-Anime API v{API_VERSION} started")
    yield
    await http_client.stop()
    executor.shutdown(wait=False)
    logger.info("RO-Anime API shut down")

app = FastAPI(
    title="RO-Anime API",
    version=API_VERSION,
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":  "ok",
        "version": API_VERSION,
        "cache":   cache.stats(),
        "circuit": breaker.state,
    }


@app.get("/search")
async def search(
    request: Request,
    q: str   = Query(..., min_length=1, max_length=200),
):
    _check_rate_limit(request)
    results = await get_search(q)
    return {"results": results, "count": len(results)}


@app.get("/suggest")
async def suggest(
    request: Request,
    q: str   = Query(..., min_length=1, max_length=200),
):
    _check_rate_limit(request)
    results = await get_suggest(q)
    return {"results": results}


@app.get("/episodes/{anime_id}")
async def episodes(request: Request, anime_id: str):
    _check_rate_limit(request)
    eps = await get_episodes(anime_id)
    if not eps:
        raise HTTPException(404, f"No episodes found for anime_id={anime_id}")
    return {"anime_id": anime_id, "episodes": eps, "count": len(eps)}


@app.get("/home/thumbnails")
async def home_thumbnails(request: Request):
    """Returns the flat trending list the player expects — plain JSON array."""
    _check_rate_limit(request)
    return await get_home()


@app.get("/stream/{episode_id}")
async def stream(request: Request, episode_id: str):
    """Legacy v9 single-server Vidcloud stream."""
    _check_rate_limit(request)
    info = await get_stream(episode_id)
    if not info:
        raise HTTPException(404, f"No stream found for episode_id={episode_id}")
    return info


@app.get("/api/v2/stream/{episode_id}")
async def stream_v2(
    request:    Request,
    episode_id: str,
    type:       str = Query(default="sub", pattern="^(sub|dub)$"),
):
    """v2 stream — MEW_BASE with type + date params."""
    _check_rate_limit(request)
    data = await get_source_v2(episode_id, stream_type=type)
    if not data:
        raise HTTPException(404, f"No v2 source for episode_id={episode_id} type={type}")
    return data


@app.get("/api/v10/stream/{episode_id}")
async def stream_v10(
    request:    Request,
    episode_id: str,
    type:       str = Query(default="sub", pattern="^(sub|dub)$"),
):
    """
    Unified stream — v10.1.
    Returns ONLY CDN URLs whose audio track matches the requested type
    (default: sub).  404 when no matching sources are found.
    """
    _check_rate_limit(request)
    result = await get_unified_stream(episode_id, stream_type=type)
    if not result.servers:
        raise HTTPException(
            404,
            f"No {type} CDN sources found for episode_id={episode_id}. "
            "The episode may not have this track on any available server."
        )
    return result


@app.get("/cache/stats")
async def cache_stats():
    return cache.stats()


@app.get("/cache/invalidate/{key:path}")
async def cache_invalidate(key: str):
    cache.invalidate(key)
    return {"invalidated": key}
if __name__ == '__main__':
      import uvicorn
      uvloop.install()
      uvicorn.run(app , host = '0.0.0.0',port = 8000,log_level = 'warning')
