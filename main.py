""": v10 unified stream pipeline with VidStream/VidCloud server naming,
           SmartCache (LRU+SWR), HttpClient with origin/referer, /proxy route,
           parse_sub_server_ids, get_unified_stream, get_date_param.
           HLS proxy (/stream.m3u8, /chunk) kept intact via curl_cffi.

Frontend : Full RO-Anime premium UI (black/pink Netflix-style theme).
           Streaming JS fully integrated calling local /api/v10/stream/{id}.
           Subtitle system v4 (FIXED):
             - Per-episode token prevents stale roSubAutoSelectDone across episodes
             - Auto-select delay reduced 800ms → 200ms for faster subtitle appearance
             - Hardened VTT validation: rejects HTML error pages from public proxies
             - Fast-path: if direct URL works, skip remaining CDN candidates immediately
             - roSwitchSubtitle allows retry on tracks that previously failed
             - roAutoFallbackToNextTrack also considers 'loading' state tracks
             - Multi-language detection from API tracks[]
             - Enhanced VTT validation — rejects empty/broken files
             - 5-tier CDN fallback for .vtt files
               (direct → alt CDN hostname → /proxy → corsproxy.io → allorigins.win)
             - Automatic fallback to next valid subtitle track if current fails
             - Subtitle state reset on episode/server change
             - Duplicate-track deduplication
             - Working clickable dropdown with smooth switching
             - Auto-selects default/English track on load
             - Real-time subtitle rendering with no gaps
             - Subtitle validation: checks WEBVTT header + cue count
             - Backend subtitle validation endpoint added (/api/v10/subtitle/validate)
             - Graceful degradation — never blocks playback
           Playback reliability:
             - Banned CDN filter (storm/strom/crimsonstorm)
             - 4-second manifest timeout → auto next server
             - 5-second stall watchdog → auto next server
             - Infinite retry loop with re-fetch on full exhaustion
             - "Video not available yet" shown when truly exhausted
"""

import asyncio
import logging
import random
import re
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, quote, unquote

import msgspec
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from httpx import AsyncClient, Limits, Timeout
from selectolax.lexbor import LexborHTMLParser

from curl_cffi import requests as cf_requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
MEW_BASE         = "https://nine.mewcdn.online"
RAPID_CLOUD_BASE = "https://rapid-cloud.co"
BASE_URL         = "https://9animetv.to"

UPSTREAM_CONCURRENCY = 15
CACHE_MAX_SIZE       = 3000
PARSER_WORKERS       = 4

TTL = {
    "unified_stream": 120,
    "home":           180,
    "search":         600,
    "suggest":        600,
    "episode":        300,
}

# Server display names — mapped by index order returned from API
# First server is usually VidCloud (RapidCloud), subsequent ones vary.
# We label them descriptively so users know what they're switching to.
SERVER_LABELS = [
    "VidCloud",
    "VidStream",
    "StreamSB",
    "MegaCloud",
    "StreamTape",
    "DoodStream",
]

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
class JSONFormatter(logging.Formatter):
    def format(self, r):
        d = {"ts": self.formatTime(r), "lvl": r.levelname, "msg": r.getMessage()}
        if r.exc_info:
            d["exc"] = self.formatException(r.exc_info)
        return msgspec.json.encode(d).decode()

_h = logging.StreamHandler()
_h.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_h])
logger = logging.getLogger("anistream-v10")

# ─────────────────────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────────────────────
class QualityInfo(msgspec.Struct):
    label:   str
    url:     str
    bitrate: Optional[int] = None

class Subtitle(msgspec.Struct):
    file:    str
    label:   str
    kind:    str   = "captions"
    default: bool  = False

class ServerSource(msgspec.Struct):
    serverName:  str
    serverLabel: str
    m3u8Url:     str
    qualities:   List[QualityInfo]
    referer:     str

class EpisodeResponse(msgspec.Struct):
    episodeId:  str
    type:       str
    servers:    List[ServerSource]
    cdnDomain:  str
    subtitles:  List[Subtitle]  = msgspec.field(default_factory=list)

# ─────────────────────────────────────────────────────────────────────────────
# BROWSER PROFILES
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# SMART CACHE  (LRU + SWR)
# ─────────────────────────────────────────────────────────────────────────────
class SmartCache:
    def __init__(self, max_size: int = 3000):
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
        if now <= expires_at:
            self.hits += 1
            return value, False
        self.swr_hits += 1
        return value, True

    def set(self, key: str, value: Any, ttl: int) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        elif len(self._store) >= self._max:
            self._store.popitem(last=False)
        now = time.monotonic()
        self._store[key] = (value, now + ttl, now + ttl * 2)

    def stats(self) -> Dict:
        total = self.hits + self.misses + self.swr_hits
        return {
            "size":     len(self._store),
            "hits":     self.hits,
            "swr_hits": self.swr_hits,
            "misses":   self.misses,
            "hit_rate": round((self.hits + self.swr_hits) / total, 3) if total else 0.0,
        }

cache = SmartCache(CACHE_MAX_SIZE)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP CLIENT
# ─────────────────────────────────────────────────────────────────────────────
class HttpClient:
    def __init__(self):
        self.client: Optional[AsyncClient] = None
        self.sem = asyncio.Semaphore(UPSTREAM_CONCURRENCY)

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
        referer: str  = "",
        origin:  str  = "",
        is_json: bool = False,
    ) -> Any:
        for attempt in range(8):
            profile = BROWSER_PROFILES[attempt % len(BROWSER_PROFILES)]
            headers = {
                **profile,
                "Referer":         referer or BASE_URL,
                "Accept":          "*/*",
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                "Connection":      "keep-alive",
                "Sec-Fetch-Dest":  "empty",
                "Sec-Fetch-Mode":  "cors",
                "Sec-Fetch-Site":  "cross-site",
            }
            if origin:
                headers["Origin"] = origin
            try:
                async with self.sem:
                    resp = await self.client.get(url, headers=headers)
                    resp.raise_for_status()
                    return resp.json() if is_json else resp.text
            except Exception:
                if attempt == 7:
                    raise
                delay = (
                    (0.3 * (2 ** attempt)) + random.uniform(0, 0.2)
                    if attempt < 3
                    else 1.5 + random.uniform(0, 0.5)
                )
                await asyncio.sleep(delay)

    async def fetch_text_raw(
        self,
        url:     str,
        referer: str = "",
        origin:  str = "",
        timeout: float = 10.0,
    ) -> Optional[str]:
        """Fetch raw text with a single attempt — used for subtitle validation."""
        profile = random.choice(BROWSER_PROFILES)
        headers = {
            **profile,
            "Referer":         referer or BASE_URL,
            "Accept":          "*/*",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Connection":      "keep-alive",
        }
        if origin:
            headers["Origin"] = origin
        try:
            async with self.sem:
                resp = await self.client.get(url, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp.text
        except Exception as e:
            logger.debug(f"[subtitle-backend] fetch_text_raw failed for {url[:60]}: {e}")
            return None

http_client = HttpClient()
executor    = ThreadPoolExecutor(max_workers=PARSER_WORKERS)

# ─────────────────────────────────────────────────────────────────────────────
# curl_cffi session for HLS proxy  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────
PROXY_HEADERS = {
    "Accept":             "*/*",
    "Accept-Encoding":    "gzip, deflate, br, zstd",
    "Accept-Language":    "en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7",
    "Connection":         "keep-alive",
    "Origin":             "https://rapid-cloud.co",
    "Referer":            "https://rapid-cloud.co/",
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "cross-site",
    "User-Agent":         "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36",
}
cf_session = cf_requests.Session()

# ─────────────────────────────────────────────────────────────────────────────
# SUBTITLE VALIDATION HELPERS (BACKEND)
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_vtt_text(text: str) -> bool:
    """
    Validate that a string is a genuine, non-empty WebVTT subtitle file.
    Checks:
      1. Starts with WEBVTT header (required by spec)
      2. Contains at least one valid timecode line (HH:MM:SS.mmm --> HH:MM:SS.mmm)
      3. Has at least one non-empty cue body line
    Returns False for empty files, HTML error pages, or malformed VTT.
    """
    if not text or len(text.strip()) < 20:
        return False
    stripped = text.strip()
    # Must start with WEBVTT (case-insensitive, allows BOM)
    if not re.search(r'^(?:\ufeff)?WEBVTT', stripped, re.IGNORECASE):
        return False
    # Must have at least one timecode line
    timecode_pattern = re.compile(
        r'\d{1,2}:\d{2}:\d{2}[.,]\d{3}\s+-->\s+\d{1,2}:\d{2}:\d{2}[.,]\d{3}'
    )
    if not timecode_pattern.search(stripped):
        return False
    # Must have non-empty cue text (at least one non-blank line after a timecode)
    lines = stripped.split('\n')
    found_timecode = False
    for line in lines:
        if timecode_pattern.match(line.strip()):
            found_timecode = True
            continue
        if found_timecode and line.strip():
            return True  # found at least one cue body line
    return False


def count_vtt_cues(text: str) -> int:
    """Count the number of subtitle cues in a VTT string."""
    if not text:
        return 0
    timecode_pattern = re.compile(
        r'\d{1,2}:\d{2}:\d{2}[.,]\d{3}\s+-->\s+\d{1,2}:\d{2}:\d{2}[.,]\d{3}'
    )
    return len(timecode_pattern.findall(text))


async def fetch_and_validate_vtt_backend(url: str, referer: str = "") -> Tuple[bool, int]:
    """
    Fetch a VTT URL from the backend and validate it.
    Returns (is_valid: bool, cue_count: int).
    Used by /api/v10/subtitle/validate endpoint.
    """
    if not referer:
        if "vod.netmagcdn.com" in url or "rapid-cloud.co" in url:
            referer = "https://rapid-cloud.co/"
        elif "megacloud" in url:
            referer = "https://megacloud.tv/"
        else:
            referer = BASE_URL

    parsed = urlparse(referer)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    # Try direct URL first
    text = await http_client.fetch_text_raw(url, referer=referer, origin=origin)
    if text and is_valid_vtt_text(text):
        return True, count_vtt_cues(text)

    # Try alt CDN hostname (vod.netmagcdn.com)
    try:
        parsed_url = urlparse(url)
        alt_host   = "vod.netmagcdn.com"
        if parsed_url.hostname != alt_host:
            alt_url = parsed_url._replace(netloc=alt_host).geturl()
            alt_text = await http_client.fetch_text_raw(alt_url, referer=referer, origin=origin)
            if alt_text and is_valid_vtt_text(alt_text):
                return True, count_vtt_cues(alt_text)
    except Exception:
        pass

    return False, 0

# ─────────────────────────────────────────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────────────────────────────────────────
def parse_sub_server_ids(html: str) -> List[str]:
    """Extract all server IDs from the SUB block with fallback."""
    parser     = LexborHTMLParser(html)
    server_ids = []
    # Primary: find SUB block by title
    for block in parser.css("div.ps_-block"):
        title_div = block.css_first("div.ps__-title")
        if title_div and "sub" in title_div.text().lower():
            for item in block.css("div.item.server-item"):
                sid = item.attributes.get("data-id")
                if sid:
                    server_ids.append(sid)
            break
    # Fallback: flat items with data-type=sub
    if not server_ids:
        for item in parser.css("div.item.server-item"):
            if item.attributes.get("data-type", "").lower() == "sub":
                sid = item.attributes.get("data-id")
                if sid:
                    server_ids.append(sid)
    return server_ids

def parse_home(html: str) -> List[Dict]:
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

def parse_search(html: str) -> List[Dict]:
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

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_date_param() -> str:
    """URL-encoded datetime param required by MEW_BASE endpoints."""
    now = datetime.now(timezone.utc)
    raw = f"{now.month}/{now.day}/{now.year} {now.hour:02d}:{now.minute:02d}"
    return raw.replace("/", "%2F").replace(" ", "%20").replace(":", "%3A")

def is_banned_cdn(url: str) -> bool:
    """Ban storm/strom/crimsonstorm CDNs — blocked or unreliable."""
    low = url.lower()
    return "storm" in low or "strom" in low or "crimsonstorm" in low

def get_server_label(index: int) -> str:
    """Return a human-friendly server label by position."""
    if index < len(SERVER_LABELS):
        return SERVER_LABELS[index]
    return f"Server {index + 1}"

def fix_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(
        path  = parsed.path.replace('+', '%2B'),
        query = parsed.query.replace('+', '%2B') if parsed.query else parsed.query
    ).geturl()

def cf_fetch(url: str) -> cf_requests.Response:
    h = PROXY_HEADERS.copy()
    h["Host"] = urlparse(url).netloc
    return cf_session.get(fix_url(url), headers=h, impersonate="chrome110", allow_redirects=True)

def rewrite_m3u8(content: str, original_url: str, base_local: str) -> str:
    content     = content.replace('\r\n', '\n').replace('\r', '\n')
    base_remote = original_url.rsplit("/", 1)[0] + "/"
    out         = []
    for line in content.split('\n'):
        if not line.strip():
            out.append("")
            continue
        s = line.strip()
        if 'URI="' in line:
            def repl(m):
                uri = m.group(1)
                if not uri.startswith("http"):
                    uri = urljoin(base_remote, uri)
                uri     = uri.replace('+', '%2B')
                proxied = f'{base_local}/chunk?url={quote(uri, safe=":/?=&%")}'
                return f'URI="{proxied}"'
            line = re.sub(r'URI="([^"]+)"', repl, line)
            out.append(line)
            continue
        if s.startswith("#"):
            out.append(line)
            continue
        full    = s if s.startswith("http") else urljoin(base_remote, s)
        full    = full.replace('+', '%2B')
        proxied = f"{base_local}/chunk?url={quote(full, safe=':/?=&%')}"
        out.append(proxied)
    return "\n".join(out)

def is_m3u8(url: str, text: str) -> bool:
    return ".m3u8" in url or text.strip().startswith("#EXTM3U")

def deduplicate_subtitles(tracks: List[Dict]) -> List[Dict]:
    """Remove duplicate subtitle tracks by label (keep first occurrence)."""
    seen   = set()
    result = []
    for t in tracks:
        key = (t.get("label", "").strip().lower(), t.get("kind", "captions"))
        if key not in seen:
            seen.add(key)
            result.append(t)
    return result

# ─────────────────────────────────────────────────────────────────────────────
# V10 UNIFIED STREAM SERVICE
# ─────────────────────────────────────────────────────────────────────────────
async def get_unified_stream(episode_id: str) -> EpisodeResponse:
    key           = f"us:{episode_id}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh() -> EpisodeResponse:
        date        = get_date_param()
        servers_url = (
            f"{MEW_BASE}/ajax/episode/servers"
            f"?episodeId={episode_id}&type=sub-{date}"
        )

        try:
            servers_data = await http_client.fetch(
                servers_url, referer=MEW_BASE, origin=MEW_BASE, is_json=True
            )
        except Exception as e:
            logger.error(f"[v10] servers fetch failed for {episode_id}: {e}")
            raise HTTPException(502, "Failed to fetch episode server list")

        html       = servers_data.get("html", "")
        server_ids = parse_sub_server_ids(html)

        if not server_ids:
            raise HTTPException(404, f"No SUB servers found for episode {episode_id}")

        # Fetch all source links in parallel
        tasks = [
            http_client.fetch(
                f"{MEW_BASE}/ajax/episode/sources?id={sid}&type=sub-{date}",
                referer=MEW_BASE, origin=MEW_BASE, is_json=True,
            )
            for sid in server_ids
        ]
        source_results = await asyncio.gather(*tasks, return_exceptions=True)

        all_servers:       List[ServerSource] = []
        all_subtitles_raw: List[Dict]         = []

        for res in source_results:
            if isinstance(res, Exception):
                continue
            link = res.get("link", "")
            if not link:
                continue
                        # 1. Handle direct VTT files (e.g., megastatics)
            if ".vtt" in link:
                all_subtitles_raw.append({
                    "file":    link,
                    "label":   res.get("label", "Sub"),
                    "kind":    "captions",
                    "default": False,
                })
                continue

            # 2. Handle embed sources (RapidCloud, MegaCloud)
            if "rapid-cloud.co" not in link and "megacloud" not in link:
                continue

            embed_id = link.rstrip("/").split("/")[-1].split("?")[0]
            api_url  = f"{RAPID_CLOUD_BASE}/embed-2/v2/e-1/getSources?id={embed_id}"
            try:
                api_res = await http_client.fetch(
                    api_url, referer=link, origin=RAPID_CLOUD_BASE, is_json=True
                )

                # Collect all subtitle tracks from the embed API
                for sub in api_res.get("tracks", []):
                    kind = sub.get("kind", "")
                    file = sub.get("file", "")
                    if kind in ("captions", "subtitles") and file:
                        all_subtitles_raw.append({
                            "file":    file,
                            "label":   sub.get("label", "Unknown"),
                            "kind":    kind,
                            "default": bool(sub.get("default", False)),
                        })

                for s in api_res.get("sources", []):
                    m3u8 = s.get("file", "")
                    if not m3u8:
                        continue
                    if is_banned_cdn(m3u8):
                        logger.info(f"[v10] Banned CDN skipped: {m3u8[:60]}")
                        continue
                    idx   = len(all_servers)
                    label = get_server_label(idx)
                    all_servers.append(ServerSource(
                        serverName  = f"{label} (Server {idx + 1})",
                        serverLabel = label,
                        m3u8Url     = m3u8,
                        qualities   = [QualityInfo(label="Auto", url=m3u8)],
                        referer     = link,
                    ))
            except Exception as e:
                logger.warning(f"[v10] embed fetch failed for {embed_id}: {e}")
                continue

        if not all_servers:
            raise HTTPException(404, "No playable SUB streams found")

        # Sort: vod.netmagcdn (CF-free) first, everything else second
        def sort_key(s: ServerSource) -> int:
            return 0 if "vod.netmagcdn.com" in s.m3u8Url.lower() else 1

        all_servers.sort(key=sort_key)
        for i, s in enumerate(all_servers):
            label       = get_server_label(i)
            s.serverLabel = label
            s.serverName  = f"{label} (S{i + 1})"

        main_cdn = urlparse(all_servers[0].m3u8Url).hostname or "unknown"

        # Deduplicate subtitles
        deduped = deduplicate_subtitles(all_subtitles_raw)
        subtitles = [
            Subtitle(
                file    = t["file"],
                label   = t["label"],
                kind    = t.get("kind", "captions"),
                default = t.get("default", False),
            )
            for t in deduped
        ]

        result = EpisodeResponse(
            episodeId = episode_id,
            type      = "sub",
            servers   = all_servers,
            cdnDomain = main_cdn,
            subtitles = subtitles,
        )
        cache.set(key, result, TTL["unified_stream"])
        return result

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

# ─────────────────────────────────────────────────────────────────────────────
# AUXILIARY SERVICE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
async def get_search(query: str) -> List[Dict]:
    key           = f"search:{query.lower().strip()}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        html    = await http_client.fetch(f"{BASE_URL}/search?keyword={quote(query)}")
        loop    = asyncio.get_running_loop()
        results = await loop.run_in_executor(executor, parse_search, html)
        cache.set(key, results, TTL["search"])
        return results

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_suggest(query: str) -> List[Dict]:
    key           = f"sug:{query.lower().strip()}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        data = await http_client.fetch(
            f"{BASE_URL}/ajax/search/suggest?keyword={quote(query)}", is_json=True
        )
        html   = data.get("html", "")
        loop   = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, parse_suggest, html) if html else data.get("result", [])
        cache.set(key, result, TTL["suggest"])
        return result

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
        data = await http_client.fetch(
            f"{BASE_URL}/ajax/episode/list/{anime_id}", is_json=True
        )
        loop = asyncio.get_running_loop()
        eps  = await loop.run_in_executor(executor, parse_episodes, data.get("html", ""))
        cache.set(key, eps, TTL["episode"])
        return eps

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

async def get_home() -> List[Dict]:
    key           = "home"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        html    = await http_client.fetch(f"{BASE_URL}/home")
        loop    = asyncio.get_running_loop()
        results = await loop.run_in_executor(executor, parse_home, html)
        cache.set(key, results, TTL["home"])
        return results

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()

# ─────────────────────────────────────────────────────────────────────────────
# PROXY ENDPOINT  (subtitle VTT CDN fallback + m3u8 manifest proxy)
# ─────────────────────────────────────────────────────────────────────────────
async def _proxy_url(url: str, referer: str = "") -> Response:
    if not url:
        raise HTTPException(400, "Missing url")
    if not referer:
        if "vod.netmagcdn.com" in url or "rapid-cloud.co" in url:
            referer = "https://rapid-cloud.co/"
        elif "megacloud" in url:
            referer = "https://megacloud.tv/"
        else:
            referer = BASE_URL

    parsed = urlparse(referer)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    profile = random.choice(BROWSER_PROFILES)
    headers = {
        **profile,
        "Referer":         referer,
        "Origin":          origin,
        "Accept":          "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection":      "keep-alive",
    }
    async with http_client.sem:
        resp = await http_client.client.get(url, headers=headers, timeout=12.0)
        resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "application/octet-stream")
    body         = resp.text

    if ".m3u8" in url or "mpegurl" in content_type:
        base_url = url.rsplit("/", 1)[0] + "/"
        lines    = []
        for line in body.splitlines():
            s = line.strip()
            if s and not s.startswith("#") and not s.startswith("http"):
                s = base_url + s
            lines.append(s)
        body = "\n".join(lines)

    return Response(
        content    = body,
        media_type = content_type,
        headers    = {"Access-Control-Allow-Origin": "*"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# FRONTEND  —  Full RO-Anime premium UI with integrated v10 streaming JS
# ─────────────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RO-ANIME - Search &amp; Stream</title>
    <link rel="preconnect" href="https://myanimelist.net">
    <link rel="preconnect" href="https://cdn.noitatnemucod.net">
    <link rel="preconnect" href="https://cdn.jsdelivr.net">
    <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
    <style>
        /* ===== BLACK & PINK THEME - PREMIUM NETFLIX STYLE ===== */
        :root {
            --primary-black: #0a0a0a;
            --secondary-black: #1a1a1a;
            --tertiary-black: #2a2a2a;
            --accent-pink: #ff006e;
            --accent-pink-light: #ff1493;
            --accent-pink-dark: #c2185b;
            --accent-neon: #ff0080;
            --text-primary: #ffffff;
            --text-secondary: #e0e0e0;
            --text-tertiary: #a0a0a0;
            --border-color: #333333;
            --shadow-sm: 0 2px 8px rgba(255, 0, 110, 0.1);
            --shadow-md: 0 8px 24px rgba(255, 0, 110, 0.2);
            --shadow-lg: 0 16px 48px rgba(255, 0, 110, 0.3);
            --transition-fast: 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            --transition-smooth: 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
            --glow-pink: 0 0 20px rgba(255, 0, 110, 0.5);
            --glow-pink-strong: 0 0 40px rgba(255, 0, 110, 0.8);
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        html, body { width: 100%; height: 100%; overflow-x: hidden; }
        body {
            background: linear-gradient(135deg, var(--primary-black) 0%, #1a0a15 100%);
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
            color: var(--text-primary);
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            line-height: 1.6;
            background-attachment: fixed;
        }
        html { scroll-behavior: smooth; }
        ::-webkit-scrollbar { width: 10px; }
        ::-webkit-scrollbar-track { background: var(--secondary-black); }
        ::-webkit-scrollbar-thumb { background: var(--accent-pink); border-radius: 5px; transition: background var(--transition-fast); }
        ::-webkit-scrollbar-thumb:hover { background: var(--accent-pink-light); }
        .suggestion-item {
            padding: 12px 16px; cursor: pointer; color: var(--text-secondary);
            border-bottom: 1px solid var(--border-color);
            transition: background var(--transition-fast), color var(--transition-fast); font-size: 13px;
        }
        .suggestion-item.hovered { background: var(--tertiary-black); color: var(--accent-pink); }
        .live-badge {
            display: inline-block; background: var(--accent-pink); color: white;
            font-size: 10px; font-weight: 900; padding: 2px 7px; border-radius: 4px;
            letter-spacing: 1px; vertical-align: middle;
            animation: livePulse 1.6s ease-in-out infinite;
        }
        @keyframes livePulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .sub-dub-toggle { display: flex; align-items: center; gap: 8px; margin-bottom: 16px; }
        .subdub-btn {
            padding: 6px 18px; border-radius: 6px; border: 2px solid var(--border-color);
            background: var(--tertiary-black); color: var(--text-tertiary); font-size: 11px;
            font-weight: 800; letter-spacing: 1px; cursor: pointer;
            transition: all var(--transition-fast); text-transform: uppercase;
        }
        .subdub-btn.active {
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            border-color: var(--accent-pink); color: white;
            box-shadow: 0 4px 14px rgba(255, 0, 110, 0.4);
        }
        .subdub-btn:hover:not(.active) { border-color: var(--accent-pink); color: var(--accent-pink); }
        .stream-status { font-size: 10px; font-weight: 700; letter-spacing: 0.5px; color: var(--text-tertiary); text-transform: uppercase; margin-left: 4px; }
        .stream-status.loading { color: var(--accent-pink); }
        .stream-status.live    { color: #00e676; }
        .stream-status.error   { color: #ff5252; }
        /* ===== SPLASH SCREEN ===== */
        #splashScreen {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: var(--primary-black); display: flex; align-items: center;
            justify-content: center; z-index: 9999; opacity: 1;
            transition: opacity 0.6s ease-out;
        }
        #splashScreen.hidden { opacity: 0; pointer-events: none; }
        .splash-content { text-align: center; animation: splashPulse 2s ease-in-out infinite; }
        @keyframes splashPulse { 0%, 100% { transform: scale(1); opacity: 1; } 50% { transform: scale(1.1); opacity: 0.8; } }
        .splash-logo {
            width: 80px; height: 80px; margin: 0 auto 20px; border-radius: 50%;
            border: 3px solid var(--accent-pink); display: flex; align-items: center;
            justify-content: center; box-shadow: 0 0 40px rgba(255, 0, 110, 0.6);
            animation: splashGlow 2s ease-in-out infinite;
        }
        @keyframes splashGlow { 0%, 100% { box-shadow: 0 0 40px rgba(255, 0, 110, 0.6); } 50% { box-shadow: 0 0 80px rgba(255, 0, 110, 1); } }
        .splash-logo img { width: 90%; height: 90%; border-radius: 50%; object-fit: cover; }
        .splash-text {
            font-size: 24px; font-weight: 900; color: var(--accent-pink);
            letter-spacing: 2px; text-transform: uppercase; margin-bottom: 15px;
            animation: splashText 1.5s ease-in-out infinite;
        }
        @keyframes splashText { 0%, 100% { opacity: 1; transform: translateY(0); } 50% { opacity: 0.7; transform: translateY(-5px); } }
        .splash-dots { display: flex; justify-content: center; gap: 8px; margin-top: 15px; }
        .splash-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--accent-pink); animation: splashDot 1.2s ease-in-out infinite; }
        .splash-dot:nth-child(1) { animation-delay: 0s; }
        .splash-dot:nth-child(2) { animation-delay: 0.2s; }
        .splash-dot:nth-child(3) { animation-delay: 0.4s; }
        @keyframes splashDot { 0%, 100% { opacity: 0.3; transform: scale(0.8); } 50% { opacity: 1; transform: scale(1.2); } }
        /* ===== HEADER ===== */
        header {
            background: linear-gradient(180deg, var(--secondary-black) 0%, rgba(26,26,26,0.8) 100%);
            padding: 12px 20px; display: flex; align-items: center; justify-content: space-between;
            backdrop-filter: blur(10px); border-bottom: 2px solid var(--accent-pink);
            position: sticky; top: 0; z-index: 1000; box-shadow: var(--shadow-md);
            animation: slideDown 0.6s var(--transition-smooth); gap: 12px; flex-wrap: wrap;
        }
        @keyframes slideDown { from { opacity: 0; transform: translateY(-20px); } to { opacity: 1; transform: translateY(0); } }
        .header-left { display: flex; align-items: center; gap: 12px; min-width: 0; }
        .logo {
            color: var(--text-primary); font-weight: 900; font-size: 18px; letter-spacing: -1px;
            text-transform: uppercase; text-decoration: none; cursor: pointer;
            display: flex; align-items: center; gap: 8px; transition: all var(--transition-fast);
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; white-space: nowrap;
        }
        .logo:hover { transform: scale(1.05); filter: brightness(1.2); }
        .logo-gojo {
            width: 36px; height: 36px; border-radius: 50%; overflow: hidden;
            border: 2px solid var(--accent-pink); background: var(--tertiary-black);
            display: flex; align-items: center; justify-content: center; flex-shrink: 0;
            box-shadow: 0 0 20px rgba(255, 0, 110, 0.5); animation: pulse 2s ease-in-out infinite;
        }
        @keyframes pulse { 0%, 100% { box-shadow: 0 0 20px rgba(255, 0, 110, 0.5); } 50% { box-shadow: 0 0 40px rgba(255, 0, 110, 0.8); } }
        .logo-gojo img { width: 100%; height: 100%; object-fit: cover; }
        .search-container { display: flex; flex: 1; min-width: 200px; max-width: 500px; position: relative; gap: 8px; }
        .search-container input {
            flex: 1; padding: 10px 12px; border-radius: 6px; border: 2px solid var(--border-color);
            background: var(--tertiary-black); color: var(--text-primary); font-size: 13px; outline: none;
            transition: all var(--transition-fast);
        }
        .search-container input::placeholder { color: var(--text-tertiary); }
        .search-container input:focus { background: var(--secondary-black); border-color: var(--accent-pink); box-shadow: 0 0 20px rgba(255, 0, 110, 0.3); }
        .search-container button {
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            border: none; color: white; padding: 10px 12px; border-radius: 6px;
            cursor: pointer; font-weight: 700; font-size: 10px; transition: all var(--transition-fast);
            text-transform: uppercase; letter-spacing: 0.3px; box-shadow: 0 4px 15px rgba(255, 0, 110, 0.3);
            white-space: nowrap; flex-shrink: 0;
        }
        @media (min-width: 480px) { .search-container button { padding: 10px 14px; font-size: 10.5px; letter-spacing: 0.4px; } }
        @media (min-width: 768px) { .search-container button { padding: 10px 16px; font-size: 11px; letter-spacing: 0.5px; } }
        .search-container button:hover { transform: scale(1.05); box-shadow: 0 8px 25px rgba(255, 0, 110, 0.5); }
        .search-container button:active { transform: scale(0.98); }
        @media (max-width: 600px) {
            header { flex-direction: column; align-items: stretch; padding: 12px 16px; }
            .header-left { width: 100%; justify-content: center; margin-bottom: 8px; }
            .search-container { width: 100%; }
        }
        /* ===== HERO BANNER ===== */
        #heroBanner {
            position: relative; width: 100%; height: 250px; border-radius: 12px;
            overflow: hidden; margin-bottom: 20px; border: 2px solid var(--accent-pink);
            box-shadow: var(--shadow-lg); display: none;
        }
        @media (min-width: 768px)  { #heroBanner { height: 350px; margin-bottom: 30px; display: block; } }
        @media (min-width: 1024px) { #heroBanner { height: 450px; margin-bottom: 40px; } }
        .hero-slider-container { width: 100%; height: 100%; position: relative; }
        .hero-slide {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background-size: cover; background-position: center;
            opacity: 0; transition: opacity 0.8s ease-in-out; display: flex; align-items: flex-end;
        }
        .hero-slide.active { opacity: 1; z-index: 1; }
        .hero-slide-overlay {
            width: 100%; padding: 40px;
            background: linear-gradient(0deg, rgba(10,10,10,0.9) 0%, transparent 100%); color: white;
        }
        .hero-slide-title { font-size: 28px; font-weight: 900; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; text-shadow: 0 2px 10px rgba(0,0,0,0.5); }
        @media (min-width: 768px) { .hero-slide-title { font-size: 36px; } }
        .hero-slide-ep { font-size: 14px; font-weight: 700; color: var(--accent-pink); text-transform: uppercase; letter-spacing: 2px; }
        .hero-watch-btn {
            display: inline-block; margin-top: 14px; padding: 12px 32px;
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            color: white; border: none; border-radius: 8px; font-size: 14px; font-weight: 900;
            letter-spacing: 1.5px; text-transform: uppercase; cursor: pointer;
            box-shadow: var(--glow-pink-strong); transition: all var(--transition-smooth);
            position: relative; overflow: hidden;
        }
        .hero-watch-btn::before {
            content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
            transition: left 0.6s;
        }
        .hero-watch-btn:hover { transform: scale(1.08); box-shadow: var(--glow-pink-strong), 0 0 30px rgba(255,0,110,0.6); }
        .hero-watch-btn:hover::before { left: 100%; }
        .hero-controls { position: absolute; bottom: 20px; right: 20px; display: flex; gap: 10px; z-index: 10; }
        .hero-nav-btn {
            width: 40px; height: 40px; border: none; border-radius: 50%;
            background: rgba(255,0,110,0.2); color: var(--accent-pink);
            cursor: pointer; font-size: 16px; display: flex; align-items: center; justify-content: center;
            transition: all var(--transition-fast); backdrop-filter: blur(10px);
            border: 2px solid var(--accent-pink); box-shadow: var(--glow-pink);
        }
        @media (min-width: 768px) { .hero-nav-btn { width: 48px; height: 48px; font-size: 20px; } }
        .hero-nav-btn:hover { background: var(--accent-pink); color: var(--primary-black); transform: scale(1.15); box-shadow: var(--glow-pink-strong); }
        .hero-nav-btn:active { transform: scale(0.95); }
        /* ===== MAIN CONTENT ===== */
        main { padding: 20px 16px; max-width: 1600px; margin: 0 auto; }
        @media (min-width: 768px)  { main { padding: 30px 24px; } }
        @media (min-width: 1024px) { main { padding: 40px 24px; } }
        .section-title {
            font-size: 20px; font-weight: 800; margin: 25px 0 16px 0;
            border-bottom: 3px solid var(--accent-pink); padding-bottom: 12px;
            display: flex; align-items: center; gap: 10px;
            animation: fadeInUp 0.6s var(--transition-smooth);
        }
        @media (min-width: 768px)  { .section-title { font-size: 24px; margin: 30px 0 20px 0; padding-bottom: 14px; } }
        @media (min-width: 1024px) { .section-title { font-size: 28px; margin: 40px 0 24px 0; } }
        @keyframes fadeInUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
        .section-title::before {
            content: ''; width: 4px; height: 20px;
            background: linear-gradient(180deg, var(--accent-pink), var(--accent-neon));
            border-radius: 3px; animation: scaleY 0.8s var(--transition-smooth);
        }
        @media (min-width: 768px) { .section-title::before { height: 24px; width: 5px; } }
        @keyframes scaleY { from { transform: scaleY(0); } to { transform: scaleY(1); } }
        #grid, #famousGrid, #dynamicGrid {
            display: grid; grid-template-columns: repeat(3, 1fr);
            gap: 10px; margin-bottom: 30px;
            animation: fadeInUp 0.8s var(--transition-smooth) 0.2s both;
        }
        @media (min-width: 480px)  { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(4, 1fr); gap: 12px; } }
        @media (min-width: 768px)  { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 40px; } }
        @media (min-width: 1024px) { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(7, 1fr); gap: 18px; margin-bottom: 50px; } }
        @media (min-width: 1400px) { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(9, 1fr); gap: 20px; } }
        .card {
            background: var(--tertiary-black); border-radius: 12px; overflow: hidden;
            transition: all var(--transition-smooth); cursor: pointer; display: flex; flex-direction: column;
            border: 2px solid var(--border-color); box-shadow: var(--shadow-sm);
            position: relative; animation: fadeIn 0.6s ease-out; contain: layout paint;
        }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .card:hover {
            transform: translateY(-14px) scale(1.06); border-color: var(--accent-pink);
            box-shadow: var(--glow-pink-strong), 0 24px 48px rgba(0,0,0,0.6); z-index: 10;
        }
        .card-img-wrapper {
            position: relative; width: 100%; aspect-ratio: 2 / 3;
            background: linear-gradient(135deg, var(--secondary-black), var(--tertiary-black));
            overflow: hidden; flex-shrink: 0; border-radius: 10px 10px 0 0;
        }
        @supports not (aspect-ratio: 2/3) { .card-img-wrapper { padding-top: 150%; height: 0; } }
        .card-img-wrapper.loading { animation: shimmer 2s infinite; }
        @keyframes shimmer { 0% { background-position: -1000px 0; } 100% { background-position: 1000px 0; } }
        .card img {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%; object-fit: cover;
            opacity: 0; transition: opacity 0.6s ease-out, filter 0.6s ease-out, transform 0.6s ease-out;
            filter: blur(8px) saturate(0.5);
        }
        .card img.loaded { opacity: 1; filter: blur(0) saturate(1); animation: none; }
        .card:hover img.loaded { transform: scale(1.08); }
        .card-overlay {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background: linear-gradient(180deg, rgba(0,0,0,0.2) 0%, rgba(0,0,0,0.98) 100%);
            opacity: 0; transition: opacity var(--transition-fast);
            display: flex; align-items: flex-end; justify-content: center; padding-bottom: 14px;
            backdrop-filter: blur(3px);
        }
        .card:hover .card-overlay { opacity: 1; animation: overlayFade 0.3s ease-out; }
        @keyframes overlayFade { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        .card-watch-btn {
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            color: white; border: none; border-radius: 6px; padding: 9px 18px;
            font-size: 11px; font-weight: 900; letter-spacing: 1.2px; text-transform: uppercase;
            cursor: pointer; box-shadow: var(--glow-pink); transition: all var(--transition-fast);
            white-space: nowrap; position: relative; overflow: hidden;
        }
        .card-watch-btn::before {
            content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
            transition: left 0.5s;
        }
        .card-watch-btn:hover { transform: scale(1.12); box-shadow: var(--glow-pink-strong); }
        .card-watch-btn:hover::before { left: 100%; }
        .card-info {
            padding: 8px; flex-grow: 1; display: flex; flex-direction: column;
            justify-content: space-between; background: var(--tertiary-black);
        }
        @media (min-width: 768px)  { .card-info { padding: 10px; } }
        @media (min-width: 1024px) { .card-info { padding: 12px; } }
        .card-title {
            font-size: 11px; font-weight: 700; color: var(--text-primary); line-height: 1.3;
            margin-bottom: 4px; display: -webkit-box; -webkit-line-clamp: 2;
            -webkit-box-orient: vertical; overflow: hidden;
        }
        @media (min-width: 768px)  { .card-title { font-size: 12px; margin-bottom: 6px; } }
        @media (min-width: 1024px) { .card-title { font-size: 13px; } }
        .card-meta { font-size: 9px; color: var(--accent-pink); font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
        @media (min-width: 1024px) { .card-meta { font-size: 10px; } }
        #streamingSection { display: none; animation: fadeIn 0.6s ease-out; }
        #videoContainer { margin-bottom: 20px; }
        #videoContainer.hidden { display: none; }
        .player-wrapper {
            position: relative; width: 100%; aspect-ratio: 16 / 9; background: #000;
            border-radius: 12px; overflow: hidden; border: 2px solid var(--accent-pink);
            box-shadow: 0 0 0 1px rgba(255,0,110,0.1), 0 0 40px rgba(255,0,110,0.2), 0 20px 50px rgba(0,0,0,0.7);
            cursor: default; user-select: none; max-height: 65vh; transition: all 0.3s ease;
        }
        @media (min-width: 768px) {
            .player-wrapper { width: 50vw; height: 50vh; max-height: 50vh; aspect-ratio: auto; margin: 0 auto; }
        }
        .player-wrapper.ro-fs-active {
            position: fixed !important; top: 0 !important; left: 0 !important;
            width: 100vw !important; height: 100vh !important; max-height: 100vh !important;
            aspect-ratio: auto !important; border-radius: 0 !important; border: none !important;
            z-index: 99999 !important; margin: 0 !important; padding: 0 !important;
            box-shadow: none !important; background: #000 !important;
        }
        .player-wrapper:fullscreen { width: 100vw !important; height: 100vh !important; max-height: 100vh !important; border-radius: 0; border: none; }
        .player-wrapper:-webkit-full-screen { width: 100vw !important; height: 100vh !important; max-height: 100vh !important; border-radius: 0; border: none; }
        .player-wrapper:-moz-full-screen { width: 100vw !important; height: 100vh !important; max-height: 100vh !important; border-radius: 0; border: none; }
        #roVideoEl { width: 100%; height: 100%; display: block; cursor: pointer; background: #000; object-fit: contain; }
        #roIframeWrap { position: absolute; inset: 0; display: none; z-index: 5; }
        #roIframeWrap iframe { width: 100%; height: 100%; border: none; }
        /* ===== SUBTITLE OVERLAY ===== */
        #roSubtitleOverlay {
            position: absolute; bottom: 52px; left: 50%; transform: translateX(-50%);
            z-index: 35; pointer-events: none; text-align: center; width: 80%;
            max-width: 700px; min-width: 200px;
        }
        #roSubtitleOverlay .ro-sub-line {
            display: inline; background: transparent; color: #ffffff;
            -webkit-text-stroke: 2.5px #000; paint-order: stroke fill;
            font-size: clamp(15px, 2.4vw, 22px); font-weight: 800; line-height: 1.75;
            letter-spacing: 0.3px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            white-space: pre-wrap; word-break: break-word;
            box-decoration-break: clone; -webkit-box-decoration-break: clone;
        }
        @media (max-width: 600px) {
            #roSubtitleOverlay { bottom: 48px; width: 88%; max-width: 95vw; }
            #roSubtitleOverlay .ro-sub-line { font-size: clamp(13px, 3.5vw, 18px); -webkit-text-stroke: 2px #000; }
        }
        .player-wrapper:not(.ro-fs-active):not(:fullscreen) #roSubtitleOverlay { bottom: 60px; }
        .player-wrapper.ro-fs-active #roSubtitleOverlay,
        .player-wrapper:fullscreen #roSubtitleOverlay { bottom: 48px; width: 72%; max-width: 800px; }
        .player-wrapper.ro-fs-active #roSubtitleOverlay .ro-sub-line,
        .player-wrapper:fullscreen #roSubtitleOverlay .ro-sub-line { font-size: clamp(18px, 2.8vw, 30px); -webkit-text-stroke: 3px #000; }
        /* ===== BUFFERING ===== */
        .ro-buffer { position: absolute; inset: 0; display: none; z-index: 50; pointer-events: none; }
        .ro-buffer.active { display: block; }
        .ro-topbar {
            position: absolute; top: 0; left: 0; height: 3px; width: 0%;
            background: linear-gradient(90deg, var(--accent-pink), var(--accent-neon), var(--accent-pink));
            background-size: 200% 100%; border-radius: 0 2px 2px 0;
            box-shadow: 0 0 12px rgba(255,0,110,0.7), 0 0 4px rgba(255,0,110,0.9);
            animation: roTopbarFill 1.8s cubic-bezier(0.4,0,0.2,1) forwards, roTopbarShimmer 1.2s linear infinite;
            z-index: 51;
        }
        @keyframes roTopbarFill {
            0%   { width: 0%; } 30%  { width: 40%; } 60%  { width: 65%; }
            80%  { width: 80%; } 95%  { width: 90%; } 100% { width: 90%; }
        }
        @keyframes roTopbarShimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
        .ro-topbar::after {
            content: ''; position: absolute; right: -2px; top: 50%; transform: translateY(-50%);
            width: 8px; height: 8px; border-radius: 50%; background: var(--accent-neon);
            box-shadow: 0 0 10px 4px rgba(255,0,128,0.6);
        }
        .ro-buffer-overlay { position: absolute; inset: 0; background: rgba(0,0,0,0.18); z-index: 50; }
        .ro-spinner-wrap, .ro-ring-outer, .ro-ring-inner, .ro-ring-inner2, .ro-buffer-label { display: none !important; }
        /* ===== CENTER PLAY BUTTON ===== */
        .ro-center-play {
            position: absolute; top: 50%; left: 50%;
            transform: translate(-50%, -50%) scale(0.75); width: 74px; height: 74px;
            border-radius: 50%; background: rgba(0,0,0,0.45); border: 2px solid rgba(255,255,255,0.15);
            backdrop-filter: blur(14px) saturate(1.5); -webkit-backdrop-filter: blur(14px) saturate(1.5);
            display: flex; align-items: center; justify-content: center;
            cursor: pointer; z-index: 40; opacity: 0; pointer-events: none;
            transition: opacity 0.25s ease, transform 0.25s ease, border-color 0.2s ease, background 0.2s ease;
        }
        .ro-center-play.visible { opacity: 1; transform: translate(-50%, -50%) scale(1); pointer-events: all; }
        .ro-center-play:hover { border-color: rgba(255,0,110,0.7); background: rgba(255,0,110,0.12); box-shadow: 0 0 30px rgba(255,0,110,0.25); }
        .ro-center-play svg { width: 26px; height: 26px; fill: #fff; }
        .ro-center-play.is-play svg { margin-left: 3px; }
        .ro-skip {
            position: absolute; top: 50%; transform: translateY(-50%);
            background: rgba(0,0,0,0.55); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1); border-radius: 10px;
            padding: 9px 20px; font-size: 12px; font-weight: 800; letter-spacing: 1px;
            color: rgba(255,255,255,0.9); opacity: 0; pointer-events: none;
            transition: opacity 0.18s ease; z-index: 45;
        }
        .ro-skip.left  { left: 7%; }
        .ro-skip.right { right: 7%; }
        .ro-skip.show  { opacity: 1; }
        /* ===== CONTROLS ===== */
        .ro-controls {
            position: absolute; bottom: 0; left: 0; right: 0;
            background: linear-gradient(0deg, rgba(0,0,0,0.97) 0%, rgba(0,0,0,0.75) 40%, rgba(0,0,0,0.3) 70%, transparent 100%);
            padding: 52px 18px 16px; z-index: 60; transform: translateY(0); opacity: 1;
            transition: transform 0.3s ease, opacity 0.3s ease;
        }
        .player-wrapper.ro-fs-active .ro-controls,
        .player-wrapper:fullscreen .ro-controls,
        .player-wrapper:-webkit-full-screen .ro-controls { transform: translateY(105%); opacity: 0; }
        .player-wrapper.ro-fs-active.ctrl-show .ro-controls,
        .player-wrapper:fullscreen.ctrl-show .ro-controls,
        .player-wrapper:-webkit-full-screen.ctrl-show .ro-controls { transform: translateY(0); opacity: 1; }
        .player-wrapper.megaplay-mode .ro-controls { display: none !important; }
        .player-wrapper.megaplay-mode .ro-center-play { display: none !important; }
        .player-wrapper.megaplay-mode .ro-skip { display: none !important; }
        .player-wrapper.megaplay-mode .ro-buffer { display: none !important; }
        .player-wrapper.megaplay-mode #roSubtitleOverlay { display: none !important; }
        .ro-ctrl-label {
            font-size: 11px; font-weight: 700; letter-spacing: 0.3px;
            color: rgba(255,255,255,0.5); margin-bottom: 10px;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .ro-progress {
            width: 100%; height: 3px; background: rgba(255,255,255,0.1);
            border-radius: 99px; cursor: pointer; margin-bottom: 12px;
            position: relative; transition: height 0.18s ease;
        }
        .ro-progress:hover { height: 5px; }
        .ro-prog-buf { position: absolute; left: 0; top: 0; height: 100%; background: rgba(255,255,255,0.15); border-radius: 99px; width: 0%; pointer-events: none; }
        .ro-prog-fill { position: absolute; left: 0; top: 0; height: 100%; background: linear-gradient(90deg, var(--accent-pink), var(--accent-neon)); border-radius: 99px; width: 0%; pointer-events: none; box-shadow: 0 0 10px rgba(255,0,110,0.55); }
        .ro-prog-thumb { position: absolute; top: 50%; left: 0%; transform: translate(-50%, -50%) scale(0); width: 13px; height: 13px; border-radius: 50%; background: #fff; box-shadow: 0 0 10px rgba(255,0,110,0.8); pointer-events: none; transition: transform 0.15s ease; }
        .ro-progress:hover .ro-prog-thumb { transform: translate(-50%, -50%) scale(1); }
        .ro-row { display: flex; align-items: center; justify-content: space-between; }
        .ro-left, .ro-right { display: flex; align-items: center; gap: 2px; }
        .ro-btn {
            background: transparent; border: none; color: rgba(255,255,255,0.78);
            width: 36px; height: 36px; border-radius: 8px; cursor: pointer;
            display: flex; align-items: center; justify-content: center;
            transition: background 0.15s, color 0.15s, transform 0.15s; flex-shrink: 0;
        }
        .ro-btn:hover { background: rgba(255,0,110,0.15); color: #fff; transform: scale(1.1); }
        .ro-btn svg { width: 18px; height: 18px; fill: currentColor; }
        .ro-btn.lg svg { width: 22px; height: 22px; }
        .ro-time { font-size: 11px; font-weight: 600; color: rgba(255,255,255,0.6); white-space: nowrap; padding: 0 8px; font-variant-numeric: tabular-nums; letter-spacing: 0.3px; }
        .ro-vol-group { display: flex; align-items: center; gap: 2px; max-width: 36px; overflow: hidden; transition: max-width 0.3s ease; }
        .ro-vol-group:hover { max-width: 128px; }
        .ro-vol-slider { -webkit-appearance: none; appearance: none; width: 72px; height: 3px; background: rgba(255,255,255,0.18); border-radius: 99px; outline: none; cursor: pointer; flex-shrink: 0; }
        .ro-vol-slider::-webkit-slider-thumb { -webkit-appearance: none; width: 12px; height: 12px; border-radius: 50%; background: #fff; cursor: pointer; box-shadow: 0 0 6px rgba(255,0,110,0.5); }
        .ro-vol-slider::-moz-range-thumb { width: 12px; height: 12px; border-radius: 50%; background: #fff; border: none; cursor: pointer; }
        /* ===== SERVER PILL SELECTOR ===== */
        .server-pill-row { display: flex; align-items: center; gap: 6px; margin-bottom: 12px; flex-wrap: wrap; }
        .server-pill-label { font-size: 9px; font-weight: 900; letter-spacing: 1.5px; text-transform: uppercase; color: var(--text-tertiary); margin-right: 4px; }
        .srv-pill {
            padding: 5px 14px; border-radius: 99px; border: 1.5px solid var(--border-color);
            background: var(--tertiary-black); color: var(--text-tertiary);
            font-size: 10px; font-weight: 800; letter-spacing: 0.8px; cursor: pointer;
            transition: all 0.2s ease; text-transform: uppercase; white-space: nowrap;
        }
        .srv-pill.active {
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            border-color: transparent; color: #fff; box-shadow: 0 4px 14px rgba(255,0,110,0.35);
        }
        .srv-pill:hover:not(.active) { border-color: var(--accent-pink); color: var(--accent-pink); }
        .srv-pill.failed  { border-color: #ff5252; color: #ff5252; opacity: 0.5; }
        .srv-pill.loading { border-color: var(--accent-pink); color: var(--accent-pink); animation: pilPulse 1s infinite; }
        @keyframes pilPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        /* ===== SUBTITLE DROPDOWN (ENHANCED v4) ===== */
        #roSubMenuWrap { position: relative; display: inline-block; }
        .ro-sub-dropdown {
            display: none; position: absolute; bottom: 44px; right: 0;
            background: #1a1a1a; border: 1.5px solid var(--accent-pink); border-radius: 10px;
            min-width: 200px; max-height: 280px; overflow-y: auto; z-index: 9999;
            box-shadow: 0 8px 32px rgba(255,0,110,0.3), 0 2px 8px rgba(0,0,0,0.5);
            animation: dropIn 0.15s ease-out;
        }
        @keyframes dropIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
        .ro-sub-dropdown.open { display: block; }
        .ro-sub-dropdown-item {
            padding: 10px 16px; cursor: pointer; font-size: 12px; font-weight: 700;
            color: rgba(255,255,255,0.8); transition: background 0.15s, color 0.15s;
            border-left: 3px solid transparent; user-select: none; white-space: nowrap;
            display: flex; align-items: center; gap: 8px;
        }
        .ro-sub-dropdown-item:hover { background: rgba(255,0,110,0.15); color: #fff; }
        .ro-sub-dropdown-item.active-sub {
            color: var(--accent-pink); border-left-color: var(--accent-pink);
            background: rgba(255,0,110,0.08);
        }
        .ro-sub-dropdown-item.loading-sub {
            color: rgba(255,255,255,0.4); cursor: wait;
            animation: subItemPulse 1s ease-in-out infinite;
        }
        .ro-sub-dropdown-item.failed-sub {
            color: #ff5252; opacity: 0.6;
        }
        @keyframes subItemPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .ro-sub-dropdown-item:first-child { border-radius: 8px 8px 0 0; }
        .ro-sub-dropdown-item:last-child  { border-radius: 0 0 8px 8px; }
        .ro-sub-lang-count {
            display: inline-block; background: var(--accent-pink); color: white;
            font-size: 8px; font-weight: 900; padding: 1px 5px; border-radius: 10px;
            margin-left: 6px; vertical-align: middle; letter-spacing: 0.5px;
        }
        .ro-sub-status-dot {
            width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0;
            background: rgba(255,255,255,0.2);
        }
        .ro-sub-status-dot.active  { background: #00e676; box-shadow: 0 0 6px rgba(0,230,118,0.6); }
        .ro-sub-status-dot.loading { background: var(--accent-pink); animation: subItemPulse 1s infinite; }
        .ro-sub-status-dot.failed  { background: #ff5252; }
        /* ===== INFO PANEL ===== */
        .player-info {
            background: linear-gradient(135deg, var(--secondary-black), var(--tertiary-black));
            padding: 20px; border-radius: 10px; border: 2px solid var(--accent-pink);
            animation: slideUp 0.6s var(--transition-smooth); margin-bottom: 24px;
        }
        @keyframes slideUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
        .anime-title {
            font-size: 24px; font-weight: 900; margin: 0 0 8px 0; color: var(--text-primary);
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
        }
        .episode-label { font-size: 14px; font-weight: 700; color: var(--text-tertiary); text-transform: uppercase; letter-spacing: 2px; margin-bottom: 20px; }
        .server-row { display: flex; align-items: center; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
        .server-row-label { font-size: 9px; font-weight: 900; letter-spacing: 1.5px; text-transform: uppercase; color: var(--text-tertiary); margin-right: 2px; }
        .navigation-controls { display: flex; gap: 12px; }
        .nav-btn {
            flex: 1; padding: 12px; border-radius: 8px; border: none; font-weight: 800;
            font-size: 12px; cursor: pointer; transition: all var(--transition-fast);
            text-transform: uppercase; letter-spacing: 1px;
        }
        .prev-btn { background: var(--tertiary-black); color: var(--text-primary); border: 2px solid var(--border-color); }
        .prev-btn:hover:not(:disabled) { border-color: var(--accent-pink); color: var(--accent-pink); }
        .next-btn { background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon)); color: white; box-shadow: 0 4px 15px rgba(255,0,110,0.3); }
        .next-btn:hover:not(:disabled) { transform: scale(1.02); box-shadow: 0 8px 25px rgba(255,0,110,0.5); }
        .nav-btn:disabled { opacity: 0.3; cursor: not-allowed; filter: grayscale(1); }
        /* ===== EPISODES SECTION ===== */
        .episodes-section { background: var(--secondary-black); border-radius: 12px; padding: 20px; border: 2px solid var(--border-color); transition: all var(--transition-smooth); }
        .episodes-section.active { border-color: var(--accent-pink); box-shadow: var(--shadow-md); }
        .episodes-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 12px; border-bottom: 2px solid var(--border-color); }
        .episodes-title { font-size: 18px; font-weight: 800; color: var(--text-primary); text-transform: uppercase; letter-spacing: 1px; }
        .view-all-btn { background: transparent; border: 2px solid var(--accent-pink); color: var(--accent-pink); padding: 8px 16px; border-radius: 6px; font-size: 11px; font-weight: 700; cursor: pointer; transition: all var(--transition-fast); text-transform: uppercase; }
        .view-all-btn:hover { background: var(--accent-pink); color: white; box-shadow: 0 8px 20px rgba(255,0,110,0.3); }
        .episodes-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(60px, 1fr)); gap: 10px; }
        .episode-item { background: var(--tertiary-black); border: 2px solid var(--border-color); padding: 10px; border-radius: 8px; text-align: center; cursor: pointer; font-size: 10px; font-weight: 700; transition: all var(--transition-fast); color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.5px; }
        .episode-item:hover { background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon)); color: white; border-color: var(--accent-pink); transform: translateY(-4px); box-shadow: 0 8px 20px rgba(255,0,110,0.4); }
        .episode-item.current { background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon)); color: white; border-color: var(--accent-pink); box-shadow: 0 0 20px rgba(255,0,110,0.6); }
        /* ===== EPISODE OVERLAY ===== */
        #episodeOverlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(10,10,10,0.95); align-items: center; justify-content: center; z-index: 2000; padding: 16px; backdrop-filter: blur(10px); animation: fadeIn 0.3s ease-out; }
        #episodeOverlay.active { display: flex; }
        .panel-content { background: linear-gradient(135deg, var(--secondary-black), var(--tertiary-black)); border-radius: 14px; max-width: 700px; width: 100%; max-height: 85vh; display: flex; flex-direction: column; border: 2px solid var(--accent-pink); box-shadow: 0 25px 60px rgba(255,0,110,0.2); animation: slideUp 0.4s var(--transition-smooth); }
        .panel-header { padding: 20px; border-bottom: 2px solid var(--accent-pink); display: flex; justify-content: space-between; align-items: center; }
        .panel-header h2 { margin: 0; font-size: 18px; background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; font-weight: 800; }
        .close-btn { background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon)); color: white; border: none; width: 36px; height: 36px; border-radius: 50%; cursor: pointer; font-weight: 900; font-size: 20px; display: flex; align-items: center; justify-content: center; transition: all var(--transition-fast); box-shadow: 0 8px 20px rgba(255,0,110,0.4); }
        .close-btn:hover { transform: rotate(90deg) scale(1.1); box-shadow: 0 12px 32px rgba(255,0,110,0.6); }
        .panel-body { padding: 20px; overflow-y: auto; flex-grow: 1; }
        /* ===== SKELETON CARDS ===== */
        .card-skeleton { background: var(--tertiary-black); border-radius: 8px; overflow: hidden; border: 2px solid var(--border-color); display: flex; flex-direction: column; position: relative; }
        .card-skeleton-img { width: 100%; aspect-ratio: 2 / 3; background: linear-gradient(90deg, var(--secondary-black) 0%, #2e1a24 40%, var(--secondary-black) 80%); background-size: 200% 100%; animation: skeletonPulse 1.4s ease-in-out infinite; }
        .card-skeleton-line { height: 10px; border-radius: 4px; margin: 8px 8px 4px; background: linear-gradient(90deg, var(--secondary-black) 0%, #2e1a24 40%, var(--secondary-black) 80%); background-size: 200% 100%; animation: skeletonPulse 1.4s ease-in-out infinite; }
        .card-skeleton-line.short { width: 55%; height: 8px; }
        @keyframes skeletonPulse { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
        .card-skeleton:nth-child(1) .card-skeleton-img, .card-skeleton:nth-child(1) .card-skeleton-line { animation-delay: 0s; }
        .card-skeleton:nth-child(2) .card-skeleton-img, .card-skeleton:nth-child(2) .card-skeleton-line { animation-delay: 0.07s; }
        .card-skeleton:nth-child(3) .card-skeleton-img, .card-skeleton:nth-child(3) .card-skeleton-line { animation-delay: 0.14s; }
        .card-skeleton:nth-child(4) .card-skeleton-img, .card-skeleton:nth-child(4) .card-skeleton-line { animation-delay: 0.21s; }
        .card-skeleton:nth-child(5) .card-skeleton-img, .card-skeleton:nth-child(5) .card-skeleton-line { animation-delay: 0.28s; }
        .card-skeleton:nth-child(6) .card-skeleton-img, .card-skeleton:nth-child(6) .card-skeleton-line { animation-delay: 0.35s; }
        .card.search-revealed { animation: cardReveal 0.45s cubic-bezier(0.22, 1, 0.36, 1) both; }
        @keyframes cardReveal { from { opacity: 0; transform: translateY(14px) scale(0.97); } to { opacity: 1; transform: translateY(0) scale(1); } }
        #searchResultsSection { animation: none; }
        #searchResultsSection.visible { animation: fadeInUp 0.35s cubic-bezier(0.22, 1, 0.36, 1); }
        #searchTopBar { position: fixed; top: 0; left: 0; height: 3px; width: 0%; background: linear-gradient(90deg, var(--accent-pink), var(--accent-neon), var(--accent-pink)); background-size: 200% 100%; z-index: 9998; opacity: 0; transition: opacity 0.2s ease; box-shadow: 0 0 10px rgba(255,0,110,0.7); }
        #searchTopBar.active { opacity: 1; animation: searchBarFill 0.9s cubic-bezier(0.4,0,0.2,1) forwards, roTopbarShimmer 1s linear infinite; }
        @keyframes searchBarFill { 0% { width: 0%; } 40% { width: 55%; } 80% { width: 85%; } 100% { width: 85%; } }
        .episode-loading { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 60px 20px; gap: 15px; }
        .episode-spinner { width: 50px; height: 50px; border: 3px solid rgba(255,0,110,0.2); border-top: 3px solid var(--accent-pink); border-radius: 50%; animation: spin 1s linear infinite; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .episode-loading-text { font-size: 14px; font-weight: 700; color: var(--text-secondary); letter-spacing: 1px; text-transform: uppercase; }
    </style>
</head>
<body>
<div id="searchTopBar"></div>
<!-- SPLASH SCREEN -->
<div id="splashScreen">
    <div class="splash-content">
        <div class="splash-logo">
            <img src="https://files.manuscdn.com/user_upload_by_module/session_file/310519663321093674/oWQUOlviOKwSQlpf.png" alt="Gojo">
        </div>
        <div class="splash-text">RO-ANIME</div>
        <div class="splash-dots">
            <div class="splash-dot"></div>
            <div class="splash-dot"></div>
            <div class="splash-dot"></div>
        </div>
    </div>
</div>
<header>
    <div class="header-left">
        <div class="logo" onclick="goHome()">
            <div class="logo-gojo">
                <img src="https://files.manuscdn.com/user_upload_by_module/session_file/310519663321093674/oWQUOlviOKwSQlpf.png" alt="Gojo" loading="lazy">
            </div>
            <span>RO-ANIME</span>
        </div>
    </div>
    <div class="search-container">
        <input id="searchInput" type="text" placeholder="Search anime..." autocomplete="off">
        <button onclick="searchAnime()">SEARCH</button>
    </div>
</header>
<main>
    <div id="heroBanner">
        <div class="hero-slider-container" id="heroSliderContainer"></div>
        <div class="hero-controls">
            <button class="hero-nav-btn" onclick="heroPrevSlide()" title="Previous">&#10094;</button>
            <button class="hero-nav-btn" onclick="heroNextSlide()" title="Next">&#10095;</button>
        </div>
    </div>
    <!-- ====== STREAMING SECTION ====== -->
    <div id="streamingSection">
        <button class="nav-btn prev-btn" style="width:auto;margin-bottom:16px;padding:8px 16px;font-size:11px;" onclick="goHome()">&#8592; BACK</button>
        <!-- ====== PLAYER ====== -->
        <div id="videoContainer">
            <div class="player-wrapper" id="playerWrapper">
                <video id="roVideoEl" playsinline></video>
                <div id="roIframeWrap"></div>
                <div id="roSubtitleOverlay"></div>
                <div class="ro-buffer" id="roBuffer">
                    <div class="ro-buffer-overlay"></div>
                    <div class="ro-topbar" id="roTopbar"></div>
                    <div class="ro-spinner-wrap"><div class="ro-ring-outer"></div><div class="ro-ring-inner"></div><div class="ro-ring-inner2"></div></div>
                    <div class="ro-buffer-label">Loading</div>
                </div>
                <div class="ro-center-play is-play visible" id="roCenterPlay">
                    <svg id="roCenterIcon" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
                </div>
                <div class="ro-skip left"  id="roFlashL">&#8722;10s</div>
                <div class="ro-skip right" id="roFlashR">+10s</div>
                <div class="ro-controls" id="roCtrl">
                    <div class="ro-ctrl-label" id="roCtrlLabel">&#8212;</div>
                    <div class="ro-progress" id="roProg">
                        <div class="ro-prog-buf"   id="roProgBuf"></div>
                        <div class="ro-prog-fill"  id="roProgFill"></div>
                        <div class="ro-prog-thumb" id="roProgThumb"></div>
                    </div>
                    <div class="ro-row">
                        <div class="ro-left">
                            <button class="ro-btn lg" id="roPlayBtn" title="Play/Pause">
                                <svg id="roPpIcon" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
                            </button>
                            <button class="ro-btn" id="roRwdBtn" title="Rewind 10s">
                                <svg viewBox="0 0 24 24">
                                    <path d="M11.99 5V1l-5 5 5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6h-2c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z"/>
                                    <text x="12" y="14.5" text-anchor="middle" font-size="5" fill="currentColor" font-weight="bold">10</text>
                                </svg>
                            </button>
                            <button class="ro-btn" id="roFwdBtn" title="Forward 10s">
                                <svg viewBox="0 0 24 24">
                                    <path d="M12.01 5V1l5 5-5 5V7c-3.31 0-6 2.69-6 6s2.69 6 6 6 6-2.69 6-6h2c0 4.42-3.58 8-8 8s-8-3.58-8-8 3.58-8 8-8z"/>
                                    <text x="12" y="14.5" text-anchor="middle" font-size="5" fill="currentColor" font-weight="bold">10</text>
                                </svg>
                            </button>
                            <div class="ro-vol-group">
                                <button class="ro-btn" id="roVolBtn" title="Mute/Unmute">
                                    <svg id="roVolIcon" viewBox="0 0 24 24">
                                        <path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3c0-1.77-1.02-3.29-2.5-4.03v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z"/>
                                    </svg>
                                </button>
                                <input type="range" class="ro-vol-slider" id="roVolSlider" min="0" max="100" value="100">
                            </div>
                            <div class="ro-time">
                                <span id="roCurTime">0:00</span> / <span id="roDur">0:00</span>
                            </div>
                        </div>
                        <div class="ro-right">
                            <!-- Subtitle CC selector — enhanced multi-language v4 -->
                            <div id="roSubMenuWrap">
                                <button class="ro-btn" id="roSubBtn" title="Subtitles / CC" style="display:none;">
                                    <svg viewBox="0 0 24 24"><path d="M20 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 14H4V6h16v12zM6 10h2v2H6zm0 4h8v2H6zm10 0h2v2h-2zm-6-4h8v2h-8z"/></svg>
                                </button>
                                <div id="roSubDropdown" class="ro-sub-dropdown"></div>
                            </div>
                            <!-- Fullscreen -->
                            <button class="ro-btn" id="roFsBtn" title="Fullscreen (F)">
                                <svg id="roFsIcon" viewBox="0 0 24 24">
                                    <path d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"/>
                                </svg>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <!-- ====== END PLAYER ====== -->
        <div class="player-info">
            <h1 id="displayTitle" class="anime-title">Loading...</h1>
            <div id="displayEpisode" class="episode-label">Episode --</div>
            <!-- Server pill selector (rendered dynamically) -->
            <div class="server-pill-row" id="serverPillRow" style="display:none;">
                <span class="server-pill-label">Servers</span>
            </div>
            <!-- Status badge -->
            <div class="server-row">
                <span id="streamStatusBadge" class="stream-status"></span>
            </div>
            <div class="sub-dub-toggle">
                <button id="subBtn" class="subdub-btn active">SUB</button>
            </div>
            <div class="navigation-controls">
                <button id="prevBtn" class="nav-btn prev-btn" onclick="changeEpisode(-1)">PREVIOUS</button>
                <button id="nextBtn" class="nav-btn next-btn" onclick="changeEpisode(1)">NEXT EPISODE</button>
            </div>
        </div>
        <div class="episodes-section active" id="episodesSection">
            <div class="episodes-header">
                <h3 class="episodes-title">Episodes</h3>
                <button class="view-all-btn" onclick="toggleViewAllEpisodes()">VIEW ALL</button>
            </div>
            <div class="episodes-grid" id="episodesGrid"></div>
        </div>
    </div>
    <!-- Search Results -->
    <div id="searchResultsSection" style="display:none;">
        <h2 class="section-title">Search Results</h2>
        <div id="grid"></div>
    </div>
    <!-- Home Section -->
    <div id="homeSection">
        <div id="dynamicSection" style="display:none;">
            <h2 class="section-title">
                <span class="live-badge">LIVE</span> Now on RO-Anime
            </h2>
            <div id="dynamicGrid"></div>
        </div>
        <h2 class="section-title">Top Rated Anime</h2>
        <div id="famousGrid"></div>
    </div>
</main>
<!-- Episode Overlay -->
<div id="episodeOverlay">
    <div class="panel-content">
        <div class="panel-header">
            <h2 id="overlayTitle">Anime Title</h2>
            <button class="close-btn" onclick="closeOverlay()">&times;</button>
        </div>
        <div class="panel-body">
            <p style="margin-bottom:16px;color:var(--text-secondary);font-weight:600;font-size:13px;">SELECT EPISODE:</p>
            <div id="episodesContainer" class="episodes-grid"></div>
        </div>
    </div>
</div>
<script>
    // =========================================================================
    //  POPUP / REDIRECT BLOCKER
    // =========================================================================
    const _origOpen = window.open;
    window.open = function(url, target, features) {
        if (!url || url === 'about:blank') return null;
        try {
            const u = new URL(url, location.href);
            if (u.origin === location.origin) return _origOpen.call(window, url, target, features);
        } catch(e) {}
        console.warn('[RO-ANIME] Blocked popup:', url);
        return null;
    };
    window.addEventListener('beforeunload', e => {
        if (document.activeElement && document.activeElement.tagName === 'IFRAME') {
            e.preventDefault(); e.returnValue = '';
        }
    }, true);
    document.addEventListener('click', e => {
        const a = e.target.closest('a[target="_blank"], a[target="_top"], a[target="_parent"]');
        if (a && !a.closest('#roSubMenuWrap')) {
            const href = a.getAttribute('href') || '';
            if (href && !href.startsWith('#') && !href.startsWith('javascript')) {
                try {
                    const u = new URL(href, location.href);
                    if (u.origin !== location.origin) {
                        e.preventDefault(); e.stopImmediatePropagation();
                        console.warn('[RO-ANIME] Blocked redirect:', href);
                        return;
                    }
                } catch(e) {}
            }
        }
    }, true);

    // =========================================================================
    //  GLOBAL STATE
    // =========================================================================
    const API_BASE        = '';   // same origin — all calls go to our FastAPI backend
    const apiCache        = new Map();
    const suggestionCache = new Map();

    let currentAudioTrack    = 'sub';
    let hlsInstance          = null;
    let roCtrlTimeout        = null;
    let roIsDragging         = false;
    let currentStreamData    = null;
    let heroSlideIndex       = 0;
    let heroSlides           = [];
    let heroAutoInterval     = null;
    let imageLoadQueue       = [];
    let viewAllMode          = false;

    // ── V10 Stream State ────────────────────────────────────────────────────
    let v10Cache             = null;   // full /api/v10/stream response (w/ _cacheKey tag)
    let v10SrvIdx            = 0;      // current server index
    let v10QualIdx           = 0;      // current quality index
    let v10StallTimer        = null;   // stall watchdog interval
    let v10LastTime          = -1;     // last video.currentTime seen by watchdog
    let v10FilteredServers   = [];     // v10Cache.servers after banned CDN filter

    // ── Subtitle State v4 ───────────────────────────────────────────────────
    let roSubtitleCues       = [];     // parsed VTT cues [{start, end, text}]
    let roSubActive          = false;  // subtitle cues loaded and rendering
    let roAllSubtitleTracks  = [];     // all available tracks [{file,label,kind,default,status}]
                                       // status: 'unknown' | 'loading' | 'valid' | 'invalid'
    let roActiveSubLabel     = null;   // label of currently shown track (null = Off)
    let roSubLoadingLabel    = null;   // track being loaded (prevents race condition)
    let roSubFallbackQueue   = [];     // tracks queued for fallback attempt (ordered by priority)
    let roSubEpisodeToken    = null;   // per-episode unique token — ties roSubAutoSelectDone to episode

    // ── Fullscreen State ────────────────────────────────────────────────────
    let roIsFullscreen       = false;

    // =========================================================================
    //  ANIME LIST
    // =========================================================================
    const fullAnimeList = [
        { title: 'Steel Ball Run: JoJo no Kimyou na Bouken', image: 'https://myanimelist.net/images/anime/1448/154111l.jpg' },
        { title: 'Sousou no Frieren', image: 'https://myanimelist.net/images/anime/1015/138006l.jpg' },
        { title: 'Fullmetal Alchemist: Brotherhood', image: 'https://myanimelist.net/images/anime/1208/94745l.jpg' },
        { title: 'Chainsaw Man Movie: Reze-hen', image: 'https://myanimelist.net/images/anime/1763/150638l.jpg' },
        { title: 'Sousou no Frieren 2nd Season', image: 'https://myanimelist.net/images/anime/1921/154528l.jpg' },
        { title: 'Steins;Gate', image: 'https://myanimelist.net/images/anime/1935/127974l.jpg' },
        { title: 'Shingeki no Kyojin Season 3 Part 2', image: 'https://myanimelist.net/images/anime/1517/100633l.jpg' },
        { title: 'Gintama\u00b0', image: 'https://myanimelist.net/images/anime/3/72078l.jpg' },
        { title: 'Gintama: The Final', image: 'https://myanimelist.net/images/anime/1245/116760l.jpg' },
        { title: 'Hunter x Hunter (2011)', image: 'https://myanimelist.net/images/anime/1337/99013l.jpg' },
        { title: 'One Piece Fan Letter', image: 'https://myanimelist.net/images/anime/1455/146229l.jpg' },
        { title: "Gintama'", image: 'https://myanimelist.net/images/anime/4/50361l.jpg' },
        { title: "Gintama': Enchousen", image: 'https://myanimelist.net/images/anime/1452/123686l.jpg' },
        { title: 'Ginga Eiyuu Densetsu', image: 'https://myanimelist.net/images/anime/1976/142016l.jpg' },
        { title: 'Gintama.', image: 'https://myanimelist.net/images/anime/3/83528l.jpg' },
        { title: 'Bleach: Sennen Kessen-hen', image: 'https://myanimelist.net/images/anime/1908/135431l.jpg' },
        { title: 'Kaguya-sama wa Kokurasetai: Ultra Romantic', image: 'https://myanimelist.net/images/anime/1160/122627l.jpg' },
        { title: 'Fruits Basket: The Final', image: 'https://myanimelist.net/images/anime/1085/114792l.jpg' },
        { title: 'Clannad: After Story', image: 'https://myanimelist.net/images/anime/1299/110774l.jpg' },
        { title: 'Gintama', image: 'https://myanimelist.net/images/anime/10/73274l.jpg' },
        { title: 'Koe no Katachi', image: 'https://myanimelist.net/images/anime/1122/96435l.jpg' },
        { title: 'Code Geass: Hangyaku no Lelouch R2', image: 'https://myanimelist.net/images/anime/1088/135089l.jpg' },
        { title: 'Kusuriya no Hitorigoto 2nd Season', image: 'https://myanimelist.net/images/anime/1025/147458l.jpg' },
        { title: '3-gatsu no Lion 2nd Season', image: 'https://myanimelist.net/images/anime/3/88469l.jpg' },
        { title: 'Monster', image: 'https://myanimelist.net/images/anime/10/18793l.jpg' },
        { title: 'Vinland Saga Season 2', image: 'https://myanimelist.net/images/anime/1170/124312l.jpg' },
        { title: 'Kimi no Na wa.', image: 'https://myanimelist.net/images/anime/5/87048l.jpg' },
        { title: 'Shingeki no Kyojin: The Final Season', image: 'https://myanimelist.net/images/anime/1000/110531l.jpg' },
        { title: 'Mob Psycho 100 II', image: 'https://myanimelist.net/images/anime/1918/96303l.jpg' },
        { title: 'Vinland Saga', image: 'https://myanimelist.net/images/anime/1500/103005l.jpg' },
        { title: 'Haikyuu!! Karasuno Koukou vs. Shiratorizawa Gakuen Koukou', image: 'https://myanimelist.net/images/anime/7/81992l.jpg' },
        { title: 'Sen to Chihiro no Kamikakushi', image: 'https://myanimelist.net/images/anime/6/79597l.jpg' },
        { title: 'Bocchi the Rock!', image: 'https://myanimelist.net/images/anime/1448/127956l.jpg' },
        { title: 'One Piece', image: 'https://myanimelist.net/images/anime/1244/138851l.jpg' },
        { title: 'Jujutsu Kaisen 2nd Season', image: 'https://myanimelist.net/images/anime/1792/138022l.jpg' },
        { title: 'Cyberpunk: Edgerunners', image: 'https://myanimelist.net/images/anime/1818/126435l.jpg' },
        { title: 'Death Note', image: 'https://myanimelist.net/images/anime/1079/138100l.jpg' },
        { title: 'Haikyuu!! Second Season', image: 'https://myanimelist.net/images/anime/9/76662l.jpg' },
        { title: 'Dungeon Meshi', image: 'https://myanimelist.net/images/anime/1711/142478l.jpg' },
        { title: 'Chainsaw Man', image: 'https://myanimelist.net/images/anime/1806/126216l.jpg' },
        { title: 'Hunter x Hunter', image: 'https://myanimelist.net/images/anime/1305/132237l.jpg' },
        { title: 'Haikyuu!!', image: 'https://myanimelist.net/images/anime/7/76014l.jpg' },
        { title: 'Dandadan', image: 'https://myanimelist.net/images/anime/1584/143719l.jpg' },
        { title: 'Re:Zero kara Hajimeru Isekai Seikatsu 3rd Season', image: 'https://myanimelist.net/images/anime/1706/144725l.jpg' },
        { title: 'Kimetsu no Yaiba', image: 'https://myanimelist.net/images/anime/1286/99889l.jpg' },
        { title: 'Made in Abyss', image: 'https://myanimelist.net/images/anime/6/86733l.jpg' },
        { title: 'Violet Evergarden', image: 'https://myanimelist.net/images/anime/1795/95088l.jpg' },
        { title: 'Cowboy Bebop', image: 'https://myanimelist.net/images/anime/4/19644l.jpg' },
        { title: 'Pluto', image: 'https://myanimelist.net/images/anime/1021/138568l.jpg' },
        { title: 'Odd Taxi', image: 'https://myanimelist.net/images/anime/1981/113348l.jpg' },
        { title: 'Mahou Shoujo Madoka\u2605Magica', image: 'https://myanimelist.net/images/anime/11/55225l.jpg' },
        { title: 'Samurai Champloo', image: 'https://myanimelist.net/images/anime/1370/135212l.jpg' },
        { title: 'Spy x Family', image: 'https://myanimelist.net/images/anime/1441/122795l.jpg' },
        { title: 'Jujutsu Kaisen', image: 'https://myanimelist.net/images/anime/1171/109222l.jpg' },
        { title: 'Blue Giant', image: 'https://myanimelist.net/images/anime/1958/132159l.jpg' },
        { title: 'Shigatsu wa Kimi no Uso', image: 'https://myanimelist.net/images/anime/1405/143284l.jpg' },
        { title: 'Houseki no Kuni', image: 'https://myanimelist.net/images/anime/3/88293l.jpg' },
        { title: 'One Punch Man', image: 'https://myanimelist.net/images/anime/12/76049l.jpg' },
        { title: "Vivy: Fluorite Eye's Song", image: 'https://myanimelist.net/images/anime/1551/128960l.jpg' }
    ];

    // =========================================================================
    //  SPLASH SCREEN
    // =========================================================================
    window.addEventListener('load', () => {
        setTimeout(() => document.getElementById('splashScreen').classList.add('hidden'), 30);
    });

    // =========================================================================
    //  HOME INIT
    // =========================================================================
    async function initHome() {
        const fragment = document.createDocumentFragment();
        fullAnimeList.forEach(anime => fragment.appendChild(createCard(anime, true)));
        document.getElementById('famousGrid').appendChild(fragment);
        initHeroSlider(null);
        startAsyncImageLoading();
        fetchDynamicHomeData();
    }

    async function fetchDynamicHomeData() {
        try {
            const resp = await fetch(`${API_BASE}/home/thumbnails`);
            if (!resp.ok) { console.warn('Backend response not ok:', resp.status); return; }
            const data = await resp.json();
            const list = Array.isArray(data) ? data : (data.results || []);
            if (!list.length) return;
            const posterUrls = list.slice(0, 8).map(item => item.poster).filter(Boolean);
            if (posterUrls.length > 0) initHeroSlider(posterUrls);
            renderDynamicThumbnails(list);
        } catch(e) { console.error('Error fetching dynamic home data:', e); }
    }

    function renderDynamicThumbnails(animeList) {
        const grid    = document.getElementById('dynamicGrid');
        const section = document.getElementById('dynamicSection');
        const fragment = document.createDocumentFragment();
        animeList.forEach(anime => fragment.appendChild(createCard(anime, false)));
        grid.appendChild(fragment);
        section.style.display = 'block';
    }

    async function identifyHeroAnime(imgUrl) {
        try {
            const response = await fetch('https://api.anthropic.com/v1/messages', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    model: 'claude-sonnet-4-20250514', max_tokens: 100,
                    messages: [{ role: 'user', content: [
                        { type: 'image', source: { type: 'url', url: imgUrl } },
                        { type: 'text', text: 'This is an anime banner/thumbnail. Reply with ONLY the anime title, nothing else. If you cannot identify it, reply with exactly: unknown' }
                    ]}]
                })
            });
            const data  = await response.json();
            const title = data.content?.[0]?.text?.trim();
            if (!title || title.toLowerCase() === 'unknown') return null;
            return title;
        } catch(e) { return null; }
    }

    function initHeroSlider(dynamicImages) {
        const container = document.getElementById('heroSliderContainer');
        container.innerHTML = '';
        heroSlides = [];
        if (heroAutoInterval) { clearInterval(heroAutoInterval); heroAutoInterval = null; }
        let slides;
        if (dynamicImages && dynamicImages.length > 0) {
            slides = dynamicImages.slice(0, Math.min(dynamicImages.length, 8)).map(imgUrl => ({ image: imgUrl, title: null }));
        } else {
            slides = fullAnimeList.slice(0, 5).map(a => ({ image: a.image, title: a.title }));
        }
        heroSlideIndex = 0;
        slides.forEach((slideData, index) => {
            const slide = document.createElement('div');
            slide.className = `hero-slide ${index === 0 ? 'active' : ''}`;
            slide.style.backgroundImage = `url('${slideData.image}')`;
            const overlay = document.createElement('div');
            overlay.className = 'hero-slide-overlay';
            slide.appendChild(overlay);
            if (slideData.title) {
                overlay.innerHTML = `<h2 class="hero-slide-title">${slideData.title}</h2><span class="hero-slide-ep">FEATURED &bull; HD</span><button class="hero-watch-btn" onclick="quickOpenEpisodes('${slideData.title.replace(/'/g, "\\'")}')">&#9654; WATCH NOW</button>`;
            } else {
                identifyHeroAnime(slideData.image).then(title => {
                    if (title) overlay.innerHTML = `<h2 class="hero-slide-title">${title}</h2><span class="hero-slide-ep">FEATURED &bull; HD</span><button class="hero-watch-btn" onclick="quickOpenEpisodes('${title.replace(/'/g, "\\'")}')">&#9654; WATCH NOW</button>`;
                });
            }
            container.appendChild(slide);
            heroSlides.push(slide);
        });
        heroAutoInterval = setInterval(() => {
            heroSlideIndex = (heroSlideIndex + 1) % slides.length;
            updateHeroSlide();
        }, 6000);
    }

    function updateHeroSlide() { heroSlides.forEach((s, i) => s.classList.toggle('active', i === heroSlideIndex)); }
    function heroPrevSlide() { heroSlideIndex = (heroSlideIndex - 1 + heroSlides.length) % heroSlides.length; updateHeroSlide(); }
    function heroNextSlide() { heroSlideIndex = (heroSlideIndex + 1) % heroSlides.length; updateHeroSlide(); }

    // =========================================================================
    //  LAZY IMAGE LOADING
    // =========================================================================
    let imgObserver = null;
    function startAsyncImageLoading() {
        if ('IntersectionObserver' in window) {
            if (!imgObserver) {
                imgObserver = new IntersectionObserver((entries) => {
                    entries.forEach(entry => {
                        if (entry.isIntersecting) {
                            const img = entry.target;
                            if (img.dataset.src) { img.src = img.dataset.src; delete img.dataset.src; }
                            imgObserver.unobserve(img);
                        }
                    });
                }, { rootMargin: '200px' });
            }
            imageLoadQueue.forEach(img => imgObserver.observe(img));
            imageLoadQueue = [];
        } else {
            imageLoadQueue.forEach(img => { if (img.dataset.src) img.src = img.dataset.src; });
            imageLoadQueue = [];
        }
    }
    function observeNewImage(img) {
        if (imgObserver) imgObserver.observe(img);
        else imageLoadQueue.push(img);
    }

    // =========================================================================
    //  CARD CREATION
    // =========================================================================
    function createCard(element, isStatic = false) {
        const card = document.createElement('div');
        card.className = 'card';
        const imgWrapper = document.createElement('div');
        imgWrapper.className = 'card-img-wrapper loading';
        const img = document.createElement('img');
        img.loading = 'lazy';
        img.onload  = () => { imgWrapper.classList.remove('loading'); img.classList.add('loaded'); };
        img.onerror = () => {
            imgWrapper.classList.remove('loading');
            imgWrapper.style.background = 'var(--tertiary-black)';
            imgWrapper.innerHTML = `<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);font-size:9px;color:var(--text-tertiary);text-align:center;">IMAGE<br>UNAVAILABLE</div>`;
        };
        const imageUrl = element.poster || element.image || element.thumbnail || element.img || '';
        if (imageUrl) { img.dataset.src = imageUrl; observeNewImage(img); }
        imgWrapper.appendChild(img);
        const overlay = document.createElement('div');
        overlay.className = 'card-overlay';
        if (!isStatic) {
            const watchBtn = document.createElement('button');
            watchBtn.className   = 'card-watch-btn';
            watchBtn.textContent = '\u25b6 WATCH NOW';
            watchBtn.addEventListener('click', (e) => { e.stopPropagation(); fetchEpisodes(element); });
            overlay.appendChild(watchBtn);
        }
        imgWrapper.appendChild(overlay);
        const info = document.createElement('div');
        info.className = 'card-info';
        const title = document.createElement('div');
        title.className   = 'card-title';
        title.textContent = element.title;
        const meta = document.createElement('div');
        meta.className   = 'card-meta';
        meta.textContent = 'Series \u2022 HD';
        info.appendChild(title);
        info.appendChild(meta);
        card.appendChild(imgWrapper);
        card.appendChild(info);
        card.addEventListener('click', () => isStatic ? quickOpenEpisodes(element.title) : fetchEpisodes(element));
        return card;
    }

    // =========================================================================
    //  PREFETCH STATIC IDs
    // =========================================================================
    const staticAnimeIdCache = new Map();
    async function prefetchAllStaticAnimeIds() {
        for (const anime of fullAnimeList) {
            const key = anime.title.toLowerCase();
            if (staticAnimeIdCache.has(key)) continue;
            try {
                const searchKey = `search:${key}`;
                let results = apiCache.has(searchKey) ? apiCache.get(searchKey) : null;
                if (!results) {
                    const response = await fetch(`${API_BASE}/search/${encodeURIComponent(anime.title)}`);
                    if (!response.ok) continue;
                    const data = await response.json();
                    results = data.results || [];
                    apiCache.set(searchKey, results);
                }
                if (results.length) {
                    staticAnimeIdCache.set(key, results.find(r => r.title?.toLowerCase() === key) || results[0]);
                }
            } catch(e) {}
            await new Promise(r => setTimeout(r, 150));
        }
    }

    // =========================================================================
    //  QUICK OPEN EPISODES
    // =========================================================================
    async function quickOpenEpisodes(title) {
        const overlay   = document.getElementById('episodeOverlay');
        const container = document.getElementById('episodesContainer');
        container.innerHTML = `<div class="episode-loading"><div class="episode-spinner"></div><div class="episode-loading-text">LOADING EPISODES...</div></div>`;
        document.getElementById('overlayTitle').textContent = title;
        overlay.classList.add('active');
        const key = title.toLowerCase();
        if (staticAnimeIdCache.has(key)) { fetchEpisodes(staticAnimeIdCache.get(key)); return; }
        try {
            const searchKey = `search:${key}`;
            let results = apiCache.has(searchKey) ? apiCache.get(searchKey) : null;
            if (!results) {
                const response = await fetch(`${API_BASE}/search/${encodeURIComponent(title)}`);
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();
                results = data.results || [];
                apiCache.set(searchKey, results);
            }
            if (!results.length) { container.innerHTML = "<p style='text-align:center;padding:30px;color:var(--text-tertiary);'>No results found.</p>"; return; }
            const best = results.find(r => r.title?.toLowerCase() === key) || results[0];
            staticAnimeIdCache.set(key, best);
            fetchEpisodes(best);
        } catch(e) {
            container.innerHTML = "<p style='color:var(--accent-pink);text-align:center;padding:30px;'>&#9888; Could not reach the server. Try again.</p>";
        }
    }

    // =========================================================================
    //  SEARCH
    // =========================================================================
    function showSearchSkeletons(count = 18) {
        const grid = document.getElementById('grid');
        grid.innerHTML = '';
        const frag = document.createDocumentFragment();
        for (let i = 0; i < count; i++) {
            const sk = document.createElement('div');
            sk.className = 'card-skeleton';
            sk.innerHTML = `<div class="card-skeleton-img"></div><div class="card-skeleton-line"></div><div class="card-skeleton-line short" style="margin-bottom:8px"></div>`;
            frag.appendChild(sk);
        }
        grid.appendChild(frag);
    }
    function startSearchBar() {
        const bar = document.getElementById('searchTopBar');
        bar.className = ''; bar.offsetHeight; bar.className = 'active';
    }
    function finishSearchBar() {
        const bar = document.getElementById('searchTopBar');
        bar.style.transition = 'width 0.25s ease, opacity 0.3s ease 0.25s';
        bar.style.width = '100%';
        setTimeout(() => {
            bar.style.opacity = '0';
            setTimeout(() => { bar.className = ''; bar.style.width = ''; bar.style.transition = ''; bar.style.opacity = ''; }, 400);
        }, 250);
    }
    async function searchAnime() {
        const query = document.getElementById('searchInput').value.trim();
        if (!query) return;
        hideSuggestions();
        document.getElementById('searchResultsSection').style.display = 'block';
        document.getElementById('searchResultsSection').className = 'visible';
        document.getElementById('homeSection').style.display = 'none';
        document.getElementById('streamingSection').style.display = 'none';
        document.getElementById('heroBanner').style.display = 'none';
        showSearchSkeletons(18);
        startSearchBar();
        const cacheKey = `search:${query.toLowerCase()}`;
        if (apiCache.has(cacheKey)) { finishSearchBar(); renderSearchResults(apiCache.get(cacheKey)); return; }
        try {
            const response = await fetch(`${API_BASE}/search/${encodeURIComponent(query)}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            const results = data.results || [];
            apiCache.set(cacheKey, results);
            finishSearchBar();
            renderSearchResults(results);
        } catch(error) {
            finishSearchBar();
            document.getElementById('grid').innerHTML = "<p style='grid-column:1/-1;text-align:center;color:var(--accent-pink);padding:30px;'>&#9888; Could not reach the server. Try again.</p>";
        }
    }
    function renderSearchResults(results) {
        const grid = document.getElementById('grid');
        grid.innerHTML = '';
        if (results.length > 0) {
            const fragment = document.createDocumentFragment();
            results.forEach((el, i) => {
                const card = createCard(el);
                card.classList.add('search-revealed');
                card.style.animationDelay = `${Math.min(i * 45, 400)}ms`;
                fragment.appendChild(card);
            });
            grid.appendChild(fragment);
            startAsyncImageLoading();
        } else {
            grid.innerHTML = "<p style='grid-column:1/-1;text-align:center;padding:30px;color:var(--text-tertiary);'>No results found.</p>";
        }
    }

    // =========================================================================
    //  FETCH EPISODES
    // =========================================================================
    async function fetchEpisodes(anime) {
        const overlay   = document.getElementById('episodeOverlay');
        const container = document.getElementById('episodesContainer');
        if (!overlay.classList.contains('active')) {
            container.innerHTML = `<div class="episode-loading"><div class="episode-spinner"></div><div class="episode-loading-text">LOADING EPISODES...</div></div>`;
            document.getElementById('overlayTitle').textContent = anime.title;
            overlay.classList.add('active');
        } else {
            document.getElementById('overlayTitle').textContent = anime.title;
        }
        const cacheKey = `episodes_${anime.anime_id}`;
        if (apiCache.has(cacheKey)) { renderEpisodes(anime, apiCache.get(cacheKey)); return; }
        try {
            const response = await fetch(`${API_BASE}/anime/episode/${anime.anime_id}`);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            const episodesList = data.episodes || [];
            if (episodesList.length === 0) throw new Error('Empty');
            apiCache.set(cacheKey, episodesList);
            renderEpisodes(anime, episodesList);
        } catch(error) {
            try {
                await new Promise(r => setTimeout(r, 1000));
                const response = await fetch(`${API_BASE}/anime/episode/${anime.anime_id}`);
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();
                const episodesList = data.episodes || [];
                if (episodesList.length === 0) throw new Error('Empty');
                apiCache.set(cacheKey, episodesList);
                renderEpisodes(anime, episodesList);
            } catch(retryError) {
                container.innerHTML = `<div style="text-align:center;padding:30px;"><p style="color:var(--accent-pink);margin-bottom:16px;">&#9888; Could not load episodes.</p><button onclick="fetchEpisodes(${JSON.stringify(anime).replace(/"/g,'&quot;')})" style="padding:8px 20px;background:var(--accent-pink);color:white;border:none;border-radius:6px;cursor:pointer;font-weight:700;">RETRY</button></div>`;
            }
        }
    }

    function renderEpisodes(anime, episodesList) {
        const container = document.getElementById('episodesContainer');
        container.innerHTML = '';
        if (episodesList.length > 0) {
            const fragment = document.createDocumentFragment();
            episodesList.forEach(ep => {
                const epBox = document.createElement('div');
                epBox.className   = 'episode-item';
                epBox.textContent = `EP ${ep.episode_number}`;
                epBox.addEventListener('click', () => startStreaming(anime, ep, episodesList));
                fragment.appendChild(epBox);
            });
            container.appendChild(fragment);
            setTimeout(() => startStreaming(anime, episodesList[0], episodesList), 80);
        } else {
            container.innerHTML = "<p style='text-align:center;padding:30px;color:var(--text-tertiary);'>No episodes found.</p>";
        }
    }

    // =========================================================================
    //  START STREAMING
    // =========================================================================
    async function startStreaming(anime, episode, allEpisodes) {
        closeOverlay();
        document.getElementById('searchResultsSection').style.display = 'none';
        document.getElementById('homeSection').style.display = 'none';
        document.getElementById('heroBanner').style.display = 'none';
        document.getElementById('streamingSection').style.display = 'block';
        currentStreamData = {
            id:       episode.episode_id,
            title:    anime.title,
            ep:       parseInt(episode.episode_number),
            episodes: allEpisodes
        };
        roPlayerReset();
        updatePlayerUI();
        renderEpisodesList(true);
        window.scrollTo({ top: 0, behavior: 'smooth' });
        loadStream();
    }

    // =========================================================================
    //  PLAYER RESET
    // =========================================================================
    function roPlayerReset() {
        // Clear all v10 stream state
        clearInterval(v10StallTimer);
        v10StallTimer       = null;
        v10Cache            = null;
        v10SrvIdx           = 0;
        v10QualIdx          = 0;
        v10LastTime         = -1;
        v10FilteredServers  = [];

        if (hlsInstance) { hlsInstance.destroy(); hlsInstance = null; }
        const video      = document.getElementById('roVideoEl');
        const iframeWrap = document.getElementById('roIframeWrap');
        video.removeEventListener('timeupdate', v10OnTimeUpdate);
        video.pause();
        video.src = '';
        video.style.display = 'none';
        iframeWrap.innerHTML = '';
        iframeWrap.style.display = 'none';
        document.getElementById('playerWrapper').classList.remove('megaplay-mode');
        ['roCtrl','roCenterPlay','roFlashL','roFlashR'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.visibility = '';
        });
        roShowSpinner(false);
        document.getElementById('roCenterPlay').classList.add('visible');
        roApplyProgress(0);
        document.getElementById('roCurTime').textContent = '0:00';
        document.getElementById('roDur').textContent     = '0:00';
        document.getElementById('roProgBuf').style.width = '0%';
        // Reset subtitle state completely
        roResetSubtitleState();
        // Reset server pills
        const pillRow = document.getElementById('serverPillRow');
        pillRow.innerHTML = '<span class="server-pill-label">Servers</span>';
        pillRow.style.display = 'none';
    }

    // =========================================================================
    //  SUBTITLE SYSTEM v4
    //  ─────────────────────────────────────────────────────────────────────
    //  Key improvements over v3:
    //  1. Per-episode token (roSubEpisodeToken) replaces global roSubAutoSelectDone.
    //     Each episode gets a unique string token. Auto-select is tied to this
    //     token, so switching episodes ALWAYS triggers fresh auto-select even
    //     when stream data is served from cache (needsFetch=false path).
    //  2. Auto-select delay reduced: 800ms → 200ms. Video loads fast on VOD CDN;
    //     800ms meant users saw 1-2 seconds of bare video before subs appeared.
    //  3. Hardened isValidVTT():
    //     - Rejects responses that look like HTML pages (start with <html / <!DOC)
    //     - corsproxy.io and allorigins.win sometimes return HTML error wrappers
    //       that contain "WEBVTT" in a <pre> tag — we catch this before the regex
    //  4. Fast-path in fetchAndValidateVTT(): if candidate 0 (direct) succeeds,
    //     remaining candidates are skipped entirely. Cuts load time from 3-4s → <1s
    //     for most CDNs.
    //  5. roSwitchSubtitle(): allows retry on a track previously marked active
    //     but with no cues (roSubActive=false). Previously this path early-returned
    //     and left the user without subtitles.
    //  6. roAutoFallbackToNextTrack(): also considers 'loading' state tracks
    //     (edge case when two tracks trigger fallback simultaneously).
    //  7. roResetSubtitleState(): generates a new episode token, clearing the
    //     per-episode guard for the next episode.
    // =========================================================================

    function roResetSubtitleState() {
        roSubtitleCues      = [];
        roSubActive         = false;
        roAllSubtitleTracks = [];
        roActiveSubLabel    = null;
        roSubLoadingLabel   = null;
        roSubFallbackQueue  = [];
        // Generate a new per-episode token — this is what guards auto-select,
        // not a simple boolean. A new episode always gets a fresh token.
        roSubEpisodeToken   = Math.random().toString(36).slice(2);
        document.getElementById('roSubtitleOverlay').innerHTML = '';
        roRenderSubMenu();
    }

    // ── VTT Validation ──────────────────────────────────────────────────────
    /**
     * Validate raw VTT text strictly:
     * - Reject HTML pages immediately (corsproxy/allorigins error wrappers)
     * - Must have WEBVTT header
     * - Must have at least one timecode line
     * - Must have at least one non-empty cue body line
     */
    function isValidVTT(text) {
        if (!text || text.trim().length < 20) return false;
        const stripped = text.trim();

        // FAST REJECT: HTML error page (corsproxy.io/allorigins return these on failure)
        // Check first 200 chars for telltale HTML markers
        const head200 = stripped.slice(0, 200).toLowerCase();
        if (head200.startsWith('<!doc') || head200.startsWith('<html') ||
            head200.startsWith('<?xml') || head200.includes('<html')) {
            return false;
        }

        // WEBVTT header check (allows BOM and NOTE fields)
        if (!/^(?:\ufeff)?WEBVTT/i.test(stripped)) return false;

        // Timecode pattern: HH:MM:SS.mmm --> HH:MM:SS.mmm (dot or comma as decimal sep)
        const timecodeRe = /\d{1,2}:\d{2}:\d{2}[.,]\d{3}\s+-->\s+\d{1,2}:\d{2}:\d{2}[.,]\d{3}/;
        if (!timecodeRe.test(stripped)) return false;

        // Must have actual cue text after a timecode line
        const lines = stripped.split(/\r?\n/);
        let foundTimecode = false;
        for (const line of lines) {
            if (timecodeRe.test(line.trim())) { foundTimecode = true; continue; }
            if (foundTimecode && line.trim().length > 0) return true;
        }
        return false;
    }

    // ── VTT Parser ─────────────────────────────────────────────────────────
    function parseVTT(text) {
        const cues  = [];
        const lines = text.split(/\r?\n/);
        let i = 0;
        while (i < lines.length) {
            if (lines[i] && lines[i].includes('-->')) {
                const match = lines[i].match(
                    /(\d{1,2}:\d{2}:\d{2}[.,]\d{3})\s+-->\s+(\d{1,2}:\d{2}:\d{2}[.,]\d{3})/
                );
                if (match) {
                    const start = vttTimeToSec(match[1]);
                    const end   = vttTimeToSec(match[2]);
                    i++;
                    const textLines = [];
                    while (i < lines.length && lines[i].trim() !== '') {
                        textLines.push(lines[i]); i++;
                    }
                    // Strip VTT tags
                    const cueText = textLines.join('\n').replace(/<[^>]+>/g, '').trim();
                    if (cueText) cues.push({ start, end, text: cueText });
                }
            }
            i++;
        }
        return cues;
    }

    function vttTimeToSec(t) {
        const parts = t.replace(',', '.').split(':');
        if (parts.length === 3) return parseFloat(parts[0]) * 3600 + parseFloat(parts[1]) * 60 + parseFloat(parts[2]);
        if (parts.length === 2) return parseFloat(parts[0]) * 60 + parseFloat(parts[1]);
        return 0;
    }

    // ── Build CDN Candidate URLs for a VTT file (5-tier fallback) ──────────
    /**
     * Given a VTT file URL, returns an ordered list of candidate URLs to try:
     * Tier 1: Direct URL (original from API)
     * Tier 2: Swap hostname to vod.netmagcdn.com (CF-free alt CDN)
     * Tier 3: /proxy endpoint on our backend (adds proper headers)
     * Tier 4: corsproxy.io (public CORS proxy — fallback)
     * Tier 5: allorigins.win (another public fallback — last resort)
     */
    function buildVTTCandidates(fileUrl) {
        if (!fileUrl) return [];
        const candidates = [];

        // Tier 1: direct
        candidates.push(fileUrl);

        // Tier 2: alt CDN hostname
        try {
            const parsed  = new URL(fileUrl);
            const altHost = 'vod.netmagcdn.com';
            if (parsed.hostname !== altHost) {
                const altUrl = new URL(fileUrl);
                altUrl.hostname = altHost;
                candidates.push(altUrl.toString());
            }
        } catch(e) {}

        // Tier 3: backend proxy (handles Referer/Origin correctly)
        candidates.push(`${API_BASE}/proxy?url=${encodeURIComponent(fileUrl)}`);

        // Tier 4: corsproxy.io
        candidates.push(`https://corsproxy.io/?${encodeURIComponent(fileUrl)}`);

        // Tier 5: allorigins.win raw endpoint
        candidates.push(`https://api.allorigins.win/raw?url=${encodeURIComponent(fileUrl)}`);

        return candidates;
    }

    // ── Fetch and Validate VTT Through All Candidates ──────────────────────
    /**
     * Tries each CDN candidate in order until one returns valid VTT data.
     * Returns { success: bool, cues: [], candidateUsed: string|null, aborted: bool }
     * Respects roSubLoadingLabel to abort if user switches tracks mid-load.
     *
     * FIX v4: Fast-path — if candidate 0 succeeds, return immediately without
     * trying remaining candidates. This cuts subtitle load time dramatically
     * for well-functioning CDNs (most cases).
     */
    async function fetchAndValidateVTT(fileUrl, expectedLabel) {
        const candidates = buildVTTCandidates(fileUrl);
        for (let ci = 0; ci < candidates.length; ci++) {
            const candidateUrl = candidates[ci];

            // Race condition guard: abort if user switched tracks
            if (roSubLoadingLabel !== expectedLabel) {
                console.log(`[SUB-v4] Aborted load for "${expectedLabel}" — user switched`);
                return { success: false, cues: [], candidateUsed: null, aborted: true };
            }

            try {
                const controller = new AbortController();
                // Shorter timeout for early candidates (direct CDN should be fast)
                const timeoutMs = ci === 0 ? 7000 : 9000;
                const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
                const r = await fetch(candidateUrl, { signal: controller.signal });
                clearTimeout(timeoutId);

                if (!r.ok) throw new Error(`HTTP ${r.status}`);

                // Check Content-Type before reading body — HTML = reject immediately
                const ct = r.headers.get('Content-Type') || '';
                if (ct.includes('text/html') || ct.includes('application/xhtml')) {
                    console.warn(`[SUB-v4] HTML Content-Type from ${candidateUrl.slice(0, 70)} — skipping`);
                    throw new Error('HTML Content-Type — not a VTT file');
                }

                const txt = await r.text();

                // Validate the fetched content (includes HTML body check)
                if (!isValidVTT(txt)) {
                    console.warn(`[SUB-v4] Invalid VTT from ${candidateUrl.slice(0, 70)}`);
                    throw new Error('Invalid VTT content');
                }

                const cues = parseVTT(txt);
                if (cues.length === 0) {
                    console.warn(`[SUB-v4] VTT parsed but 0 cues from ${candidateUrl.slice(0, 70)}`);
                    throw new Error('VTT has 0 cues after parsing');
                }

                console.log(`[SUB-v4] ✓ "${expectedLabel}" loaded — ${cues.length} cues via ${candidateUrl.slice(0, 70)}`);
                // Fast-path: success on first candidate — return immediately
                return { success: true, cues, candidateUsed: candidateUrl, aborted: false };

            } catch(e) {
                if (e.name === 'AbortError') {
                    console.warn(`[SUB-v4] Timeout on ${candidateUrl.slice(0, 70)}`);
                } else {
                    console.warn(`[SUB-v4] Failed on ${candidateUrl.slice(0, 70)}: ${e.message}`);
                }
                // Continue to next candidate
            }
        }

        console.warn(`[SUB-v4] All 5 candidates exhausted for "${expectedLabel}" (${fileUrl.slice(0, 60)})`);
        return { success: false, cues: [], candidateUsed: null, aborted: false };
    }

    // ── Update Track Status ─────────────────────────────────────────────────
    /**
     * Updates the status field of a track in roAllSubtitleTracks by label.
     * Status: 'unknown' | 'loading' | 'valid' | 'invalid'
     */
    function roSetTrackStatus(label, status) {
        const track = roAllSubtitleTracks.find(t => t.label === label);
        if (track) track.status = status;
        roRenderSubMenu();  // re-render to show updated status dots
    }

    // ── Subtitle Rendering on timeupdate ────────────────────────────────────
    function roRenderSubtitle(currentTime) {
        const overlay = document.getElementById('roSubtitleOverlay');
        if (!roSubActive || roSubtitleCues.length === 0) {
            overlay.innerHTML = '';
            return;
        }
        const cue = roSubtitleCues.find(c => currentTime >= c.start && currentTime <= c.end);
        if (cue) {
            const lines = cue.text.split('\n').filter(l => l.trim());
            overlay.innerHTML = lines
                .map(l => `<div><span class="ro-sub-line">${l}</span></div>`)
                .join('');
        } else {
            overlay.innerHTML = '';
        }
    }

    // ── Subtitle Dropdown Menu v4 ────────────────────────────────────────────
    /**
     * Renders the subtitle dropdown with live status indicators.
     * Each track shows:
     *   • Green dot  = valid (loaded successfully)
     *   • Pink dot   = currently loading
     *   • Red dot    = failed all candidates
     *   • Grey dot   = unknown (not yet attempted)
     * "Off" is always the first item.
     * Active track is highlighted with pink left border.
     */
    function roRenderSubMenu() {
        const btn      = document.getElementById('roSubBtn');
        const dropdown = document.getElementById('roSubDropdown');
        if (!btn || !dropdown) return;

        if (!roAllSubtitleTracks || roAllSubtitleTracks.length === 0) {
            btn.style.display = 'none';
            dropdown.classList.remove('open');
            return;
        }

        btn.style.display = 'flex';
        // Show count badge
        const validCount = roAllSubtitleTracks.filter(t => t.status === 'valid').length;
        const totalCount = roAllSubtitleTracks.length;
        const badgeNum   = validCount > 0 ? validCount : totalCount;
        const countBadge = totalCount > 1
            ? `<span class="ro-sub-lang-count">${badgeNum}</span>`
            : '';
        btn.innerHTML = `<svg viewBox="0 0 24 24"><path d="M20 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 14H4V6h16v12zM6 10h2v2H6zm0 4h8v2H6zm10 0h2v2h-2zm-6-4h8v2h-8z"/></svg>${countBadge}`;

        // Re-attach click handler (innerHTML wipe removes old one)
        btn.onclick = roToggleSubMenu;

        dropdown.innerHTML = '';

        // Helper: build a single dropdown item
        const makeItem = (fileUrl, label, status) => {
            const div       = document.createElement('div');
            const isOff     = (fileUrl === null);
            const isActive  = isOff ? (roActiveSubLabel === null) : (label === roActiveSubLabel);
            const isLoading = (!isOff && label === roSubLoadingLabel);

            let itemClass = 'ro-sub-dropdown-item';
            if (isActive && !isOff) itemClass += ' active-sub';
            if (isLoading)          itemClass += ' loading-sub';
            if (status === 'invalid' && !isActive) itemClass += ' failed-sub';
            div.className = itemClass;

            // Status dot
            const dot = document.createElement('span');
            dot.className = 'ro-sub-status-dot';
            if (isOff) {
                dot.style.display = 'none';
            } else if (isLoading) {
                dot.classList.add('loading');
            } else if (status === 'valid') {
                dot.classList.add('active');
            } else if (status === 'invalid') {
                dot.classList.add('failed');
            }
            div.appendChild(dot);

            const textNode = document.createElement('span');
            textNode.textContent = isOff ? 'Off' : (label || 'Unknown');
            if (status === 'invalid' && !isActive) textNode.style.textDecoration = 'line-through';
            div.appendChild(textNode);

            div.addEventListener('click', (e) => {
                e.stopPropagation();
                roSwitchSubtitle(fileUrl, label);
            });
            return div;
        };

        // "Off" item first
        dropdown.appendChild(makeItem(null, 'Off', null));

        // Language items sorted: valid first, then unknown, then invalid
        const sorted = [...roAllSubtitleTracks].sort((a, b) => {
            const order = { valid: 0, unknown: 1, loading: 1, invalid: 2 };
            return (order[a.status] || 1) - (order[b.status] || 1);
        });
        sorted.forEach(track => {
            dropdown.appendChild(makeItem(track.file, track.label, track.status));
        });
    }

    function roToggleSubMenu(e) {
        if (e) e.stopPropagation();
        const dropdown = document.getElementById('roSubDropdown');
        if (!dropdown) return;
        dropdown.classList.toggle('open');
    }

    // Close dropdown when clicking outside
    document.addEventListener('click', (e) => {
        if (!e.target.closest('#roSubMenuWrap')) {
            const dd = document.getElementById('roSubDropdown');
            if (dd) dd.classList.remove('open');
        }
    });

    // ── Switch Subtitle Track ────────────────────────────────────────────────
    /**
     * Core subtitle switching function.
     * - Handles "Off" (fileUrl = null) by clearing state
     * - Shows immediate UI feedback (loading state in dropdown)
     * - Loads VTT through 5-tier fallback
     * - On failure: marks track as invalid and tries next best alternative
     * - Race condition safe via roSubLoadingLabel guard
     *
     * FIX v4: No longer early-returns when label===roActiveSubLabel but
     * roSubActive===false. That state means the track was selected but cues
     * never loaded (e.g. previous failure). We allow a retry in that case.
     */
    async function roSwitchSubtitle(fileUrl, label) {
        document.getElementById('roSubDropdown').classList.remove('open');

        if (!fileUrl) {
            // User explicitly turned off subtitles
            roActiveSubLabel  = null;
            roSubLoadingLabel = null;
            roSubtitleCues    = [];
            roSubActive       = false;
            document.getElementById('roSubtitleOverlay').innerHTML = '';
            roRenderSubMenu();
            return;
        }

        // Already active AND cues loaded — no need to reload
        // FIX: only skip if BOTH active AND cues exist. If active but no cues,
        // allow retry (previous load may have failed silently).
        if (label === roActiveSubLabel && roSubActive && roSubtitleCues.length > 0) {
            return;
        }

        // Prevent duplicate loads for the same track that is already loading
        if (label === roSubLoadingLabel) return;

        // Immediate UI feedback — set loading state
        roActiveSubLabel  = label;
        roSubLoadingLabel = label;
        roSubtitleCues    = [];
        roSubActive       = false;
        document.getElementById('roSubtitleOverlay').innerHTML = '';
        roSetTrackStatus(label, 'loading');

        // Attempt to load through all CDN fallbacks
        const result = await fetchAndValidateVTT(fileUrl, label);

        // Check abort (user switched to different track while this was loading)
        if (result.aborted) return;

        // Final guard: still the track we want?
        if (roSubLoadingLabel !== label) {
            console.log(`[SUB-v4] Discarding result for "${label}" — user switched away`);
            return;
        }

        if (result.success && result.cues.length > 0) {
            // Success!
            roSubtitleCues = result.cues;
            roSubActive    = true;
            roSetTrackStatus(label, 'valid');
            roSubLoadingLabel = null;
            roRenderSubMenu();
            console.log(`[SUB-v4] ✓ Track "${label}" active (${result.cues.length} cues)`);
        } else {
            // Failed all CDN tiers — mark as invalid
            roSetTrackStatus(label, 'invalid');
            roSubLoadingLabel = null;
            roActiveSubLabel  = null;
            console.warn(`[SUB-v4] ✗ Track "${label}" failed — attempting fallback to next valid track`);

            // Automatically try next available valid/unknown track
            roAutoFallbackToNextTrack(label);
        }
    }

    // ── Auto-Fallback to Next Valid Track ────────────────────────────────────
    /**
     * When a subtitle track fails, automatically tries the next best track.
     * Priority: 'valid' tracks first, then 'unknown' tracks, then 'loading'
     * (edge case), skipping 'invalid' and the just-failed label.
     * Stops after finding a working track or exhausting all options.
     *
     * FIX v4: Also considers 'loading' state as a fallback candidate to handle
     * edge cases where two tracks trigger fallback simultaneously.
     */
    async function roAutoFallbackToNextTrack(failedLabel) {
        if (!roAllSubtitleTracks || roAllSubtitleTracks.length === 0) return;

        // Find next candidate: prefer valid, then unknown, then loading, skip invalid and failed label
        const candidates = roAllSubtitleTracks.filter(t =>
            t.label !== failedLabel &&
            t.status !== 'invalid' &&
            t.label !== roActiveSubLabel
        );

        // Sort: valid first (already confirmed working), then unknown, then loading
        candidates.sort((a, b) => {
            const order = { valid: 0, unknown: 1, loading: 2 };
            return (order[a.status] || 1) - (order[b.status] || 1);
        });

        if (candidates.length === 0) {
            console.warn('[SUB-v4] No fallback tracks available — subtitles off');
            roActiveSubLabel = null;
            roRenderSubMenu();
            return;
        }

        const next = candidates[0];
        console.log(`[SUB-v4] Auto-fallback → trying "${next.label}"`);
        await roSwitchSubtitle(next.file, next.label);
    }

    // ── Auto-Select Default/English Subtitle ────────────────────────────────
    /**
     * Called once per episode load. Selects the best subtitle track automatically:
     * 1. Track marked as default=true by the API
     * 2. Track labeled "English" (exact match, case-insensitive)
     * 3. Track labeled "English" (partial match)
     * 4. Track labeled "Eng" (exact match, case-insensitive)
     * 5. Track labeled "Eng" (partial match)
     * 6. First track in the list
     *
     * FIX v4: Guard is now per-episode token (roSubEpisodeToken), not a
     * global boolean. A new episode always gets a fresh token in
     * roResetSubtitleState(), guaranteeing this runs even when v10Cache is
     * reused (needsFetch=false). Capturing the token at call time means stale
     * async continuations from previous episodes are silently discarded.
     */
    async function roAutoSelectDefaultSubtitle() {
        if (!roAllSubtitleTracks || roAllSubtitleTracks.length === 0) return;

        // Capture the token for this episode at the start of the async function.
        // If the episode changes before we finish, our token won't match the
        // current roSubEpisodeToken and we bail out.
        const myToken = roSubEpisodeToken;
        if (!myToken) return;

        // Priority order for auto-selection
        const def =
            roAllSubtitleTracks.find(t => t.default === true) ||
            roAllSubtitleTracks.find(t => t.label && /^english$/i.test(t.label.trim())) ||
            roAllSubtitleTracks.find(t => t.label && /english/i.test(t.label)) ||
            roAllSubtitleTracks.find(t => t.label && /^eng$/i.test(t.label.trim())) ||
            roAllSubtitleTracks.find(t => t.label && /eng/i.test(t.label)) ||
            roAllSubtitleTracks[0];

        if (!def) return;

        // Guard: still the same episode?
        if (roSubEpisodeToken !== myToken) {
            console.log('[SUB-v4] Auto-select aborted — episode changed');
            return;
        }

        console.log(`[SUB-v4] Auto-selecting: "${def.label}" (default=${def.default})`);
        await roSwitchSubtitle(def.file, def.label);
    }

    // ── Background Pre-validation of All Tracks ──────────────────────────────
    /**
     * After auto-select completes, quietly probes all other tracks in the background
     * to discover which ones are valid/invalid WITHOUT showing them in the dropdown
     * as "loading". This pre-populates the status dots so the user sees green/red
     * before even clicking the dropdown.
     *
     * FIX v4: Captures episode token at start. Any async continuation from a
     * previous episode is automatically cancelled when the token doesn't match.
     */
    async function roPrevalidateAllTracks() {
        if (!roAllSubtitleTracks || roAllSubtitleTracks.length === 0) return;
        const myToken = roSubEpisodeToken;
        if (!myToken) return;

        for (const track of roAllSubtitleTracks) {
            // Bail if episode changed
            if (roSubEpisodeToken !== myToken) break;
            // Skip: already loading, already known valid/invalid, or currently active
            if (track.status !== 'unknown') continue;
            if (track.label === roSubLoadingLabel) continue;
            // Small delay between checks to avoid hammering CDN
            await new Promise(r => setTimeout(r, 400));
            // Double-check episode hasn't changed during the await
            if (roSubEpisodeToken !== myToken || !currentStreamData) break;
            try {
                const candidates = buildVTTCandidates(track.file);
                // Only try first two tiers for pre-validation (fast check)
                for (const c of candidates.slice(0, 2)) {
                    // Check episode token before each request
                    if (roSubEpisodeToken !== myToken) break;
                    try {
                        const ctrl = new AbortController();
                        const tid  = setTimeout(() => ctrl.abort(), 6000);
                        const r    = await fetch(c, { signal: ctrl.signal });
                        clearTimeout(tid);
                        if (!r.ok) throw new Error(`HTTP ${r.status}`);
                        const ct = r.headers.get('Content-Type') || '';
                        if (ct.includes('text/html')) throw new Error('HTML response');
                        const txt = await r.text();
                        if (isValidVTT(txt)) {
                            if (roSubEpisodeToken === myToken) {
                                roSetTrackStatus(track.label, 'valid');
                            }
                            break;
                        } else {
                            throw new Error('Invalid VTT');
                        }
                    } catch(e) {
                        // Try next tier
                    }
                }
                // If still unknown after checking, leave as unknown (don't mark invalid prematurely)
            } catch(e) {
                // Ignore pre-validation errors — full load will try all tiers
            }
        }
    }

    // =========================================================================
    //  V10 TIMEUPDATE — subtitle rendering
    // =========================================================================
    function v10OnTimeUpdate() {
        roRenderSubtitle(document.getElementById('roVideoEl').currentTime);
    }

    // =========================================================================
    //  V10 STALL WATCHDOG
    //  Checks every 5 seconds if video has been stuck at the same timestamp.
    //  If so, falls through to the next server automatically.
    // =========================================================================
    function v10StartWatchdog() {
        clearInterval(v10StallTimer);
        const video = document.getElementById('roVideoEl');
        v10LastTime = video.currentTime;
        v10StallTimer = setInterval(() => {
            if (!video.paused && !video.ended && video.currentTime === v10LastTime) {
                console.warn('[v10] Stall detected — trying next server');
                v10NextServer();
            }
            v10LastTime = video.currentTime;
        }, 5000);
    }

    // =========================================================================
    //  V10 SERVER FALLBACK — infinite retry loop
    //  Cycles through all servers. When last server fails, wraps back to 0
    //  and re-fetches fresh API data so URLs are not stale.
    // =========================================================================
    async function v10NextServer() {
        clearInterval(v10StallTimer);
        if (hlsInstance) { hlsInstance.destroy(); hlsInstance = null; }
        const saved = document.getElementById('roVideoEl').currentTime;

        if (!v10Cache) { loadStream(); return; }

        v10SrvIdx++;

        if (v10SrvIdx >= v10FilteredServers.length) {
            // All servers exhausted — re-fetch fresh URLs and wrap around
            v10SrvIdx  = 0;
            v10QualIdx = 0;
            const badge = document.getElementById('streamStatusBadge');
            badge.textContent = 'Retrying... re-fetching stream data';
            badge.className   = 'stream-status loading';
            roShowSpinner(true);
            console.warn('[v10] All servers tried — re-fetching API for fresh URLs');

            try {
                const episodeId = currentStreamData.id;
                const res = await fetch(`${API_BASE}/api/v10/stream/${episodeId}?type=sub`);
                if (!res.ok) throw new Error('HTTP ' + res.status);
                const fresh = await res.json();
                fresh._cacheKey    = episodeId + '_sub';
                v10Cache           = fresh;
                v10FilteredServers = (fresh.servers || []).filter(s => !isBannedCDN(s.m3u8Url));
                console.log('[v10] Fresh data: ' + v10FilteredServers.length + ' servers');
            } catch(err) {
                console.warn('[v10] Re-fetch failed:', err.message, '— retrying with stale cache');
                // If we have no servers at all, show not-available
                if (v10FilteredServers.length === 0) {
                    document.getElementById('streamStatusBadge').textContent = '\u26a0 Video not available yet';
                    document.getElementById('streamStatusBadge').className   = 'stream-status error';
                    roShowSpinner(false);
                    return;
                }
            }
        }

        v10QualIdx = 0;
        v10AttachHLS(saved);
    }

    // =========================================================================
    //  BANNED CDN CHECK
    // =========================================================================
    function isBannedCDN(url) {
        const l = url.toLowerCase();
        return l.includes('storm') || l.includes('strom') || l.includes('crimsonstorm');
    }

    // =========================================================================
    //  BUILD SERVER PILL SELECTOR
    // =========================================================================
    function roRenderServerPills() {
        const row = document.getElementById('serverPillRow');
        row.innerHTML = '<span class="server-pill-label">Servers</span>';
        if (!v10FilteredServers.length) { row.style.display = 'none'; return; }
        row.style.display = 'flex';
        v10FilteredServers.forEach((s, i) => {
            const pill = document.createElement('button');
            pill.className   = 'srv-pill' + (i === v10SrvIdx ? ' active' : '');
            pill.textContent = s.serverLabel || s.serverName || `Server ${i + 1}`;
            pill.dataset.idx = i;
            pill.onclick = () => {
                if (i === v10SrvIdx) return;
                const saved = document.getElementById('roVideoEl').currentTime;
                v10SrvIdx  = i;
                v10QualIdx = 0;
                roRenderServerPills();
                // Reset subtitle state when manually switching server so new
                // server's tracks get auto-selected fresh
                roResetSubtitleState();
                // Re-load subtitle tracks from cache for new server selection
                if (v10Cache && v10Cache.subtitles && v10Cache.subtitles.length > 0) {
                    const seen = new Set();
                    roAllSubtitleTracks = v10Cache.subtitles
                        .filter(s => {
                            if (!s.file) return false;
                            const key = (s.label || '').toLowerCase().trim();
                            if (seen.has(key)) return false;
                            seen.add(key);
                            return true;
                        })
                        .map(s => ({
                            file:    s.file,
                            label:   s.label || 'Unknown',
                            kind:    s.kind  || 'captions',
                            default: Boolean(s.default),
                            status:  'unknown',
                        }));
                    roRenderSubMenu();
                    setTimeout(() => {
                        roAutoSelectDefaultSubtitle().then(() => {
                            setTimeout(roPrevalidateAllTracks, 1500);
                        });
                    }, 200);
                }
                v10AttachHLS(saved);
            };
            row.appendChild(pill);
        });
    }

    function roUpdateServerPill(idx, state) {
        const row   = document.getElementById('serverPillRow');
        const pills = row.querySelectorAll('.srv-pill');
        pills.forEach((p, i) => {
            p.className = 'srv-pill' + (i === idx ? (state === 'active' ? ' active' : ' ' + state) : '');
        });
    }

    // =========================================================================
    //  V10 HLS ATTACH — wires HLS.js (or native Safari) to the video element
    //
    //  CDN routing:
    //    vod.netmagcdn.com → load DIRECT (CF-free, no Referer needed)
    //    everything else   → route manifest through /proxy (adds Referer/Origin)
    //    megaplay referer  → iframe embed (no HLS.js)
    //
    //  4-second manifest timeout → v10NextServer() automatically
    // =========================================================================
    function v10AttachHLS(resumeTime) {
        resumeTime = resumeTime || 0;

        if (!v10FilteredServers.length || v10SrvIdx >= v10FilteredServers.length) {
            document.getElementById('streamStatusBadge').textContent = '\u26a0 Video not available yet. Try another episode.';
            document.getElementById('streamStatusBadge').className   = 'stream-status error';
            roShowSpinner(false);
            return;
        }

        const server     = v10FilteredServers[v10SrvIdx];
        const quality    = server.qualities[v10QualIdx] || server.qualities[0];
        const rawUrl     = quality.url;
        const badge      = document.getElementById('streamStatusBadge');
        const video      = document.getElementById('roVideoEl');
        const pw         = document.getElementById('playerWrapper');
        const iframeWrap = document.getElementById('roIframeWrap');

        const isVod      = rawUrl.toLowerCase().includes('vod.netmagcdn.com') || rawUrl.toLowerCase().includes('vod.');
        const isMegaplay = server.referer && server.referer.includes('megaplay.buzz');

        let playUrl;
        if (isVod) {
            playUrl = rawUrl;
        } else if (!isMegaplay) {
            const ref = encodeURIComponent(server.referer || '');
            playUrl = `${API_BASE}/proxy?url=${encodeURIComponent(rawUrl)}&referer=${ref}`;
        }

        clearInterval(v10StallTimer);
        if (hlsInstance) { hlsInstance.destroy(); hlsInstance = null; }

        badge.textContent = 'Loading ' + (server.serverLabel || server.serverName) + '...';
        badge.className   = 'stream-status loading';
        roShowSpinner(true);
        roUpdateServerPill(v10SrvIdx, 'loading');

        // ── Megaplay: iframe embed ────────────────────────────────────────
        if (isMegaplay) {
            video.pause(); video.src = ''; video.style.display = 'none';
            iframeWrap.innerHTML = '';
            ['roCtrl','roCenterPlay','roFlashL','roFlashR'].forEach(id => {
                const el = document.getElementById(id);
                if (el) el.style.visibility = 'hidden';
            });
            pw.classList.add('megaplay-mode');
            const epId = currentStreamData.id;
            iframeWrap.innerHTML = `<iframe src="https://megaplay.buzz/stream/s-2/${epId}/sub"
                allowfullscreen allow="autoplay; encrypted-media; fullscreen"
                sandbox="allow-scripts allow-same-origin allow-forms allow-presentation allow-downloads"
                referrerpolicy="no-referrer" loading="lazy"
                style="pointer-events:auto;"></iframe>`;
            iframeWrap.style.display = 'block';
            badge.textContent = '\u25cf ' + (server.serverLabel || server.serverName);
            badge.className   = 'stream-status live';
            roShowSpinner(false);
            roUpdateServerPill(v10SrvIdx, 'active');
            return;
        }

        // ── HLS path ─────────────────────────────────────────────────────
        pw.classList.remove('megaplay-mode');
        iframeWrap.innerHTML = ''; iframeWrap.style.display = 'none';
        ['roCtrl','roCenterPlay','roFlashL','roFlashR'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.visibility = '';
        });
        video.style.display = 'block';
        video.removeEventListener('timeupdate', v10OnTimeUpdate);
        video.addEventListener('timeupdate', v10OnTimeUpdate);

        // 4-second manifest load timeout → auto fallback
        let v10LoadTimeout = setTimeout(() => {
            console.warn('[v10] 4s timeout on', server.serverName, '— trying next');
            roUpdateServerPill(v10SrvIdx, 'failed');
            v10NextServer();
        }, 4000);

        const onReady = () => {
            clearTimeout(v10LoadTimeout);
            if (resumeTime > 0) video.currentTime = resumeTime;
            video.play().catch(() => {});
            badge.textContent = '\u25cf ' + (server.serverLabel || server.serverName) + (isVod ? ' \u2022 VOD' : '');
            badge.className   = 'stream-status live';
            roShowSpinner(false);
            roShowControls();
            document.getElementById('roCtrlLabel').textContent = currentStreamData.title + '  \u00b7  EP ' + currentStreamData.ep;
            roUpdateServerPill(v10SrvIdx, 'active');
            v10StartWatchdog();
        };

        console.log('[v10] Attaching', server.serverName, isVod ? '(direct)' : '(proxied)', playUrl.slice(0, 80));

        if (typeof Hls !== 'undefined' && Hls.isSupported()) {
            hlsInstance = new Hls({
                enableWorker:            true,
                lowLatencyMode:          false,
                maxBufferLength:         20,
                maxMaxBufferLength:      60,
                backBufferLength:        60,
                startLevel:              -1,
                fragLoadingTimeOut:      10000,
                manifestLoadingTimeOut:  8000,
                fragLoadingMaxRetry:     2,
                manifestLoadingMaxRetry: 2,
            });
            hlsInstance.loadSource(playUrl);
            hlsInstance.attachMedia(video);
            hlsInstance.on(Hls.Events.MANIFEST_PARSED, onReady);
            hlsInstance.on(Hls.Events.ERROR, (_, d) => {
                console.warn('[v10] HLS error:', d.type, d.details, 'fatal:', d.fatal, 'code:', d.response?.code);
                if (d.fatal || [403, 404].includes(d.response?.code)) {
                    clearTimeout(v10LoadTimeout);
                    roUpdateServerPill(v10SrvIdx, 'failed');
                    v10NextServer();
                    return;
                }
                if (d.type === Hls.ErrorTypes.NETWORK_ERROR) hlsInstance.startLoad();
                else if (d.type === Hls.ErrorTypes.MEDIA_ERROR) hlsInstance.recoverMediaError();
            });
        } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
            // Native Safari HLS
            video.src = playUrl;
            video.addEventListener('loadedmetadata', () => {
                clearTimeout(v10LoadTimeout);
                onReady();
            }, { once: true });
            video.addEventListener('error', () => {
                clearTimeout(v10LoadTimeout);
                roUpdateServerPill(v10SrvIdx, 'failed');
                v10NextServer();
            }, { once: true });
        } else {
            clearTimeout(v10LoadTimeout);
            badge.textContent = '\u26a0 HLS not supported in this browser';
            badge.className   = 'stream-status error';
            roShowSpinner(false);
        }
    }

    // =========================================================================
    //  LOAD STREAM — fetches API v10 once per episode (cached), then attaches
    //
    //  FIX v4: Subtitle auto-select is now triggered with a 200ms delay instead
    //  of 800ms. The per-episode token in roResetSubtitleState() ensures
    //  roAutoSelectDefaultSubtitle() always runs for new episodes, even when
    //  v10Cache is reused from a previous fetch (needsFetch=false path).
    // =========================================================================
    async function loadStream() {
        const episodeId  = currentStreamData.id;
        const badge      = document.getElementById('streamStatusBadge');
        const video      = document.getElementById('roVideoEl');
        const iframeWrap = document.getElementById('roIframeWrap');
        const pw         = document.getElementById('playerWrapper');

        badge.textContent = 'Loading...';
        badge.className   = 'stream-status loading';

        clearInterval(v10StallTimer);
        if (hlsInstance) { hlsInstance.destroy(); hlsInstance = null; }
        video.pause();
        video.src = '';
        video.style.display = 'none';
        iframeWrap.innerHTML = '';
        iframeWrap.style.display = 'none';
        pw.classList.remove('megaplay-mode');
        roShowSpinner(true);
        document.getElementById('roCenterPlay').classList.add('visible');

        // Clear subtitle state for new episode — generates new episode token
        roResetSubtitleState();

        const cacheKey   = episodeId + '_sub';
        const needsFetch = !v10Cache || v10Cache._cacheKey !== cacheKey;

        if (needsFetch) {
            v10SrvIdx  = 0;
            v10QualIdx = 0;
            try {
                badge.textContent = 'Fetching stream data...';
                const res = await fetch(`${API_BASE}/api/v10/stream/${episodeId}?type=sub`);
                if (!res.ok) throw new Error('HTTP ' + res.status);
                v10Cache           = await res.json();
                v10Cache._cacheKey = cacheKey;
                console.log('[v10] API response:', v10Cache);
            } catch(err) {
                console.warn('[v10] API fetch failed:', err.message);
                badge.textContent = '\u26a0 Could not load stream. Please retry.';
                badge.className   = 'stream-status error';
                roShowSpinner(false);
                return;
            }
        }

        // Filter out banned CDNs
        v10FilteredServers = (v10Cache.servers || []).filter(s => !isBannedCDN(s.m3u8Url));

        if (!v10FilteredServers.length) {
            badge.textContent = '\u26a0 Video not available yet. No playable servers.';
            badge.className   = 'stream-status error';
            roShowSpinner(false);
            return;
        }

        // Render server pill selector
        roRenderServerPills();

        // ─── Load subtitle tracks from API response ───────────────────────
        // Deduplicate by label + normalize status to 'unknown'
        if (v10Cache.subtitles && v10Cache.subtitles.length > 0) {
            const seen = new Set();
            roAllSubtitleTracks = v10Cache.subtitles
                .filter(s => {
                    if (!s.file) return false;
                    const key = (s.label || '').toLowerCase().trim();
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                })
                .map(s => ({
                    file:    s.file,
                    label:   s.label || 'Unknown',
                    kind:    s.kind  || 'captions',
                    default: Boolean(s.default),
                    status:  'unknown',   // will be updated as tracks load
                }));

            console.log(`[SUB-v4] ${roAllSubtitleTracks.length} subtitle track(s) found:`,
                roAllSubtitleTracks.map(t => t.label));

            // Show subtitle button with track list
            roRenderSubMenu();

            // FIX v4: reduced delay 800ms → 200ms for faster subtitle appearance.
            // Auto-select runs async and does NOT block HLS playback start below.
            setTimeout(() => {
                roAutoSelectDefaultSubtitle().then(() => {
                    // After auto-select, quietly pre-validate remaining tracks in background
                    setTimeout(roPrevalidateAllTracks, 1500);
                });
            }, 200);
        } else {
            console.log('[SUB-v4] No subtitle tracks in API response');
            roRenderSubMenu();
        }

        // Start HLS playback on first server
        v10AttachHLS(0);
    }

    // =========================================================================
    //  UPDATE PLAYER UI
    // =========================================================================
    function updatePlayerUI() {
        document.getElementById('displayTitle').textContent   = currentStreamData.title;
        document.getElementById('displayEpisode').textContent = `Episode ${currentStreamData.ep}`;
        const hasPrev = currentStreamData.episodes.some(e => parseInt(e.episode_number) === currentStreamData.ep - 1);
        const hasNext = currentStreamData.episodes.some(e => parseInt(e.episode_number) === currentStreamData.ep + 1);
        document.getElementById('prevBtn').disabled = !hasPrev;
        document.getElementById('nextBtn').disabled = !hasNext;
        document.getElementById('subBtn').classList.toggle('active', currentAudioTrack === 'sub');
    }

    // =========================================================================
    //  RENDER EPISODES LIST
    // =========================================================================
    let episodesListRendered = false;

    function renderEpisodesList(forceRebuild = false) {
        const grid = document.getElementById('episodesGrid');
        if (episodesListRendered && !forceRebuild && !viewAllMode) {
            grid.querySelectorAll('.episode-item').forEach(item => {
                item.classList.toggle('current', parseInt(item.dataset.ep) === currentStreamData.ep);
            });
            return;
        }
        grid.innerHTML = '';
        episodesListRendered = false;
        const displayLimit   = viewAllMode ? currentStreamData.episodes.length : 50;
        const episodesToShow = currentStreamData.episodes.slice(0, displayLimit);
        const fragment = document.createDocumentFragment();
        episodesToShow.forEach(ep => {
            const epBox = document.createElement('div');
            epBox.className   = 'episode-item';
            epBox.dataset.ep  = ep.episode_number;
            if (parseInt(ep.episode_number) === currentStreamData.ep) epBox.classList.add('current');
            epBox.textContent = `EP ${ep.episode_number}`;
            epBox.addEventListener('click', () => startStreaming({ title: currentStreamData.title }, ep, currentStreamData.episodes));
            fragment.appendChild(epBox);
        });
        grid.appendChild(fragment);
        episodesListRendered = true;
    }

    function toggleViewAllEpisodes() {
        viewAllMode = !viewAllMode;
        const btn       = document.querySelector('.view-all-btn');
        const container = document.getElementById('videoContainer');
        if (viewAllMode) { btn.textContent = 'HIDE PLAYER'; container.classList.add('hidden'); }
        else             { btn.textContent = 'VIEW ALL';    container.classList.remove('hidden'); }
        renderEpisodesList();
    }

    function changeEpisode(delta) {
        const nextEpNum = currentStreamData.ep + delta;
        const nextEp    = currentStreamData.episodes.find(e => parseInt(e.episode_number) === nextEpNum);
        if (nextEp) startStreaming({ title: currentStreamData.title }, nextEp, currentStreamData.episodes);
    }

    // =========================================================================
    //  NAVIGATION
    // =========================================================================
    function goHome() {
        if (roIsFullscreen) roExitFullscreen();
        roPlayerReset();
        document.getElementById('streamingSection').style.display     = 'none';
        document.getElementById('searchResultsSection').style.display = 'none';
        document.getElementById('homeSection').style.display          = 'block';
        if (window.innerWidth >= 768) document.getElementById('heroBanner').style.display = 'block';
        document.getElementById('searchInput').value = '';
        viewAllMode          = false;
        episodesListRendered = false;
    }
    function closeOverlay() { document.getElementById('episodeOverlay').classList.remove('active'); }

    // =========================================================================
    //  SEARCH AS YOU TYPE
    // =========================================================================
    let searchTimeout     = null;
    let suggestionsDropdown = null;

    function createSuggestionsDropdown() {
        const dropdown = document.createElement('div');
        dropdown.id = 'suggestionsDropdown';
        dropdown.style.cssText = 'position:absolute;top:100%;left:0;right:0;background:var(--secondary-black);border:2px solid var(--accent-pink);border-top:none;border-radius:0 0 6px 6px;max-height:300px;overflow-y:auto;z-index:1001;display:none;box-shadow:0 8px 24px rgba(255,0,110,0.2);';
        return dropdown;
    }
    function showSuggestions(suggestions) {
        if (!suggestionsDropdown) {
            suggestionsDropdown = createSuggestionsDropdown();
            document.querySelector('.search-container').style.position = 'relative';
            document.querySelector('.search-container').appendChild(suggestionsDropdown);
        }
        suggestionsDropdown.innerHTML = '';
        if (suggestions.length === 0) { suggestionsDropdown.style.display = 'none'; return; }
        suggestions.slice(0, 8).forEach(suggestion => {
            const item = document.createElement('div');
            item.className   = 'suggestion-item';
            item.textContent = suggestion.title || suggestion;
            item.onmouseover = () => item.classList.add('hovered');
            item.onmouseout  = () => item.classList.remove('hovered');
            item.onclick     = () => {
                document.getElementById('searchInput').value = suggestion.title || suggestion;
                suggestionsDropdown.style.display = 'none';
                searchAnime();
            };
            suggestionsDropdown.appendChild(item);
        });
        suggestionsDropdown.style.display = 'block';
    }
    function hideSuggestions() { if (suggestionsDropdown) suggestionsDropdown.style.display = 'none'; }
    async function fetchSuggestions(query) {
        if (!query || query.length < 2) { hideSuggestions(); return; }
        if (suggestionCache.has(query)) { showSuggestions(suggestionCache.get(query)); return; }
        try {
            const response = await fetch(`${API_BASE}/suggest/${encodeURIComponent(query)}`);
            const data = await response.json();
            const suggestions = data.results || data.result || [];
            suggestionCache.set(query, suggestions);
            showSuggestions(suggestions);
        } catch(error) { console.error('Suggestion fetch error:', error); }
    }
    document.getElementById('searchInput').addEventListener('input',   e => { clearTimeout(searchTimeout); searchTimeout = setTimeout(() => fetchSuggestions(e.target.value.trim()), 200); });
    document.getElementById('searchInput').addEventListener('keypress', e => { if (e.key === 'Enter') { hideSuggestions(); searchAnime(); } });
    document.getElementById('searchInput').addEventListener('focus',   e => { if (e.target.value.trim().length >= 2) fetchSuggestions(e.target.value.trim()); });
    document.addEventListener('click', e => { if (!e.target.closest('.search-container')) hideSuggestions(); });
    document.getElementById('episodeOverlay').addEventListener('click', e => { if (e.target.id === 'episodeOverlay') closeOverlay(); });

    // =========================================================================
    //  PREFETCH ON LOAD
    // =========================================================================
    function prefetchPopularAnime() {
        ['Naruto', 'One Piece', 'Dragon Ball', 'Bleach', 'My Hero Academia'].forEach(title => {
            setTimeout(() => {
                fetch(`${API_BASE}/suggest/${encodeURIComponent(title)}`)
                    .then(r => r.json())
                    .then(data => suggestionCache.set(title, data.results || data.result || []))
                    .catch(() => {});
            }, Math.random() * 2000);
        });
    }
    window.addEventListener('load', () => {
        setTimeout(prefetchPopularAnime, 4000);
        setTimeout(prefetchAllStaticAnimeIds, 6000);
    });

    // =========================================================================
    //  NATIVE PLAYER — CONTROLS ENGINE
    // =========================================================================
    const roVideo = () => document.getElementById('roVideoEl');
    const roPW    = () => document.getElementById('playerWrapper');

    function roShowSpinner(on) {
        const buf = document.getElementById('roBuffer');
        buf.classList.toggle('active', on);
        if (on) {
            const bar = document.getElementById('roTopbar');
            bar.style.animation = 'none'; bar.offsetHeight; bar.style.animation = '';
        }
    }

    // ── Smart Controls Visibility ───────────────────────────────────────────
    // Outside fullscreen: always visible (CSS default)
    // Inside fullscreen: hidden by default, reveal on interaction, hide after 2s
    function roShowControls() {
        const pw = roPW();
        pw.classList.add('ctrl-show');
        clearTimeout(roCtrlTimeout);
        if (roIsFullscreen) {
            roCtrlTimeout = setTimeout(() => {
                if (!roVideo().paused) pw.classList.remove('ctrl-show');
            }, 2000);
        }
    }
    function roHideControlsIfPlaying() {
        if (roIsFullscreen && !roVideo().paused) roPW().classList.remove('ctrl-show');
    }

    function roFormatTime(s) {
        if (!s || isNaN(s)) return '0:00';
        const m = Math.floor(s / 60), sec = Math.floor(s % 60);
        return `${m}:${sec < 10 ? '0' : ''}${sec}`;
    }

    function roApplyProgress(pct) {
        pct = Math.min(100, Math.max(0, pct));
        document.getElementById('roProgFill').style.width = pct + '%';
        document.getElementById('roProgThumb').style.left = pct + '%';
    }

    const RO_PLAY  = 'M8 5v14l11-7z';
    const RO_PAUSE = 'M6 19h4V5H6v14zm8-14v14h4V5h-4z';

    function roSyncIcons() {
        const paused = roVideo().paused;
        document.getElementById('roPpIcon').innerHTML     = `<path d="${paused ? RO_PLAY : RO_PAUSE}"/>`;
        document.getElementById('roCenterIcon').innerHTML = `<path d="${paused ? RO_PLAY : RO_PAUSE}"/>`;
        document.getElementById('roCenterPlay').classList.toggle('visible', paused);
        document.getElementById('roCenterPlay').classList.toggle('is-play', paused);
        if (!paused) roShowControls();
    }

    function roTogglePlay() {
        const v = roVideo();
        if (v.style.display === 'none') return;
        v.paused ? v.play() : v.pause();
    }

    function roFlashSkip(el) {
        el.classList.add('show');
        setTimeout(() => el.classList.remove('show'), 650);
    }

    // ── Player Wrapper Interaction Handlers ─────────────────────────────────
    const pw = document.getElementById('playerWrapper');
    pw.addEventListener('mousemove',  roShowControls);
    pw.addEventListener('mouseenter', roShowControls);
    pw.addEventListener('mouseleave', () => { clearTimeout(roCtrlTimeout); roHideControlsIfPlaying(); });
    pw.addEventListener('touchstart', () => { roShowControls(); }, { passive: true });
    pw.addEventListener('touchmove',  roShowControls, { passive: true });

    // Play / Pause
    document.getElementById('roPlayBtn').addEventListener('click', roTogglePlay);
    document.getElementById('roCenterPlay').addEventListener('click', (e) => { e.stopPropagation(); roTogglePlay(); });
    document.getElementById('roVideoEl').addEventListener('click',  () => { if (!roIsDragging) roTogglePlay(); });
    document.getElementById('roVideoEl').addEventListener('play',   roSyncIcons);
    document.getElementById('roVideoEl').addEventListener('pause',  roSyncIcons);
    document.getElementById('roVideoEl').addEventListener('ended',  roSyncIcons);

    // Buffering indicators
    document.getElementById('roVideoEl').addEventListener('waiting', () => roShowSpinner(true));
    document.getElementById('roVideoEl').addEventListener('canplay', () => roShowSpinner(false));
    document.getElementById('roVideoEl').addEventListener('playing', () => roShowSpinner(false));

    // Time / Progress update
    document.getElementById('roVideoEl').addEventListener('timeupdate', () => {
        const v = roVideo();
        if (!roIsDragging && v.duration) {
            roApplyProgress((v.currentTime / v.duration) * 100);
            document.getElementById('roCurTime').textContent = roFormatTime(v.currentTime);
        }
    });
    document.getElementById('roVideoEl').addEventListener('loadedmetadata', () => {
        document.getElementById('roDur').textContent = roFormatTime(roVideo().duration);
    });
    document.getElementById('roVideoEl').addEventListener('progress', () => {
        const v = roVideo();
        if (v.buffered.length && v.duration) {
            const end = v.buffered.end(v.buffered.length - 1);
            document.getElementById('roProgBuf').style.width = (end / v.duration * 100) + '%';
        }
    });

    // Progress bar drag
    function roSeekFromEvent(e) {
        const rect = document.getElementById('roProg').getBoundingClientRect();
        const pct  = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));
        const v    = roVideo();
        if (v.duration) v.currentTime = pct * v.duration;
        roApplyProgress(pct * 100);
    }
    document.getElementById('roProg').addEventListener('mousedown', e => { roIsDragging = true; roSeekFromEvent(e); });
    document.addEventListener('mousemove', e => { if (roIsDragging) roSeekFromEvent(e); });
    document.addEventListener('mouseup',   () => { roIsDragging = false; });
    document.getElementById('roProg').addEventListener('touchstart', e => {
        roIsDragging = true; roSeekFromEvent(e.touches[0]);
    }, { passive: true });
    document.addEventListener('touchmove', e => { if (roIsDragging) roSeekFromEvent(e.touches[0]); }, { passive: true });
    document.addEventListener('touchend',  () => { roIsDragging = false; });

    // Rewind / Forward
    document.getElementById('roRwdBtn').addEventListener('click', () => {
        roVideo().currentTime = Math.max(0, roVideo().currentTime - 10);
        roFlashSkip(document.getElementById('roFlashL')); roShowControls();
    });
    document.getElementById('roFwdBtn').addEventListener('click', () => {
        const v = roVideo();
        v.currentTime = Math.min(v.duration || Infinity, v.currentTime + 10);
        roFlashSkip(document.getElementById('roFlashR')); roShowControls();
    });

    // Volume
    const VOL_ON  = 'M3 9v6h4l5 5V4L7 9H3zm13.5 3c0-1.77-1.02-3.29-2.5-4.03v8.05c1.48-.73 2.5-2.25 2.5-4.02zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z';
    const VOL_OFF = 'M16.5 12c0-1.77-1.02-3.29-2.5-4.03v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51C20.63 14.91 21 13.5 21 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06c1.38-.31 2.63-.95 3.69-1.81L19.73 21 21 19.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z';

    function roSyncVolIcon() {
        const v = roVideo();
        document.getElementById('roVolIcon').innerHTML = `<path d="${(v.muted || v.volume === 0) ? VOL_OFF : VOL_ON}"/>`;
    }
    document.getElementById('roVolSlider').addEventListener('input', e => {
        const v = roVideo(); v.volume = e.target.value / 100; v.muted = (e.target.value == 0); roSyncVolIcon();
    });
    document.getElementById('roVolBtn').addEventListener('click', () => {
        const v = roVideo(); v.muted = !v.muted;
        document.getElementById('roVolSlider').value = v.muted ? 0 : v.volume * 100; roSyncVolIcon();
    });

    // =========================================================================
    //  FULLSCREEN ENGINE
    // =========================================================================
    const FS_ENTER = 'M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z';
    const FS_EXIT  = 'M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z';

    function roUpdateFsIcon() {
        document.getElementById('roFsIcon').innerHTML = `<path d="${roIsFullscreen ? FS_EXIT : FS_ENTER}"/>`;
    }

    function roEnterFullscreen() {
        const pw = roPW();
        pw.classList.add('ro-fs-active');
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
        roIsFullscreen = true;
        roUpdateFsIcon();
        const fsReq = pw.requestFullscreen || pw.webkitRequestFullscreen || pw.mozRequestFullScreen || pw.msRequestFullscreen;
        if (fsReq) {
            fsReq.call(pw).then(() => {
                if (screen.orientation && screen.orientation.lock) screen.orientation.lock('landscape').catch(() => {});
            }).catch(() => {});
        }
        if (window.history && window.history.pushState) window.history.pushState({ roFs: true }, '');
        roShowControls();
    }

    function roExitFullscreen() {
        const pw = roPW();
        pw.classList.remove('ro-fs-active');
        pw.classList.remove('ctrl-show');
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
        roIsFullscreen = false;
        roUpdateFsIcon();
        if (screen.orientation && screen.orientation.unlock) try { screen.orientation.unlock(); } catch(e) {}
        const fsExit    = document.exitFullscreen || document.webkitExitFullscreen || document.mozCancelFullScreen || document.msExitFullscreen;
        const isNativeFs = document.fullscreenElement || document.webkitFullscreenElement || document.mozFullScreenElement || document.msFullscreenElement;
        if (isNativeFs && fsExit) fsExit.call(document).catch(() => {});
    }

    document.getElementById('roFsBtn').addEventListener('click', () => {
        if (roIsFullscreen) roExitFullscreen(); else roEnterFullscreen();
    });

    function roOnNativeFsChange() {
        const isNativeFs = !!(document.fullscreenElement || document.webkitFullscreenElement || document.mozFullScreenElement || document.msFullscreenElement);
        if (!isNativeFs && roIsFullscreen) {
            const pw = roPW();
            pw.classList.remove('ro-fs-active'); pw.classList.remove('ctrl-show');
            document.body.style.overflow = ''; document.documentElement.style.overflow = '';
            roIsFullscreen = false; roUpdateFsIcon();
            if (screen.orientation && screen.orientation.unlock) try { screen.orientation.unlock(); } catch(e) {}
        } else if (isNativeFs && !roIsFullscreen) {
            roPW().classList.add('ro-fs-active'); roIsFullscreen = true; roUpdateFsIcon(); roShowControls();
        }
    }
    document.addEventListener('fullscreenchange',       roOnNativeFsChange);
    document.addEventListener('webkitfullscreenchange', roOnNativeFsChange);
    document.addEventListener('mozfullscreenchange',    roOnNativeFsChange);
    document.addEventListener('msfullscreenchange',     roOnNativeFsChange);

    window.addEventListener('popstate', () => { if (roIsFullscreen) roExitFullscreen(); });

    // ── Keyboard Shortcuts ──────────────────────────────────────────────────
    document.addEventListener('keydown', e => {
        if (e.key === 'Escape' && roIsFullscreen) { roExitFullscreen(); return; }
        if (['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName)) return;
        const v = roVideo();
        if (v.style.display === 'none') return;
        if (e.key === ' ' || e.key === 'k') { e.preventDefault(); roTogglePlay(); }
        else if (e.key === 'ArrowRight') { v.currentTime += 5; roFlashSkip(document.getElementById('roFlashR')); roShowControls(); }
        else if (e.key === 'ArrowLeft')  { v.currentTime -= 5; roFlashSkip(document.getElementById('roFlashL')); roShowControls(); }
        else if (e.key === 'f') { if (roIsFullscreen) roExitFullscreen(); else roEnterFullscreen(); }
        else if (e.key === 'm') { document.getElementById('roVolBtn').click(); }
        else if (e.key === 'ArrowUp')   { e.preventDefault(); const s = document.getElementById('roVolSlider'); s.value = Math.min(100, parseInt(s.value) + 10); s.dispatchEvent(new Event('input')); }
        else if (e.key === 'ArrowDown') { e.preventDefault(); const s = document.getElementById('roVolSlider'); s.value = Math.max(0, parseInt(s.value) - 10); s.dispatchEvent(new Event('input')); }
    });

    // =========================================================================
    initHome();
</script>

<footer style="text-align:center;padding:20px;color:var(--text-secondary);font-size:12px;border-top:1px solid var(--border-color);margin-top:40px;">
    Made by aniket with struggle
</footer>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# APP LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await http_client.start()
    logger.info("AniStream v10.4 — Subtitle Fix Edition (Megastatics Support) — Online")
    yield
    await http_client.stop()
    executor.shutdown()

app = FastAPI(lifespan=lifespan, title="AniStream v10.4 — Subtitle Fix Edition (Megastatics Support)")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=512)

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return HTMLResponse(HTML)

# ── V10 unified stream endpoint ──────────────────────────────────────────────
@app.get("/api/v10/stream/{episode_id}")
async def route_unified_stream(
    episode_id: str,
    type: str = Query(default="sub", pattern="^(sub|dub)$"),
):
    """
    Primary stream endpoint called by the player.
    Returns EpisodeResponse with servers[], subtitles[], cdnDomain.
    Servers are labeled VidCloud/VidStream/etc and sorted (VOD CDN first).
    Banned CDNs (storm/strom/crimsonstorm) are removed at collection time.
    """
    res = await get_unified_stream(episode_id)
    return Response(msgspec.json.encode(res), media_type="application/json")

# ── Subtitle Validation Endpoint ─────────────────────────────────────────────
@app.get("/api/v10/subtitle/validate")
async def route_subtitle_validate(
    url:     str = Query(...,  description="VTT subtitle URL to validate"),
    referer: str = Query("",  description="Referer header to use when fetching"),
):
    """
    Backend subtitle validation endpoint.
    Fetches the VTT URL from the server side (bypasses browser CORS) and
    checks whether it contains valid WEBVTT data with at least one cue.

    Response:
        { valid: bool, cue_count: int, url: str }
    """
    if not url:
        raise HTTPException(400, "Missing url param")
    try:
        is_valid, cue_count = await fetch_and_validate_vtt_backend(url, referer)
        return JSONResponse({
            "valid":     is_valid,
            "cue_count": cue_count,
            "url":       url,
        })
    except Exception as e:
        logger.warning(f"[subtitle-validate] Error validating {url[:60]}: {e}")
        return JSONResponse({"valid": False, "cue_count": 0, "url": url, "error": str(e)})

# ── Proxy — handles subtitle VTT CDN fallback + m3u8 manifest proxy ─────────
@app.get("/proxy")
async def route_proxy(url: str = "", referer: str = ""):
    """
    Universal proxy for fetching protected resources:
    - HLS m3u8 manifests (adds correct Referer/Origin headers)
    - VTT subtitle files (CDN fallback used by the subtitle system v4)
    Auto-detects CDN type and sets appropriate Referer when not supplied.
    """
    if not url:
        raise HTTPException(400, "Missing url param")
    return await _proxy_url(url, referer)

# ── HLS chunk proxy (curl_cffi path — unchanged from original) ───────────────
@app.get("/stream.m3u8")
async def stream_m3u8(request: Request, src: str = ""):
    master_url = unquote(src) if src else ""
    if not master_url:
        return Response("No src provided", status_code=400)
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, lambda: cf_fetch(master_url))
    if resp.status_code != 200:
        return Response(content=f"Upstream {resp.status_code}", status_code=502)
    base_local = str(request.base_url).rstrip("/")
    rewritten  = rewrite_m3u8(resp.text, master_url, base_local)
    return Response(
        content    = rewritten,
        media_type = "application/vnd.apple.mpegurl",
        headers    = {
            "Access-Control-Allow-Origin": "*",
            "Content-Type":               "application/vnd.apple.mpegurl; charset=utf-8",
        },
    )

@app.get("/chunk")
async def proxy_chunk(request: Request):
    raw = str(request.url).split("?", 1)[-1]
    url = ""
    for p in raw.split("&"):
        if p.startswith("url="):
            url = p[4:]
            break
    url  = fix_url(unquote(url))
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, lambda: cf_fetch(url))
    if resp.status_code != 200:
        return Response(content=f"Upstream {resp.status_code}", status_code=502)
    text = resp.text
    if is_m3u8(url, text):
        base_local = str(request.base_url).rstrip("/")
        return Response(
            content    = rewrite_m3u8(text, url, base_local),
            media_type = "application/vnd.apple.mpegurl",
            headers    = {
                "Access-Control-Allow-Origin": "*",
                "Content-Type":               "application/vnd.apple.mpegurl; charset=utf-8",
            },
        )
    return Response(
        content    = resp.content,
        media_type = "video/mp2t",
        headers    = {
            "Access-Control-Allow-Origin": "*",
            "Content-Type":               "video/mp2t",
            "Cache-Control":              "public, max-age=3600",
        },
    )

# ── Auxiliary routes ─────────────────────────────────────────────────────────

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

@app.get("/home/thumbnails")
async def route_home():
    res = await get_home()
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/health")
async def health():
    return {
        "status":     "ok",
        "version":    "10.3",
        "cache":      cache.stats(),
        "inflight":   0,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
