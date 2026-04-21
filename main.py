import asyncio
import logging
import random
import re
import time
from bs4 import BeautifulSoup
from collections import OrderedDict
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, quote, unquote
import httpx
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
BASE_URL      "https://9anime.org.lv/"

UPSTREAM_CONCURRENCY = 15
CACHE_MAX_SIZE       = 3000
PARSER_WORKERS       = 4

TTL = {
    "unified_stream":  120,
    "home":            180,
    "search":          600,
    "suggest":         600,
    "episode":         300,
    "banners":         300,
    "recently_added":  300,   # ← NEW
}

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


http_client = HttpClient()
executor    = ThreadPoolExecutor(max_workers=PARSER_WORKERS)

# ─────────────────────────────────────────────────────────────────────────────
# curl_cffi session for HLS proxy
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

session = cf_requests.Session()
HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7",
    "Connection": "keep-alive",
    "Origin": "https://rapid-cloud.co",
    "Referer": "https://rapid-cloud.co/",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36",
}

def fix_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(
        path=parsed.path.replace('+', '%2B'),
        query=parsed.query.replace('+', '%2B') if parsed.query else parsed.query
    ).geturl()


def fetch(url: str) -> cf_requests.Response:
    h = HEADERS.copy()
    h["Host"] = urlparse(url).netloc
    return session.get(fix_url(url), headers=h, impersonate="chrome110", allow_redirects=True)


def rewrite_m3u8(content: str, original_url: str, base_local: str) -> str:
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    base_remote = original_url.rsplit("/", 1)[0] + "/"
    out = []

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
                uri = uri.replace('+', '%2B')
                proxied = f'{base_local}/chunk?url={quote(uri, safe=":/?=&%")}'
                return f'URI="{proxied}"'
            line = re.sub(r'URI="([^"]+)"', repl, line)
            out.append(line)
            continue

        if s.startswith("#"):
            out.append(line)
            continue

        full = s if s.startswith("http") else urljoin(base_remote, s)
        full = full.replace('+', '%2B')
        proxied = f"{base_local}/chunk?url={quote(full, safe=':/?=&%')}"
        out.append(proxied)

    return "\n".join(out)

def is_m3u8(url: str, text: str) -> bool:
    return ".m3u8" in url or text.strip().startswith("#EXTM3U")
# ─────────────────────────────────────────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────────────────────────────────────────
def parse_server_ids_by_type(html: str, ep_type: str) -> List[Tuple[str, str]]:
    parser     = LexborHTMLParser(html)
    server_ids = []
    for item in parser.css("div.item.server-item"):
        dtype = item.attributes.get("data-type", "").lower().strip()
        sid   = item.attributes.get("data-id", "").strip()
        name  = item.text().strip()
        if sid and dtype == ep_type:
            server_ids.append((sid, name))
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

def parse_banners(html: str) -> List[Dict]:
    parser  = LexborHTMLParser(html)
    banners = []
    selectors = [
        "div.deslide-item",
        "div.swiper-slide",
        "div.slider-item",
        "div.banner-item",
        "li.slide-item",
    ]
    items = []
    for sel in selectors:
        items = parser.css(sel)
        if items:
            break
    for item in items:
        a = item.css_first("a[href]")
        if not a:
            continue
        href     = a.attributes.get("href", "")
        parts    = [p for p in href.rstrip("/").split("-") if p.isdigit()]
        anime_id = parts[-1] if parts else None
        if not anime_id:
            continue
        title = (
            a.attributes.get("title") or
            a.attributes.get("aria-label") or
            item.attributes.get("data-title") or
            ""
        ).strip()
        if not title:
            h = item.css_first("h2, h3, .desi-head-title, .title")
            if h:
                title = h.text().strip()
        if not title:
            continue
        img     = item.css_first("img")
        img_url = None
        if img:
            img_url = (
                img.attributes.get("src") or
                img.attributes.get("data-src") or
                img.attributes.get("data-lazy-src")
            )
        if not img_url:
            style = item.attributes.get("style", "")
            m     = re.search(r'url\(["\']?(https?://[^"\')\s]+)["\']?\)', style)
            if m:
                img_url = m.group(1)
        banners.append({
            "title":    title,
            "anime_id": anime_id,
            "img_url":  img_url or "",
        })
    return banners


# ─────────────────────────────────────────────────────────────────────────────
# ★ NEW: parse_recently_added
# ─────────────────────────────────────────────────────────────────────────────
def parse_recently_added(html: str) -> List[Dict]:
    """
    Scrape recently-added / trending anime from the MEW home page.
    Mirrors the user-provided scraper: looks for div.film-poster[data-id]
    inside .flw-item cards, extracting anime_id, title, and poster image.
    """
    parser  = LexborHTMLParser(html)
    results = []
    seen    = set()

    # Primary strategy — same structure as parse_home but pulls data-id from film-poster
    for item in parser.css("div.flw-item"):
        poster = item.css_first("div.film-poster")
        if not poster:
            continue
        anime_id  = (poster.attributes.get("data-id") or "").strip()
        img       = poster.css_first("img")
        if not img or not anime_id:
            continue
        image_url = (
            img.attributes.get("data-src") or
            img.attributes.get("src") or ""
        ).strip()
        # Try film-name anchor first (has richer title), then img alt
        title_node = item.css_first("h3.film-name a, .film-name a")
        if title_node:
            title = (
                title_node.attributes.get("title") or
                title_node.text() or ""
            ).strip()
        else:
            title = (img.attributes.get("alt") or "").strip()
        if title and anime_id and anime_id not in seen:
            seen.add(anime_id)
            results.append({
                "anime_id": anime_id,
                "title":    title,
                "poster":   image_url,
            })

    # Fallback — any film-poster with data-id anywhere on page
    if not results:
        for poster in parser.css("div.film-poster[data-id]"):
            anime_id  = (poster.attributes.get("data-id") or "").strip()
            img       = poster.css_first("img")
            if not img or not anime_id or anime_id in seen:
                continue
            image_url = (
                img.attributes.get("data-src") or
                img.attributes.get("src") or ""
            ).strip()
            title = (img.attributes.get("alt") or "").strip()
            if title and anime_id:
                seen.add(anime_id)
                results.append({
                    "anime_id": anime_id,
                    "title":    title,
                    "poster":   image_url,
                })

    return results


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_date_param() -> str:
    now = datetime.now(timezone.utc)
    raw = f"{now.month}/{now.day}/{now.year} {now.hour:02d}:{now.minute:02d}"
    return quote(raw, safe="")

def is_banned_cdn(url: str) -> bool:
    low = url.lower()
    return "storm" in low or "strom" in low or "douvid" in low

def get_server_label(index: int) -> str:
    if index < len(SERVER_LABELS):
        return SERVER_LABELS[index]
    return f"Server {index + 1}"

def fix_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(
        path  = parsed.path.replace('+', '%2B'),
        query = parsed.query.replace('+', '%2B') if parsed.query else parsed.query
    ).geturl()

def cf_fetch(url: str, referer: str = "") -> cf_requests.Response:
    h = PROXY_HEADERS.copy()
    h["Host"] = urlparse(url).netloc
    return cf_session.get(fix_url(url), headers=h, impersonate="chrome110", allow_redirects=True)

def rewrite_m3u8(content: str, original_url: str, base_local: str, referer: str = "") -> str:
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
async def get_unified_stream(episode_id: str, ep_type: str = "sub") -> EpisodeResponse:
    key           = f"us:{episode_id}:{ep_type}"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh() -> EpisodeResponse:
        date = get_date_param()

        all_servers:       List[ServerSource] = []
        all_subtitles_raw: List[Dict]         = []

        getSources_headers = {
            "Accept":             "*/*",
            "X-Requested-With":   "XMLHttpRequest",
            "User-Agent":         PROXY_HEADERS["User-Agent"],
        }

        async with AsyncClient(
            follow_redirects=True,
            timeout=Timeout(15.0),
        ) as client:

            servers_url = f"{MEW_BASE}/ajax/episode/servers?episodeId={episode_id}&type={ep_type}-{date}"
            logger.info(f"[v10] servers url: {servers_url}")
            try:
                resp = await client.get(servers_url)
                html = resp.json().get("html", "")
            except Exception as e:
                logger.error(f"[v10] servers fetch failed: {e}")
                raise HTTPException(502, "Failed to fetch server list")

            server_items = parse_server_ids_by_type(html, ep_type)
            logger.info(f"[v10] found {len(server_items)} server items for type={ep_type}")

            if not server_items:
                raise HTTPException(404, f"No {ep_type} servers for episode {episode_id}")

            for sid, name in server_items:
                try:
                    src_url = f"{MEW_BASE}/ajax/episode/sources?id={sid}&type={ep_type}-{date}"
                    r       = await client.get(src_url)
                    link    = r.json().get("link", "")
                    if not link:
                        logger.warning(f"[v10] sid={sid} ({name}): empty link")
                        continue

                    if ".vtt" in link:
                        all_subtitles_raw.append({
                            "file": link, "label": "Sub",
                            "kind": "captions", "default": False,
                        })
                        continue

                    if "rapid-cloud.co" not in link and "megacloud" not in link:
                        logger.info(f"[v10] sid={sid} ({name}): unsupported embed host, skipping")
                        continue

                    embed_id = link.rstrip("/").split("/")[-1].split("?")[0]

                    src_resp = await client.get(
                        f"https://rapid-cloud.co/embed-2/v2/e-1/getSources?id={embed_id}",
                        headers={
                            **getSources_headers,
                            "Referer": link,
                        }
                    )
                    api = src_resp.json()
                    logger.info(f"[v10] getSources for {name}/{embed_id}: sources={len(api.get('sources',[]))}, tracks={len(api.get('tracks',[]))}")

                    for t in api.get("tracks", []):
                        if t.get("kind") in ("captions", "subtitles") and t.get("file"):
                            all_subtitles_raw.append({
                                "file":    t["file"],
                                "label":   t.get("label", "Unknown"),
                                "kind":    t["kind"],
                                "default": bool(t.get("default", False)),
                            })

                    for s in api.get("sources", []):
                        m3u8 = s.get("file", "")
                        if not m3u8:
                            continue
                        if is_banned_cdn(m3u8):
                            logger.info(f"[v10] banned CDN url skipped: {m3u8[:60]}")
                            continue
                        idx   = len(all_servers)
                        label = get_server_label(idx)
                        all_servers.append(ServerSource(
                            serverName  = f"{label} (S{idx+1})",
                            serverLabel = label,
                            m3u8Url     = m3u8,
                            qualities   = [QualityInfo(label="Auto", url=m3u8)],
                            referer     = link,
                        ))

                except Exception as e:
                    logger.warning(f"[v10] sid={sid} ({name}) error: {e}")
                    continue

        if not all_servers:
            raise HTTPException(404, "No playable streams found")

        all_servers.sort(key=lambda s: 0 if "vod.netmagcdn.com" in s.m3u8Url.lower() else 1)

        for i, s in enumerate(all_servers):
            s.serverLabel = get_server_label(i)
            s.serverName  = f"{s.serverLabel} (S{i+1})"

        deduped   = deduplicate_subtitles(all_subtitles_raw)
        subtitles = [
            Subtitle(
                file    = t["file"],
                label   = t["label"],
                kind    = t.get("kind", "captions"),
                default = t.get("default", False),
            )
            for t in deduped
        ]

        logger.info(f"[v10] episode={episode_id} type={ep_type}: {len(all_servers)} servers, {len(subtitles)} subtitles")

        result = EpisodeResponse(
            episodeId = episode_id,
            type      = ep_type,
            servers   = all_servers,
            cdnDomain = urlparse(all_servers[0].m3u8Url).hostname or "unknown",
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

async def get_banners() -> List[Dict]:
    key           = "banners"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        html    = await http_client.fetch(f"{BASE_URL}/home")
        loop    = asyncio.get_running_loop()
        results = await loop.run_in_executor(executor, parse_banners, html)
        cache.set(key, results, TTL["banners"])
        return results

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


# ─────────────────────────────────────────────────────────────────────────────
# ★ NEW: get_recently_added service
# ─────────────────────────────────────────────────────────────────────────────
async def get_recently_added() -> List[Dict]:
    """
    Fetches the MEW home page and extracts recently-added anime cards.
    Results are cached for 5 minutes (TTL["recently_added"]).
    """
    key           = "recently_added"
    val, is_stale = cache.get(key)
    if val and not is_stale:
        return val

    async def refresh():
        html    = await http_client.fetch(f"{MEW_BASE}/home")
        loop    = asyncio.get_running_loop()
        results = await loop.run_in_executor(executor, parse_recently_added, html)
        # Deduplicate by anime_id
        seen, unique = set(), []
        for r in results:
            if r["anime_id"] not in seen:
                seen.add(r["anime_id"])
                unique.append(r)
        cache.set(key, unique, TTL["recently_added"])
        logger.info(f"[recently_added] scraped {len(unique)} anime")
        return unique

    if is_stale:
        asyncio.create_task(refresh())
        return val
    return await refresh()


# ─────────────────────────────────────────────────────────────────────────────
# PROXY HELPER
# ─────────────────────────────────────────────────────────────────────────────
async def _proxy_url(url: str, referer: str = "") -> Response:
    if not url:
        raise HTTPException(400, "Missing url")
    loop    = asyncio.get_event_loop()
    cf_resp = await loop.run_in_executor(None, lambda: cf_fetch(url))
    if cf_resp.status_code != 200:
        raise HTTPException(cf_resp.status_code, f"Upstream returned {cf_resp.status_code}")
    content_type = cf_resp.headers.get("Content-Type", "application/octet-stream")
    return Response(
        content    = cf_resp.text,
        media_type = content_type,
        headers    = {"Access-Control-Allow-Origin": "*"},
    )

# ─────────────────────────────────────────────────────────────────────────────
# FRONTEND HTML
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
        /* ===== GOJO SATORU PREMIUM THEME ===== */
        @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Orbitron:wght@400;700;900&family=Inter:wght@300;400;500;600;700;800;900&display=swap');

        :root {
            --primary-black: #03010a;
            --secondary-black: #0d0818;
            --tertiary-black: #1a0f2e;
            --accent-pink: #ff006e;
            --accent-pink-light: #ff1493;
            --accent-pink-dark: #c2185b;
            --accent-neon: #ff0080;
            /* Gojo signature colors */
            --gojo-blue: #00d4ff;
            --gojo-purple: #7c3aed;
            --gojo-violet: #a855f7;
            --gojo-white: #e8e0ff;
            --gojo-void: #0a0015;
            --gojo-infinity: rgba(0, 212, 255, 0.08);
            --limitless-blue: #06b6d4;
            --hollow-purple: #8b5cf6;
            --text-primary: #f0ebff;
            --text-secondary: #c4b5fd;
            --text-tertiary: #7c6fa0;
            --border-color: #2d1f4a;
            --border-glow: rgba(0, 212, 255, 0.3);
            --shadow-sm: 0 2px 8px rgba(0, 212, 255, 0.08);
            --shadow-md: 0 8px 24px rgba(124, 58, 237, 0.2);
            --shadow-lg: 0 16px 48px rgba(124, 58, 237, 0.35);
            --transition-fast: 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            --transition-smooth: 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
            --glow-pink: 0 0 20px rgba(255, 0, 110, 0.5);
            --glow-pink-strong: 0 0 40px rgba(255, 0, 110, 0.8);
            --glow-blue: 0 0 30px rgba(0, 212, 255, 0.6);
            --glow-purple: 0 0 30px rgba(124, 58, 237, 0.5);
            --glow-hollow-purple: 0 0 60px rgba(139, 92, 246, 0.4), 0 0 120px rgba(255, 0, 110, 0.2);
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }
        html, body { width: 100%; height: 100%; overflow-x: hidden; }

        body {
            background: var(--primary-black);
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            color: var(--text-primary);
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
            line-height: 1.6;
        }

        /* ===== INFINITE VOID BACKGROUND ===== */
        body::before {
            content: '';
            position: fixed;
            top: 0; left: 0;
            width: 100%; height: 100%;
            background:
                radial-gradient(ellipse 80% 60% at 20% 10%, rgba(124, 58, 237, 0.12) 0%, transparent 60%),
                radial-gradient(ellipse 60% 40% at 80% 80%, rgba(0, 212, 255, 0.08) 0%, transparent 55%),
                radial-gradient(ellipse 100% 80% at 50% 50%, rgba(255, 0, 110, 0.04) 0%, transparent 70%),
                linear-gradient(135deg, #03010a 0%, #0a0318 30%, #050112 60%, #03010a 100%);
            pointer-events: none;
            z-index: 0;
        }

        /* Infinity domain stars */
        body::after {
            content: '';
            position: fixed;
            top: 0; left: 0;
            width: 100%; height: 100%;
            background-image:
                radial-gradient(1px 1px at 15% 20%, rgba(0, 212, 255, 0.6) 0%, transparent 100%),
                radial-gradient(1px 1px at 35% 65%, rgba(168, 85, 247, 0.5) 0%, transparent 100%),
                radial-gradient(1.5px 1.5px at 55% 12%, rgba(255, 255, 255, 0.3) 0%, transparent 100%),
                radial-gradient(1px 1px at 75% 40%, rgba(0, 212, 255, 0.4) 0%, transparent 100%),
                radial-gradient(1px 1px at 88% 75%, rgba(168, 85, 247, 0.4) 0%, transparent 100%),
                radial-gradient(1px 1px at 25% 88%, rgba(255, 255, 255, 0.2) 0%, transparent 100%),
                radial-gradient(1px 1px at 60% 55%, rgba(0, 212, 255, 0.3) 0%, transparent 100%),
                radial-gradient(2px 2px at 92% 25%, rgba(168, 85, 247, 0.6) 0%, transparent 100%),
                radial-gradient(1px 1px at 45% 35%, rgba(255, 255, 255, 0.25) 0%, transparent 100%),
                radial-gradient(1.5px 1.5px at 10% 50%, rgba(0, 212, 255, 0.35) 0%, transparent 100%);
            pointer-events: none;
            z-index: 0;
            animation: voidStars 8s ease-in-out infinite alternate;
        }

        @keyframes voidStars {
            0%   { opacity: 0.6; transform: scale(1); }
            100% { opacity: 1; transform: scale(1.02); }
        }

        html { scroll-behavior: smooth; }

        /* Scrollbar - Gojo style */
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: var(--secondary-black); }
        ::-webkit-scrollbar-thumb {
            background: linear-gradient(180deg, var(--gojo-blue), var(--hollow-purple));
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb:hover { background: linear-gradient(180deg, var(--gojo-blue), var(--accent-pink)); }

        /* ===== INFINITY CANVAS BACKGROUND ===== */
        #gojoCanvas {
            position: fixed;
            top: 0; left: 0;
            width: 100%; height: 100%;
            pointer-events: none;
            z-index: 0;
            opacity: 0.35;
        }

        /* ===== SUGGESTIONS ===== */
        .suggestion-item {
            padding: 12px 16px;
            cursor: pointer;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border-color);
            transition: background var(--transition-fast), color var(--transition-fast);
            font-size: 13px;
            font-family: 'Inter', sans-serif;
        }
        .suggestion-item.hovered {
            background: rgba(0, 212, 255, 0.08);
            color: var(--gojo-blue);
        }

        /* ===== LIVE BADGE ===== */
        .live-badge {
            display: inline-block;
            background: linear-gradient(135deg, var(--accent-pink), var(--accent-neon));
            color: white;
            font-size: 10px;
            font-weight: 900;
            padding: 2px 8px;
            border-radius: 4px;
            letter-spacing: 1.5px;
            vertical-align: middle;
            font-family: 'Orbitron', sans-serif;
            animation: livePulse 1.6s ease-in-out infinite;
            box-shadow: 0 0 12px rgba(255, 0, 110, 0.5);
        }
        @keyframes livePulse { 0%, 100% { opacity: 1; box-shadow: 0 0 12px rgba(255,0,110,0.5); } 50% { opacity: 0.7; box-shadow: 0 0 24px rgba(255,0,110,0.8); } }

        /* ===== SUB/DUB TOGGLE ===== */
        .sub-dub-toggle { display: flex; align-items: center; gap: 8px; margin-bottom: 16px; }
        .subdub-btn {
            padding: 7px 20px;
            border-radius: 6px;
            border: 1.5px solid var(--border-color);
            background: rgba(13, 8, 24, 0.8);
            color: var(--text-tertiary);
            font-size: 11px;
            font-weight: 800;
            letter-spacing: 1.5px;
            cursor: pointer;
            transition: all var(--transition-fast);
            text-transform: uppercase;
            font-family: 'Orbitron', sans-serif;
            backdrop-filter: blur(10px);
        }
        .subdub-btn.active {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.15), rgba(124, 58, 237, 0.2));
            border-color: var(--gojo-blue);
            color: var(--gojo-blue);
            box-shadow: 0 0 16px rgba(0, 212, 255, 0.3), inset 0 0 12px rgba(0, 212, 255, 0.05);
        }
        .subdub-btn:hover:not(.active) { border-color: var(--gojo-blue); color: var(--gojo-blue); }

        .stream-status { font-size: 10px; font-weight: 700; letter-spacing: 0.5px; color: var(--text-tertiary); text-transform: uppercase; margin-left: 4px; font-family: 'Orbitron', sans-serif; }
        .stream-status.loading { color: var(--gojo-blue); animation: livePulse 1.2s infinite; }
        .stream-status.live { color: #00e676; }
        .stream-status.error { color: #ff5252; }

        /* ===== RECENTLY ADDED ===== */
        #recentlyAddedSection { display: none; animation: fadeInUp 0.8s var(--transition-smooth) 0.1s both; }
        .recently-added-header {
            display: flex; align-items: center; justify-content: space-between;
            margin: 25px 0 16px 0; padding-bottom: 12px;
            border-bottom: 2px solid transparent;
            border-image: linear-gradient(90deg, var(--gojo-blue), var(--hollow-purple), var(--accent-pink)) 1;
        }
        .recently-added-title {
            font-size: 20px; font-weight: 800;
            display: flex; align-items: center; gap: 10px;
            font-family: 'Rajdhani', sans-serif;
            letter-spacing: 2px;
            background: linear-gradient(135deg, var(--gojo-blue), var(--gojo-white));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        @media (min-width: 768px) { .recently-added-title { font-size: 24px; } }
        @media (min-width: 1024px) { .recently-added-title { font-size: 28px; } }
        .recently-added-title::before {
            content: '';
            width: 4px; height: 22px;
            background: linear-gradient(180deg, var(--gojo-blue), var(--hollow-purple));
            border-radius: 3px;
            box-shadow: 0 0 10px rgba(0, 212, 255, 0.6);
        }
        .recently-added-badge {
            display: inline-flex; align-items: center; gap: 5px;
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(124, 58, 237, 0.1));
            border: 1px solid rgba(0, 212, 255, 0.3);
            border-radius: 20px;
            padding: 4px 12px; font-size: 10px; font-weight: 700;
            color: var(--gojo-blue); letter-spacing: 1px; text-transform: uppercase;
            font-family: 'Orbitron', sans-serif;
            box-shadow: 0 0 12px rgba(0, 212, 255, 0.15);
        }
        .recently-added-badge::before {
            content: ''; width: 6px; height: 6px; border-radius: 50%;
            background: var(--gojo-blue);
            box-shadow: 0 0 8px var(--gojo-blue);
            animation: livePulse 1.2s ease-in-out infinite;
        }
        #recentlyAddedGrid {
            display: grid; grid-template-columns: repeat(3, 1fr);
            gap: 10px; margin-bottom: 30px;
        }
        @media (min-width: 480px)  { #recentlyAddedGrid { grid-template-columns: repeat(4, 1fr); gap: 12px; } }
        @media (min-width: 768px)  { #recentlyAddedGrid { grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 40px; } }
        @media (min-width: 1024px) { #recentlyAddedGrid { grid-template-columns: repeat(7, 1fr); gap: 18px; } }
        @media (min-width: 1400px) { #recentlyAddedGrid { grid-template-columns: repeat(9, 1fr); gap: 20px; } }

        .card-new-badge {
            position: absolute; top: 8px; left: 8px; z-index: 5;
            background: linear-gradient(135deg, var(--gojo-blue), var(--hollow-purple));
            color: white; font-size: 8px; font-weight: 900; padding: 2px 7px;
            border-radius: 4px; letter-spacing: 1.5px; text-transform: uppercase;
            box-shadow: 0 0 10px rgba(0, 212, 255, 0.6);
            font-family: 'Orbitron', sans-serif;
        }

        /* ===== SPLASH SCREEN - GOJO DOMAIN EXPANSION ===== */
        #splashScreen {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: var(--gojo-void);
            display: flex; align-items: center; justify-content: center;
            z-index: 9999; opacity: 1;
            transition: opacity 0.8s ease-out;
            overflow: hidden;
        }
        #splashScreen::before {
            content: '';
            position: absolute;
            width: 600px; height: 600px;
            border-radius: 50%;
            background: radial-gradient(circle, rgba(0, 212, 255, 0.06) 0%, rgba(124, 58, 237, 0.08) 40%, transparent 70%);
            animation: splashDomain 3s ease-in-out infinite;
        }
        #splashScreen::after {
            content: '';
            position: absolute;
            width: 300px; height: 300px;
            border-radius: 50%;
            border: 1px solid rgba(0, 212, 255, 0.2);
            box-shadow: 0 0 60px rgba(0, 212, 255, 0.15), inset 0 0 60px rgba(124, 58, 237, 0.1);
            animation: splashDomainRing 3s ease-in-out infinite reverse;
        }
        @keyframes splashDomain {
            0%, 100% { transform: scale(0.8); opacity: 0.5; }
            50% { transform: scale(1.2); opacity: 1; }
        }
        @keyframes splashDomainRing {
            0%, 100% { transform: scale(1) rotate(0deg); border-color: rgba(0, 212, 255, 0.3); }
            50% { transform: scale(1.3) rotate(180deg); border-color: rgba(168, 85, 247, 0.5); }
        }
        #splashScreen.hidden { opacity: 0; pointer-events: none; }
        .splash-content { text-align: center; position: relative; z-index: 10; }
        .splash-logo {
            width: 90px; height: 90px; margin: 0 auto 20px;
            border-radius: 50%;
            border: 2px solid transparent;
            background: linear-gradient(var(--gojo-void), var(--gojo-void)) padding-box,
                        linear-gradient(135deg, var(--gojo-blue), var(--hollow-purple), var(--accent-pink)) border-box;
            display: flex; align-items: center; justify-content: center;
            box-shadow: 0 0 40px rgba(0, 212, 255, 0.4), 0 0 80px rgba(124, 58, 237, 0.2);
            animation: splashGlow 2s ease-in-out infinite;
            position: relative;
        }
        .splash-logo::before {
            content: '';
            position: absolute;
            inset: -8px;
            border-radius: 50%;
            border: 1px solid rgba(0, 212, 255, 0.15);
            animation: splashDomainRing 4s linear infinite;
        }
        .splash-logo::after {
            content: '';
            position: absolute;
            inset: -16px;
            border-radius: 50%;
            border: 1px solid rgba(168, 85, 247, 0.1);
            animation: splashDomainRing 6s linear infinite reverse;
        }
        @keyframes splashGlow {
            0%, 100% { box-shadow: 0 0 40px rgba(0, 212, 255, 0.4), 0 0 80px rgba(124, 58, 237, 0.2); }
            50% { box-shadow: 0 0 70px rgba(0, 212, 255, 0.7), 0 0 140px rgba(124, 58, 237, 0.4), 0 0 200px rgba(168, 85, 247, 0.2); }
        }
        .splash-logo img { width: 88%; height: 88%; border-radius: 50%; object-fit: cover; filter: brightness(1.1) contrast(1.05); }
        .splash-text {
            font-size: 28px; font-weight: 900;
            font-family: 'Orbitron', sans-serif;
            background: linear-gradient(135deg, var(--gojo-blue) 0%, var(--gojo-white) 50%, var(--hollow-purple) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            letter-spacing: 6px; text-transform: uppercase; margin-bottom: 8px;
            animation: splashText 2s ease-in-out infinite;
        }
        .splash-sub {
            font-size: 10px;
            color: rgba(0, 212, 255, 0.5);
            letter-spacing: 4px;
            text-transform: uppercase;
            font-family: 'Orbitron', sans-serif;
            margin-bottom: 20px;
        }
        @keyframes splashText {
            0%, 100% { opacity: 1; text-shadow: 0 0 20px rgba(0, 212, 255, 0.5); }
            50% { opacity: 0.8; text-shadow: 0 0 40px rgba(0, 212, 255, 0.9), 0 0 60px rgba(168, 85, 247, 0.4); }
        }
        .splash-dots { display: flex; justify-content: center; gap: 10px; }
        .splash-dot { width: 8px; height: 8px; border-radius: 50%; animation: splashDot 1.2s ease-in-out infinite; }
        .splash-dot:nth-child(1) { background: var(--gojo-blue); animation-delay: 0s; }
        .splash-dot:nth-child(2) { background: var(--hollow-purple); animation-delay: 0.2s; }
        .splash-dot:nth-child(3) { background: var(--accent-pink); animation-delay: 0.4s; }
        @keyframes splashDot {
            0%, 100% { opacity: 0.3; transform: scale(0.8); box-shadow: none; }
            50% { opacity: 1; transform: scale(1.3); box-shadow: 0 0 10px currentColor; }
        }

        /* ===== HEADER ===== */
        header {
            background: rgba(3, 1, 10, 0.85);
            padding: 12px 20px;
            display: flex; align-items: center; justify-content: space-between;
            backdrop-filter: blur(20px) saturate(1.5);
            -webkit-backdrop-filter: blur(20px) saturate(1.5);
            border-bottom: 1px solid transparent;
            background-clip: padding-box;
            position: sticky; top: 0; z-index: 1000;
            box-shadow: 0 0 30px rgba(0, 212, 255, 0.08), 0 4px 20px rgba(0, 0, 0, 0.6);
            animation: slideDown 0.6s var(--transition-smooth);
            gap: 12px; flex-wrap: wrap;
            /* Animated border */
            border-image: none;
        }
        header::after {
            content: '';
            position: absolute;
            bottom: 0; left: 0; right: 0;
            height: 1px;
            background: linear-gradient(90deg,
                transparent 0%,
                rgba(0, 212, 255, 0.6) 20%,
                rgba(168, 85, 247, 0.8) 50%,
                rgba(255, 0, 110, 0.6) 80%,
                transparent 100%
            );
            background-size: 200% 100%;
            animation: headerBorderFlow 4s linear infinite;
        }
        @keyframes headerBorderFlow {
            0% { background-position: 200% 0; }
            100% { background-position: -200% 0; }
        }
        @keyframes slideDown { from { opacity: 0; transform: translateY(-20px); } to { opacity: 1; transform: translateY(0); } }

        .header-left { display: flex; align-items: center; gap: 12px; min-width: 0; }

        /* ===== LOGO - GOJO STYLE ===== */
        .logo {
            color: var(--text-primary); font-weight: 900; font-size: 20px;
            letter-spacing: 3px;
            text-transform: uppercase; text-decoration: none; cursor: pointer;
            display: flex; align-items: center; gap: 10px;
            transition: all var(--transition-fast);
            background: linear-gradient(135deg, var(--gojo-blue), var(--gojo-white), var(--hollow-purple));
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            background-clip: text;
            font-family: 'Orbitron', sans-serif;
            white-space: nowrap;
        }
        .logo:hover { transform: scale(1.05); filter: brightness(1.3); }

        .logo-gojo {
            width: 38px; height: 38px; border-radius: 50%; overflow: hidden;
            border: 2px solid transparent;
            background: linear-gradient(var(--secondary-black), var(--secondary-black)) padding-box,
                        linear-gradient(135deg, var(--gojo-blue), var(--hollow-purple)) border-box;
            display: flex; align-items: center; justify-content: center; flex-shrink: 0;
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.4), 0 0 40px rgba(124, 58, 237, 0.2);
            animation: gojoAura 3s ease-in-out infinite;
            position: relative;
        }
        .logo-gojo::before {
            content: '';
            position: absolute;
            inset: -4px;
            border-radius: 50%;
            border: 1px solid rgba(0, 212, 255, 0.2);
            animation: splashDomainRing 3s linear infinite;
        }
        @keyframes gojoAura {
            0%, 100% { box-shadow: 0 0 20px rgba(0, 212, 255, 0.4), 0 0 40px rgba(124, 58, 237, 0.2); }
            50% { box-shadow: 0 0 35px rgba(0, 212, 255, 0.7), 0 0 70px rgba(168, 85, 247, 0.4), 0 0 100px rgba(0, 212, 255, 0.2); }
        }
        .logo-gojo img { width: 100%; height: 100%; object-fit: cover; }

        /* ===== SEARCH ===== */
        .search-container { display: flex; flex: 1; min-width: 200px; max-width: 500px; position: relative; gap: 8px; }
        .search-container input {
            flex: 1; padding: 10px 14px; border-radius: 8px;
            border: 1px solid var(--border-color);
            background: rgba(13, 8, 24, 0.8);
            color: var(--text-primary); font-size: 13px; outline: none;
            transition: all var(--transition-fast);
            font-family: 'Inter', sans-serif;
            backdrop-filter: blur(10px);
        }
        .search-container input::placeholder { color: var(--text-tertiary); }
        .search-container input:focus {
            background: rgba(26, 15, 46, 0.9);
            border-color: var(--gojo-blue);
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.2), 0 0 40px rgba(124, 58, 237, 0.1), inset 0 0 10px rgba(0, 212, 255, 0.03);
        }
        .search-container button {
            background: linear-gradient(135deg, var(--gojo-blue), var(--hollow-purple));
            border: none; color: white; padding: 10px 16px; border-radius: 8px;
            cursor: pointer; font-weight: 700; font-size: 10px;
            transition: all var(--transition-fast);
            text-transform: uppercase; letter-spacing: 1px;
            box-shadow: 0 4px 15px rgba(0, 212, 255, 0.25);
            white-space: nowrap; flex-shrink: 0;
            font-family: 'Orbitron', sans-serif;
        }
        .search-container button:hover { transform: scale(1.05); box-shadow: 0 8px 25px rgba(0, 212, 255, 0.5); }
        .search-container button:active { transform: scale(0.98); }

        @media (max-width: 600px) {
            header { flex-direction: column; align-items: stretch; padding: 12px 16px; }
            .header-left { width: 100%; justify-content: center; margin-bottom: 8px; }
            .search-container { width: 100%; }
        }

        /* ===== HERO BANNER ===== */
        #heroBanner {
            position: relative; width: 100%; height: 250px; border-radius: 16px;
            overflow: hidden; margin-bottom: 20px;
            border: 1px solid rgba(0, 212, 255, 0.2);
            box-shadow: 0 0 40px rgba(0, 212, 255, 0.1), 0 20px 60px rgba(0, 0, 0, 0.7);
            display: none;
        }
        #heroBanner::before {
            content: '';
            position: absolute;
            inset: 0;
            border-radius: 16px;
            border: 1px solid transparent;
            background: linear-gradient(135deg, rgba(0,212,255,0.15), rgba(168,85,247,0.1)) border-box;
            -webkit-mask: linear-gradient(#fff 0 0) padding-box, linear-gradient(#fff 0 0);
            -webkit-mask-composite: destination-out;
            mask-composite: exclude;
            pointer-events: none;
            z-index: 5;
        }
        @media (min-width: 768px) { #heroBanner { height: 350px; margin-bottom: 30px; display: block; } }
        @media (min-width: 1024px) { #heroBanner { height: 450px; margin-bottom: 40px; } }
        .hero-slider-container { width: 100%; height: 100%; position: relative; }
        .hero-slide {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background-size: cover; background-position: center;
            opacity: 0; transition: opacity 0.8s ease-in-out;
            display: flex; align-items: flex-end;
        }
        .hero-slide.active { opacity: 1; z-index: 1; }
        .hero-slide-overlay {
            width: 100%; padding: 40px;
            background: linear-gradient(0deg, rgba(3, 1, 10, 0.95) 0%, rgba(3, 1, 10, 0.5) 50%, transparent 100%);
            color: white;
        }
        .hero-slide-title {
            font-size: 28px; font-weight: 900; margin-bottom: 8px;
            text-transform: uppercase; letter-spacing: 2px;
            font-family: 'Rajdhani', sans-serif;
            background: linear-gradient(135deg, #fff 0%, var(--gojo-white) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            text-shadow: none;
            filter: drop-shadow(0 0 20px rgba(0, 212, 255, 0.3));
        }
        @media (min-width: 768px) { .hero-slide-title { font-size: 36px; } }
        .hero-slide-ep {
            font-size: 11px; font-weight: 700; color: var(--gojo-blue);
            text-transform: uppercase; letter-spacing: 3px;
            font-family: 'Orbitron', sans-serif;
        }
        .hero-watch-btn {
            display: inline-block; margin-top: 14px; padding: 12px 32px;
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.15), rgba(124, 58, 237, 0.2));
            color: var(--gojo-blue); border: 1px solid rgba(0, 212, 255, 0.4);
            border-radius: 8px; font-size: 12px; font-weight: 900;
            letter-spacing: 2px; text-transform: uppercase; cursor: pointer;
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.2), inset 0 0 20px rgba(0, 212, 255, 0.05);
            transition: all var(--transition-smooth);
            position: relative; overflow: hidden;
            font-family: 'Orbitron', sans-serif;
            backdrop-filter: blur(10px);
        }
        .hero-watch-btn::before {
            content: '';
            position: absolute; top: 0; left: -100%; width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(0, 212, 255, 0.3), transparent);
            transition: left 0.6s;
        }
        .hero-watch-btn:hover {
            transform: scale(1.06);
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.25), rgba(124, 58, 237, 0.3));
            box-shadow: 0 0 40px rgba(0, 212, 255, 0.4), 0 8px 30px rgba(124, 58, 237, 0.3);
            color: white;
            border-color: var(--gojo-blue);
        }
        .hero-watch-btn:hover::before { left: 100%; }
        .hero-controls { position: absolute; bottom: 20px; right: 20px; display: flex; gap: 10px; z-index: 10; }
        .hero-nav-btn {
            width: 40px; height: 40px; border: none; border-radius: 50%;
            background: rgba(0, 212, 255, 0.1);
            color: var(--gojo-blue);
            cursor: pointer; font-size: 16px;
            display: flex; align-items: center; justify-content: center;
            transition: all var(--transition-fast); backdrop-filter: blur(10px);
            border: 1px solid rgba(0, 212, 255, 0.3);
            box-shadow: 0 0 15px rgba(0, 212, 255, 0.2);
        }
        @media (min-width: 768px) { .hero-nav-btn { width: 48px; height: 48px; font-size: 20px; } }
        .hero-nav-btn:hover {
            background: rgba(0, 212, 255, 0.2);
            color: white;
            transform: scale(1.15);
            box-shadow: 0 0 25px rgba(0, 212, 255, 0.5);
        }

        /* ===== MAIN ===== */
        main { padding: 20px 16px; max-width: 1600px; margin: 0 auto; position: relative; z-index: 1; }
        @media (min-width: 768px) { main { padding: 30px 24px; } }
        @media (min-width: 1024px) { main { padding: 40px 24px; } }

        /* ===== SECTION TITLES ===== */
        .section-title {
            font-size: 20px; font-weight: 800;
            margin: 25px 0 16px 0;
            padding-bottom: 12px;
            display: flex; align-items: center; gap: 10px;
            animation: fadeInUp 0.6s var(--transition-smooth);
            font-family: 'Rajdhani', sans-serif;
            letter-spacing: 3px;
            background: linear-gradient(135deg, var(--gojo-blue), var(--gojo-white) 60%, var(--hollow-purple));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            text-transform: uppercase;
            border-bottom: 1px solid;
            border-image: linear-gradient(90deg, var(--gojo-blue), var(--hollow-purple), transparent) 1;
        }
        @media (min-width: 768px)  { .section-title { font-size: 24px; margin: 30px 0 20px 0; } }
        @media (min-width: 1024px) { .section-title { font-size: 28px; margin: 40px 0 24px 0; } }
        @keyframes fadeInUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
        .section-title::before {
            content: ''; width: 4px; height: 22px;
            background: linear-gradient(180deg, var(--gojo-blue), var(--hollow-purple));
            border-radius: 3px;
            box-shadow: 0 0 10px rgba(0, 212, 255, 0.6);
            animation: scaleY 0.8s var(--transition-smooth);
            flex-shrink: 0;
        }
        @keyframes scaleY { from { transform: scaleY(0); } to { transform: scaleY(1); } }

        /* ===== GRIDS ===== */
        #grid, #famousGrid, #dynamicGrid {
            display: grid; grid-template-columns: repeat(3, 1fr);
            gap: 10px; margin-bottom: 30px;
            animation: fadeInUp 0.8s var(--transition-smooth) 0.2s both;
        }
        @media (min-width: 480px) { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(4, 1fr); gap: 12px; } }
        @media (min-width: 768px) { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 40px; } }
        @media (min-width: 1024px) { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(7, 1fr); gap: 18px; } }
        @media (min-width: 1400px) { #grid, #famousGrid, #dynamicGrid { grid-template-columns: repeat(9, 1fr); gap: 20px; } }

        /* ===== CARDS - GOJO INFINITY STYLE ===== */
        .card {
            background: linear-gradient(135deg, rgba(26, 15, 46, 0.9), rgba(13, 8, 24, 0.95));
            border-radius: 12px; overflow: hidden;
            transition: all 0.35s cubic-bezier(0.34, 1.56, 0.64, 1);
            cursor: pointer; display: flex; flex-direction: column;
            border: 1px solid rgba(45, 31, 74, 0.8);
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4), 0 0 0 0 rgba(0, 212, 255, 0);
            position: relative; animation: fadeIn 0.6s ease-out;
            will-change: transform;
        }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

        .card::before {
            content: '';
            position: absolute; inset: 0; border-radius: 12px;
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.0), rgba(168, 85, 247, 0.0));
            transition: all 0.35s ease;
            pointer-events: none; z-index: 1;
        }

        .card:hover {
            transform: translateY(-16px) scale(1.06) rotateY(2deg);
            border-color: rgba(0, 212, 255, 0.5);
            box-shadow:
                0 0 0 1px rgba(0, 212, 255, 0.2),
                0 0 30px rgba(0, 212, 255, 0.25),
                0 0 60px rgba(124, 58, 237, 0.15),
                0 24px 50px rgba(0, 0, 0, 0.7);
            z-index: 10;
        }
        .card:hover::before {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.04), rgba(168, 85, 247, 0.04));
        }

        .card-img-wrapper {
            position: relative; width: 100%; aspect-ratio: 2 / 3;
            background: linear-gradient(135deg, var(--secondary-black), var(--tertiary-black));
            overflow: hidden; flex-shrink: 0; border-radius: 11px 11px 0 0;
        }
        @supports not (aspect-ratio: 2/3) { .card-img-wrapper { padding-top: 150%; height: 0; } }
        .card-img-wrapper.loading { animation: shimmer 2s infinite; }
        @keyframes shimmer {
            0% { background: linear-gradient(90deg, var(--secondary-black), rgba(0, 212, 255, 0.05), var(--secondary-black)); background-size: 200%; background-position: 200% 0; }
            100% { background-position: -200% 0; }
        }
        .card img {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            object-fit: cover; opacity: 0;
            transition: opacity 0.6s ease-out, filter 0.6s ease-out, transform 0.5s ease-out;
            filter: blur(8px) saturate(0.5);
        }
        .card img.loaded { opacity: 1; filter: blur(0) saturate(1.05) brightness(0.95); }
        .card:hover img.loaded { transform: scale(1.08); filter: saturate(1.1) brightness(0.9); }

        .card-overlay {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background: linear-gradient(180deg, rgba(0, 0, 0, 0.1) 0%, rgba(3, 1, 10, 0.97) 100%);
            opacity: 0; transition: opacity var(--transition-fast);
            display: flex; align-items: flex-end; justify-content: center; padding-bottom: 14px;
            backdrop-filter: blur(4px);
        }
        .card:hover .card-overlay { opacity: 1; }

        .card-watch-btn {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.15), rgba(124, 58, 237, 0.2));
            color: var(--gojo-blue);
            border: 1px solid rgba(0, 212, 255, 0.4);
            border-radius: 6px; padding: 8px 16px;
            font-size: 10px; font-weight: 900; letter-spacing: 1.5px;
            text-transform: uppercase; cursor: pointer;
            box-shadow: 0 0 15px rgba(0, 212, 255, 0.2);
            transition: all var(--transition-fast);
            white-space: nowrap; position: relative; overflow: hidden;
            font-family: 'Orbitron', sans-serif;
            backdrop-filter: blur(10px);
        }
        .card-watch-btn::before {
            content: ''; position: absolute; top: 0; left: -100%; width: 100%; height: 100%;
            background: linear-gradient(90deg, transparent, rgba(0, 212, 255, 0.3), transparent);
            transition: left 0.5s;
        }
        .card-watch-btn:hover {
            background: rgba(0, 212, 255, 0.25);
            color: white;
            box-shadow: 0 0 25px rgba(0, 212, 255, 0.4);
        }
        .card-watch-btn:hover::before { left: 100%; }

        .card-info {
            padding: 8px; flex-grow: 1;
            display: flex; flex-direction: column;
            justify-content: space-between;
            background: linear-gradient(135deg, rgba(13, 8, 24, 0.98), rgba(26, 15, 46, 0.98));
        }
        @media (min-width: 768px)  { .card-info { padding: 10px; } }
        @media (min-width: 1024px) { .card-info { padding: 12px; } }

        .card-title {
            font-size: 11px; font-weight: 600; color: var(--text-primary);
            line-height: 1.3; margin-bottom: 4px;
            display: -webkit-box; -webkit-line-clamp: 2;
            -webkit-box-orient: vertical; overflow: hidden;
            font-family: 'Inter', sans-serif;
        }
        @media (min-width: 768px)  { .card-title { font-size: 12px; margin-bottom: 6px; } }
        @media (min-width: 1024px) { .card-title { font-size: 13px; } }

        .card-meta {
            font-size: 9px; color: var(--gojo-blue);
            font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px;
            font-family: 'Orbitron', sans-serif;
            opacity: 0.7;
        }

        /* ===== STREAMING SECTION ===== */
        #streamingSection { display: none; animation: fadeIn 0.6s ease-out; }
        #videoContainer { margin-bottom: 20px; }
        #videoContainer.hidden { display: none; }

        .player-wrapper {
            position: relative; width: 100%; aspect-ratio: 16 / 9; background: #000;
            border-radius: 14px; overflow: hidden;
            border: 1px solid rgba(0, 212, 255, 0.25);
            box-shadow:
                0 0 0 1px rgba(0, 212, 255, 0.08),
                0 0 40px rgba(0, 212, 255, 0.1),
                0 0 80px rgba(124, 58, 237, 0.1),
                0 20px 50px rgba(0, 0, 0, 0.8);
            cursor: default; user-select: none;
            max-height: 65vh; transition: all 0.3s ease;
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

        /* Subtitle overlay */
        #roSubtitleOverlay {
            position: absolute; bottom: 52px; left: 50%; transform: translateX(-50%);
            z-index: 35; pointer-events: none; text-align: center; width: 80%;
            max-width: 700px; min-width: 200px;
        }
        #roSubtitleOverlay .ro-sub-line {
            display: inline; background: transparent; color: #ffffff;
            -webkit-text-stroke: 2.5px #000; paint-order: stroke fill;
            font-size: clamp(15px, 2.4vw, 22px); font-weight: 800; line-height: 1.75;
            letter-spacing: 0.3px; font-family: 'Inter', sans-serif;
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

        /* Buffer/Loading */
        .ro-buffer { position: absolute; inset: 0; display: none; z-index: 50; pointer-events: none; }
        .ro-buffer.active { display: block; }
        .ro-topbar {
            position: absolute; top: 0; left: 0; height: 2px; width: 0%;
            background: linear-gradient(90deg, var(--gojo-blue), var(--hollow-purple), var(--gojo-blue));
            background-size: 200% 100%; border-radius: 0 2px 2px 0;
            box-shadow: 0 0 12px rgba(0, 212, 255, 0.8), 0 0 4px rgba(0, 212, 255, 1);
            animation: roTopbarFill 1.8s cubic-bezier(0.4,0,0.2,1) forwards, roTopbarShimmer 1.2s linear infinite;
            z-index: 51;
        }
        @keyframes roTopbarFill {
            0% { width: 0%; } 30% { width: 40%; } 60% { width: 65%; } 80% { width: 80%; } 95% { width: 90%; } 100% { width: 90%; }
        }
        @keyframes roTopbarShimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
        .ro-topbar::after {
            content: ''; position: absolute; right: -2px; top: 50%; transform: translateY(-50%);
            width: 8px; height: 8px; border-radius: 50%; background: var(--gojo-blue);
            box-shadow: 0 0 10px 4px rgba(0, 212, 255, 0.6);
        }
        .ro-buffer-overlay { position: absolute; inset: 0; background: rgba(0,0,0,0.2); z-index: 50; }
        .ro-spinner-wrap, .ro-ring-outer, .ro-ring-inner, .ro-ring-inner2, .ro-buffer-label { display: none !important; }

        /* Center play button */
        .ro-center-play {
            position: absolute; top: 50%; left: 50%;
            transform: translate(-50%, -50%) scale(0.75); width: 74px; height: 74px;
            border-radius: 50%;
            background: rgba(0, 212, 255, 0.08);
            border: 1.5px solid rgba(0, 212, 255, 0.3);
            backdrop-filter: blur(14px) saturate(1.5);
            -webkit-backdrop-filter: blur(14px) saturate(1.5);
            display: flex; align-items: center; justify-content: center;
            cursor: pointer; z-index: 40; opacity: 0; pointer-events: none;
            transition: opacity 0.25s ease, transform 0.25s ease, border-color 0.2s ease, background 0.2s ease;
        }
        .ro-center-play.visible { opacity: 1; transform: translate(-50%, -50%) scale(1); pointer-events: all; }
        .ro-center-play:hover {
            border-color: rgba(0, 212, 255, 0.7);
            background: rgba(0, 212, 255, 0.15);
            box-shadow: 0 0 30px rgba(0, 212, 255, 0.3);
        }
        .ro-center-play svg { width: 26px; height: 26px; fill: var(--gojo-blue); }
        .ro-center-play.is-play svg { margin-left: 3px; }

        /* Skip indicators */
        .ro-skip {
            position: absolute; top: 50%; transform: translateY(-50%);
            background: rgba(0, 0, 0, 0.6); backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            border: 1px solid rgba(0, 212, 255, 0.2); border-radius: 10px;
            padding: 9px 20px; font-size: 12px; font-weight: 800; letter-spacing: 1px;
            color: var(--gojo-blue); opacity: 0; pointer-events: none;
            transition: opacity 0.18s ease; z-index: 45;
            font-family: 'Orbitron', sans-serif;
        }
        .ro-skip.left  { left: 7%; }
        .ro-skip.right { right: 7%; }
        .ro-skip.show  { opacity: 1; }

        /* Controls bar */
        .ro-controls {
            position: absolute; bottom: 0; left: 0; right: 0;
            background: linear-gradient(0deg, rgba(3, 1, 10, 0.98) 0%, rgba(3, 1, 10, 0.7) 40%, rgba(3, 1, 10, 0.3) 70%, transparent 100%);
            padding: 52px 18px 16px; z-index: 60;
            transform: translateY(0); opacity: 1;
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
            font-size: 11px; font-weight: 600; letter-spacing: 0.5px;
            color: rgba(200, 180, 255, 0.5); margin-bottom: 10px;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
            font-family: 'Inter', sans-serif;
        }
        .ro-progress {
            width: 100%; height: 3px; background: rgba(255, 255, 255, 0.08);
            border-radius: 99px; cursor: pointer; margin-bottom: 12px;
            position: relative; transition: height 0.18s ease;
        }
        .ro-progress:hover { height: 5px; }
        .ro-prog-buf { position: absolute; left: 0; top: 0; height: 100%; background: rgba(0, 212, 255, 0.1); border-radius: 99px; width: 0%; pointer-events: none; }
        .ro-prog-fill {
            position: absolute; left: 0; top: 0; height: 100%;
            background: linear-gradient(90deg, var(--gojo-blue), var(--hollow-purple));
            border-radius: 99px; width: 0%; pointer-events: none;
            box-shadow: 0 0 10px rgba(0, 212, 255, 0.6);
        }
        .ro-prog-thumb {
            position: absolute; top: 50%; left: 0%; transform: translate(-50%, -50%) scale(0);
            width: 13px; height: 13px; border-radius: 50%; background: var(--gojo-blue);
            box-shadow: 0 0 10px rgba(0, 212, 255, 0.8); pointer-events: none;
            transition: transform 0.15s ease;
        }
        .ro-progress:hover .ro-prog-thumb { transform: translate(-50%, -50%) scale(1); }

        .ro-row { display: flex; align-items: center; justify-content: space-between; }
        .ro-left, .ro-right { display: flex; align-items: center; gap: 2px; }
        .ro-btn {
            background: transparent; border: none; color: rgba(200, 180, 255, 0.7);
            width: 36px; height: 36px; border-radius: 8px; cursor: pointer;
            display: flex; align-items: center; justify-content: center;
            transition: background 0.15s, color 0.15s, transform 0.15s; flex-shrink: 0;
        }
        .ro-btn:hover { background: rgba(0, 212, 255, 0.12); color: var(--gojo-blue); transform: scale(1.1); }
        .ro-btn svg { width: 18px; height: 18px; fill: currentColor; }
        .ro-btn.lg svg { width: 22px; height: 22px; }
        .ro-time { font-size: 11px; font-weight: 600; color: rgba(200, 180, 255, 0.5); white-space: nowrap; padding: 0 8px; font-variant-numeric: tabular-nums; letter-spacing: 0.3px; font-family: 'Inter', sans-serif; }

        .ro-vol-group { display: flex; align-items: center; gap: 2px; max-width: 36px; overflow: hidden; transition: max-width 0.3s ease; }
        .ro-vol-group:hover { max-width: 128px; }
        .ro-vol-slider { -webkit-appearance: none; appearance: none; width: 72px; height: 3px; background: rgba(0, 212, 255, 0.15); border-radius: 99px; outline: none; cursor: pointer; flex-shrink: 0; }
        .ro-vol-slider::-webkit-slider-thumb { -webkit-appearance: none; width: 12px; height: 12px; border-radius: 50%; background: var(--gojo-blue); cursor: pointer; box-shadow: 0 0 6px rgba(0, 212, 255, 0.5); }
        .ro-vol-slider::-moz-range-thumb { width: 12px; height: 12px; border-radius: 50%; background: var(--gojo-blue); border: none; cursor: pointer; }

        /* Server pills */
        .server-pill-row { display: flex; align-items: center; gap: 6px; margin-bottom: 12px; flex-wrap: wrap; }
        .server-pill-label { font-size: 9px; font-weight: 900; letter-spacing: 1.5px; text-transform: uppercase; color: var(--text-tertiary); margin-right: 4px; font-family: 'Orbitron', sans-serif; }
        .srv-pill {
            padding: 5px 14px; border-radius: 99px; border: 1px solid var(--border-color);
            background: rgba(13, 8, 24, 0.8); color: var(--text-tertiary);
            font-size: 10px; font-weight: 800; letter-spacing: 0.8px; cursor: pointer;
            transition: all 0.2s ease; text-transform: uppercase; white-space: nowrap;
            font-family: 'Orbitron', sans-serif;
            backdrop-filter: blur(10px);
        }
        .srv-pill.active {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(124, 58, 237, 0.15));
            border-color: var(--gojo-blue); color: var(--gojo-blue);
            box-shadow: 0 0 15px rgba(0, 212, 255, 0.25);
        }
        .srv-pill:hover:not(.active) { border-color: var(--gojo-blue); color: var(--gojo-blue); }
        .srv-pill.failed  { border-color: #ff5252; color: #ff5252; opacity: 0.5; }
        .srv-pill.loading { border-color: var(--gojo-blue); color: var(--gojo-blue); animation: pilPulse 1s infinite; }
        @keyframes pilPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }

        /* Subtitle dropdown */
        #roSubMenuWrap { position: relative; display: inline-block; }
        .ro-sub-dropdown {
            display: none; position: absolute; bottom: 44px; right: 0;
            background: rgba(13, 8, 24, 0.97); backdrop-filter: blur(20px);
            border: 1px solid rgba(0, 212, 255, 0.3); border-radius: 10px;
            min-width: 200px; max-height: 280px; overflow-y: auto; z-index: 9999;
            box-shadow: 0 8px 32px rgba(0, 212, 255, 0.15), 0 2px 8px rgba(0, 0, 0, 0.5);
            animation: dropIn 0.15s ease-out;
        }
        @keyframes dropIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
        .ro-sub-dropdown.open { display: block; }
        .ro-sub-dropdown-item {
            padding: 10px 16px; cursor: pointer; font-size: 12px; font-weight: 700;
            color: rgba(200, 180, 255, 0.7); transition: background 0.15s, color 0.15s;
            border-left: 3px solid transparent; user-select: none; white-space: nowrap;
            display: flex; align-items: center; gap: 8px; font-family: 'Inter', sans-serif;
        }
        .ro-sub-dropdown-item:hover { background: rgba(0, 212, 255, 0.08); color: var(--gojo-blue); }
        .ro-sub-dropdown-item.active-sub { color: var(--gojo-blue); border-left-color: var(--gojo-blue); background: rgba(0, 212, 255, 0.05); }
        .ro-sub-dropdown-item.loading-sub { color: rgba(200, 180, 255, 0.3); cursor: wait; animation: subItemPulse 1s ease-in-out infinite; }
        .ro-sub-dropdown-item.failed-sub { color: #ff5252; opacity: 0.6; }
        @keyframes subItemPulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .ro-sub-dropdown-item:first-child { border-radius: 8px 8px 0 0; }
        .ro-sub-dropdown-item:last-child  { border-radius: 0 0 8px 8px; }
        .ro-sub-lang-count { display: inline-block; background: var(--gojo-blue); color: white; font-size: 8px; font-weight: 900; padding: 1px 5px; border-radius: 10px; margin-left: 6px; vertical-align: middle; letter-spacing: 0.5px; }
        .ro-sub-status-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; background: rgba(200, 180, 255, 0.2); }
        .ro-sub-status-dot.active  { background: #00e676; box-shadow: 0 0 6px rgba(0, 230, 118, 0.6); }
        .ro-sub-status-dot.loading { background: var(--gojo-blue); animation: subItemPulse 1s infinite; }
        .ro-sub-status-dot.failed  { background: #ff5252; }

        /* Player info */
        .player-info {
            background: linear-gradient(135deg, rgba(13, 8, 24, 0.9), rgba(26, 15, 46, 0.8));
            padding: 20px; border-radius: 12px;
            border: 1px solid rgba(0, 212, 255, 0.15);
            box-shadow: 0 0 30px rgba(0, 212, 255, 0.05), 0 8px 30px rgba(0, 0, 0, 0.4);
            animation: slideUp 0.6s var(--transition-smooth); margin-bottom: 24px;
            backdrop-filter: blur(10px);
            position: relative;
            overflow: hidden;
        }
        .player-info::before {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0; height: 1px;
            background: linear-gradient(90deg, transparent, var(--gojo-blue), var(--hollow-purple), transparent);
        }
        @keyframes slideUp { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }

        .anime-title {
            font-size: 24px; font-weight: 900; margin: 0 0 8px 0;
            background: linear-gradient(135deg, var(--gojo-blue), var(--gojo-white) 50%, var(--hollow-purple));
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            background-clip: text;
            font-family: 'Rajdhani', sans-serif;
            letter-spacing: 2px;
        }
        .episode-label {
            font-size: 12px; font-weight: 700; color: rgba(0, 212, 255, 0.5);
            text-transform: uppercase; letter-spacing: 3px; margin-bottom: 20px;
            font-family: 'Orbitron', sans-serif;
        }
        .server-row { display: flex; align-items: center; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
        .server-row-label { font-size: 9px; font-weight: 900; letter-spacing: 1.5px; text-transform: uppercase; color: var(--text-tertiary); margin-right: 2px; font-family: 'Orbitron', sans-serif; }

        /* Navigation buttons */
        .navigation-controls { display: flex; gap: 12px; }
        .nav-btn {
            flex: 1; padding: 12px; border-radius: 8px; border: none; font-weight: 800;
            font-size: 11px; cursor: pointer; transition: all var(--transition-fast);
            text-transform: uppercase; letter-spacing: 1.5px; font-family: 'Orbitron', sans-serif;
        }
        .prev-btn {
            background: rgba(13, 8, 24, 0.8);
            color: var(--text-secondary);
            border: 1px solid var(--border-color);
            backdrop-filter: blur(10px);
        }
        .prev-btn:hover:not(:disabled) { border-color: var(--gojo-blue); color: var(--gojo-blue); box-shadow: 0 0 15px rgba(0, 212, 255, 0.2); }
        .next-btn {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(124, 58, 237, 0.15));
            color: var(--gojo-blue);
            border: 1px solid rgba(0, 212, 255, 0.4);
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.15);
        }
        .next-btn:hover:not(:disabled) { transform: scale(1.02); box-shadow: 0 0 30px rgba(0, 212, 255, 0.35); color: white; }
        .nav-btn:disabled { opacity: 0.3; cursor: not-allowed; filter: grayscale(1); }

        /* Episodes section */
        .episodes-section {
            background: linear-gradient(135deg, rgba(13, 8, 24, 0.8), rgba(26, 15, 46, 0.6));
            border-radius: 12px; padding: 20px;
            border: 1px solid var(--border-color);
            transition: all var(--transition-smooth);
            backdrop-filter: blur(10px);
        }
        .episodes-section.active { border-color: rgba(0, 212, 255, 0.2); box-shadow: 0 0 20px rgba(0, 212, 255, 0.05); }
        .episodes-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 12px; border-bottom: 1px solid var(--border-color); }
        .episodes-title {
            font-size: 16px; font-weight: 800; color: var(--text-primary);
            text-transform: uppercase; letter-spacing: 2px;
            font-family: 'Orbitron', sans-serif;
        }
        .view-all-btn {
            background: transparent;
            border: 1px solid rgba(0, 212, 255, 0.3); color: var(--gojo-blue);
            padding: 8px 16px; border-radius: 6px; font-size: 10px; font-weight: 700;
            cursor: pointer; transition: all var(--transition-fast); text-transform: uppercase;
            letter-spacing: 1px; font-family: 'Orbitron', sans-serif;
        }
        .view-all-btn:hover { background: rgba(0, 212, 255, 0.1); color: white; box-shadow: 0 0 15px rgba(0, 212, 255, 0.25); }
        .episodes-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(60px, 1fr)); gap: 10px; }
        .episode-item {
            background: rgba(13, 8, 24, 0.8); border: 1px solid var(--border-color);
            padding: 10px; border-radius: 8px; text-align: center;
            cursor: pointer; font-size: 10px; font-weight: 700;
            transition: all var(--transition-fast); color: var(--text-secondary);
            text-transform: uppercase; letter-spacing: 0.5px;
            font-family: 'Orbitron', sans-serif;
        }
        .episode-item:hover {
            background: rgba(0, 212, 255, 0.1);
            color: var(--gojo-blue); border-color: rgba(0, 212, 255, 0.4);
            transform: translateY(-4px);
            box-shadow: 0 4px 15px rgba(0, 212, 255, 0.2);
        }
        .episode-item.current {
            background: linear-gradient(135deg, rgba(0, 212, 255, 0.15), rgba(124, 58, 237, 0.15));
            color: var(--gojo-blue); border-color: rgba(0, 212, 255, 0.5);
            box-shadow: 0 0 15px rgba(0, 212, 255, 0.25);
        }

        /* Episode overlay */
        #episodeOverlay {
            display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(3, 1, 10, 0.95); align-items: center; justify-content: center;
            z-index: 2000; padding: 16px;
            backdrop-filter: blur(20px) saturate(1.5);
            -webkit-backdrop-filter: blur(20px) saturate(1.5);
            animation: fadeIn 0.3s ease-out;
        }
        #episodeOverlay.active { display: flex; }
        .panel-content {
            background: linear-gradient(135deg, rgba(13, 8, 24, 0.98), rgba(26, 15, 46, 0.95));
            border-radius: 16px; max-width: 700px; width: 100%; max-height: 85vh;
            display: flex; flex-direction: column;
            border: 1px solid rgba(0, 212, 255, 0.2);
            box-shadow: 0 0 60px rgba(0, 212, 255, 0.1), 0 25px 60px rgba(0, 0, 0, 0.8);
            animation: slideUp 0.4s var(--transition-smooth);
            position: relative; overflow: hidden;
        }
        .panel-content::before {
            content: '';
            position: absolute; top: 0; left: 0; right: 0; height: 1px;
            background: linear-gradient(90deg, transparent, var(--gojo-blue), var(--hollow-purple), var(--accent-pink), transparent);
        }
        .panel-header {
            padding: 20px; border-bottom: 1px solid var(--border-color);
            display: flex; justify-content: space-between; align-items: center;
        }
        .panel-header h2 {
            margin: 0; font-size: 18px;
            background: linear-gradient(135deg, var(--gojo-blue), var(--gojo-white), var(--hollow-purple));
            -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
            font-weight: 800; font-family: 'Rajdhani', sans-serif; letter-spacing: 2px;
        }
        .close-btn {
            background: rgba(0, 212, 255, 0.1);
            color: var(--gojo-blue); border: 1px solid rgba(0, 212, 255, 0.3);
            width: 36px; height: 36px; border-radius: 50%; cursor: pointer;
            font-weight: 900; font-size: 20px; display: flex; align-items: center; justify-content: center;
            transition: all var(--transition-fast);
        }
        .close-btn:hover { transform: rotate(90deg) scale(1.1); background: rgba(0, 212, 255, 0.2); box-shadow: 0 0 20px rgba(0, 212, 255, 0.4); }
        .panel-body { padding: 20px; overflow-y: auto; flex-grow: 1; }

        /* Skeletons */
        .card-skeleton {
            background: rgba(26, 15, 46, 0.6); border-radius: 8px; overflow: hidden;
            border: 1px solid var(--border-color); display: flex; flex-direction: column;
        }
        .card-skeleton-img {
            width: 100%; aspect-ratio: 2 / 3;
            background: linear-gradient(90deg, rgba(13, 8, 24, 0.8) 0%, rgba(0, 212, 255, 0.06) 40%, rgba(13, 8, 24, 0.8) 80%);
            background-size: 200% 100%; animation: skeletonPulse 1.4s ease-in-out infinite;
        }
        .card-skeleton-line {
            height: 10px; border-radius: 4px; margin: 8px 8px 4px;
            background: linear-gradient(90deg, rgba(13, 8, 24, 0.8) 0%, rgba(0, 212, 255, 0.06) 40%, rgba(13, 8, 24, 0.8) 80%);
            background-size: 200% 100%; animation: skeletonPulse 1.4s ease-in-out infinite;
        }
        .card-skeleton-line.short { width: 55%; height: 8px; }
        @keyframes skeletonPulse { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

        /* Search revealed */
        .card.search-revealed { animation: cardReveal 0.45s cubic-bezier(0.22, 1, 0.36, 1) both; }
        @keyframes cardReveal { from { opacity: 0; transform: translateY(14px) scale(0.97); } to { opacity: 1; transform: translateY(0) scale(1); } }
        #searchResultsSection { animation: none; }
        #searchResultsSection.visible { animation: fadeInUp 0.35s cubic-bezier(0.22, 1, 0.36, 1); }

        /* Search top bar */
        #searchTopBar {
            position: fixed; top: 0; left: 0; height: 2px; width: 0%;
            background: linear-gradient(90deg, var(--gojo-blue), var(--hollow-purple), var(--accent-pink), var(--gojo-blue));
            background-size: 200% 100%; z-index: 9998; opacity: 0;
            transition: opacity 0.2s ease;
            box-shadow: 0 0 10px rgba(0, 212, 255, 0.8);
        }
        #searchTopBar.active {
            opacity: 1;
            animation: searchBarFill 0.9s cubic-bezier(0.4,0,0.2,1) forwards, roTopbarShimmer 1s linear infinite;
        }
        @keyframes searchBarFill { 0% { width: 0%; } 40% { width: 55%; } 80% { width: 85%; } 100% { width: 85%; } }

        /* Episode loading */
        .episode-loading { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 60px 20px; gap: 15px; }
        .episode-spinner {
            width: 50px; height: 50px;
            border: 2px solid rgba(0, 212, 255, 0.1);
            border-top: 2px solid var(--gojo-blue);
            border-right: 2px solid var(--hollow-purple);
            border-radius: 50%;
            animation: spin 1s linear infinite;
            box-shadow: 0 0 20px rgba(0, 212, 255, 0.2);
        }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .episode-loading-text {
            font-size: 12px; font-weight: 700; color: var(--text-secondary);
            letter-spacing: 2px; text-transform: uppercase;
            font-family: 'Orbitron', sans-serif;
            color: rgba(0, 212, 255, 0.6);
        }

        /* ===== GOJO FLOATING PARTICLES ===== */
        .gojo-particle {
            position: fixed;
            border-radius: 50%;
            pointer-events: none;
            z-index: 0;
            animation: floatParticle linear infinite;
        }
        @keyframes floatParticle {
            0%   { transform: translateY(100vh) rotate(0deg); opacity: 0; }
            10%  { opacity: 1; }
            90%  { opacity: 0.5; }
            100% { transform: translateY(-100px) rotate(720deg); opacity: 0; }
        }

        /* Back button */
        .nav-btn.prev-btn[onclick="goHome()"] {
            font-family: 'Orbitron', sans-serif;
            font-size: 10px;
            letter-spacing: 1.5px;
        }
    </style>
</head>
<body>

<!-- ===== GOJO INFINITY CANVAS ===== -->
<canvas id="gojoCanvas"></canvas>

<div id="searchTopBar"></div>

<!-- SPLASH SCREEN -->
<div id="splashScreen">
    <div class="splash-content">
        <div class="splash-logo">
            <img src="https://files.manuscdn.com/user_upload_by_module/session_file/310519663321093674/oWQUOlviOKwSQlpf.png" alt="Gojo">
        </div>
        <div class="splash-text">RO-ANIME</div>
        <div class="splash-sub">Infinity · Unlimited</div>
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
        <input id="searchInput" type="text" placeholder="Search the infinite void..." autocomplete="off">
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
        <button class="nav-btn prev-btn" style="width:auto;margin-bottom:16px;padding:8px 16px;font-size:10px;" onclick="goHome()">&#8592; BACK</button>
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
                            <div id="roSubMenuWrap">
                                <button class="ro-btn" id="roSubBtn" title="Subtitles / CC" style="display:none;">
                                    <svg viewBox="0 0 24 24"><path d="M20 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 14H4V6h16v12zM6 10h2v2H6zm0 4h8v2H6zm10 0h2v2h-2zm-6-4h8v2h-8z"/></svg>
                                </button>
                                <div id="roSubDropdown" class="ro-sub-dropdown"></div>
                            </div>
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
            <div class="server-pill-row" id="serverPillRow" style="display:none;">
                <span class="server-pill-label">Servers</span>
            </div>
            <div class="server-row">
                <span id="streamStatusBadge" class="stream-status"></span>
            </div>
            <div class="sub-dub-toggle">
                <button id="subBtn" class="subdub-btn active">SUB</button>
            </div>
            <div class="navigation-controls">
                <button id="prevBtn" class="nav-btn prev-btn" onclick="changeEpisode(-1)">&#8592; PREVIOUS</button>
                <button id="nextBtn" class="nav-btn next-btn" onclick="changeEpisode(1)">NEXT &#8594;</button>
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
        <div id="recentlyAddedSection">
            <div class="recently-added-header">
                <div class="recently-added-title">Recently Added</div>
                <div class="recently-added-badge">Live Feed</div>
            </div>
            <div id="recentlyAddedGrid"></div>
        </div>

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
            <p style="margin-bottom:16px;color:var(--text-secondary);font-weight:600;font-size:12px;letter-spacing:2px;font-family:'Orbitron',sans-serif;text-transform:uppercase;">Select Episode:</p>
            <div id="episodesContainer" class="episodes-grid"></div>
        </div>
    </div>
</div>

<!-- ===== GOJO CANVAS EFFECT ===== -->
<script>
(function() {
    const canvas = document.getElementById('gojoCanvas');
    const ctx    = canvas.getContext('2d');
    let W, H, mouse = { x: -1000, y: -1000 };
    let particles  = [];
    let hexagons   = [];
    let time       = 0;

    function resize() {
        W = canvas.width  = window.innerWidth;
        H = canvas.height = window.innerHeight;
    }
    resize();
    window.addEventListener('resize', resize);
    window.addEventListener('mousemove', e => { mouse.x = e.clientX; mouse.y = e.clientY; });

    // Gojo-style floating particles
    class InfinityParticle {
        constructor() { this.reset(); }
        reset() {
            this.x    = Math.random() * W;
            this.y    = H + 10;
            this.vx   = (Math.random() - 0.5) * 0.5;
            this.vy   = -(0.3 + Math.random() * 0.8);
            this.size = 0.5 + Math.random() * 1.5;
            this.life = 0; this.maxLife = 200 + Math.random() * 300;
            const r   = Math.random();
            if (r < 0.4)      this.color = `rgba(0, 212, 255,`;
            else if (r < 0.7) this.color = `rgba(168, 85, 247,`;
            else               this.color = `rgba(255, 255, 255,`;
        }
        update() {
            this.x += this.vx; this.y += this.vy;
            this.life++;
            const dist = Math.hypot(this.x - mouse.x, this.y - mouse.y);
            if (dist < 80) {
                const angle = Math.atan2(this.y - mouse.y, this.x - mouse.x);
                this.vx += Math.cos(angle) * 0.04;
                this.vy += Math.sin(angle) * 0.04;
            }
            if (this.life > this.maxLife || this.y < -10) this.reset();
        }
        draw() {
            const a = Math.min(1, this.life / 40) * (1 - this.life / this.maxLife);
            ctx.beginPath();
            ctx.arc(this.x, this.y, this.size, 0, Math.PI * 2);
            ctx.fillStyle = `${this.color}${a})`;
            ctx.fill();
        }
    }

    // Infinity/domain hexagon grid lines
    class HexLine {
        constructor() { this.reset(); }
        reset() {
            this.x1 = Math.random() * W; this.y1 = Math.random() * H;
            const a = Math.random() * Math.PI * 2;
            const l = 30 + Math.random() * 80;
            this.x2 = this.x1 + Math.cos(a) * l;
            this.y2 = this.y1 + Math.sin(a) * l;
            this.life = 0; this.maxLife = 120 + Math.random() * 200;
            this.alpha = 0;
        }
        update() {
            this.life++;
            if (this.life > this.maxLife) this.reset();
            this.alpha = Math.sin((this.life / this.maxLife) * Math.PI) * 0.15;
        }
        draw() {
            ctx.beginPath();
            ctx.moveTo(this.x1, this.y1);
            ctx.lineTo(this.x2, this.y2);
            ctx.strokeStyle = `rgba(0, 212, 255, ${this.alpha})`;
            ctx.lineWidth = 0.5;
            ctx.stroke();
        }
    }

    // Gojo Infinity rings
    function drawInfinityRings() {
        const cx = W * 0.85, cy = H * 0.1;
        for (let r = 50; r <= 200; r += 40) {
            const a = (Math.sin(time * 0.3 + r * 0.02) + 1) * 0.05;
            ctx.beginPath();
            ctx.arc(cx, cy, r + Math.sin(time * 0.5) * 5, 0, Math.PI * 2);
            ctx.strokeStyle = `rgba(0, 212, 255, ${a})`;
            ctx.lineWidth = 0.5;
            ctx.stroke();
        }
    }

    // Hollow Purple glow
    function drawHollowPurpleAura() {
        const cx = W * 0.15, cy = H * 0.9;
        const grad = ctx.createRadialGradient(cx, cy, 0, cx, cy, 120);
        const pulse = (Math.sin(time * 0.6) + 1) / 2;
        grad.addColorStop(0, `rgba(139, 92, 246, ${0.04 + pulse * 0.04})`);
        grad.addColorStop(0.5, `rgba(255, 0, 110, ${0.02 + pulse * 0.02})`);
        grad.addColorStop(1, 'rgba(0,0,0,0)');
        ctx.beginPath();
        ctx.arc(cx, cy, 120, 0, Math.PI * 2);
        ctx.fillStyle = grad;
        ctx.fill();
    }

    // Init
    for (let i = 0; i < 120; i++) {
        const p = new InfinityParticle();
        p.life = Math.random() * p.maxLife;
        p.y    = Math.random() * H;
        particles.push(p);
    }
    for (let i = 0; i < 20; i++) {
        const h = new HexLine();
        h.life = Math.random() * h.maxLife;
        hexagons.push(h);
    }

    function loop() {
        ctx.clearRect(0, 0, W, H);
        time += 0.016;
        drawInfinityRings();
        drawHollowPurpleAura();
        hexagons.forEach(h => { h.update(); h.draw(); });
        particles.forEach(p => { p.update(); p.draw(); });
        requestAnimationFrame(loop);
    }
    loop();
})();
</script>

<!-- ===== MOUSE TILT EFFECT FOR CARDS ===== -->
<script>
document.addEventListener('mousemove', function(e) {
    const cards = document.querySelectorAll('.card:hover');
    cards.forEach(card => {
        const rect  = card.getBoundingClientRect();
        const cx    = rect.left + rect.width  / 2;
        const cy    = rect.top  + rect.height / 2;
        const rx    = (e.clientY - cy) / (rect.height / 2) * 6;
        const ry    = -(e.clientX - cx) / (rect.width  / 2) * 6;
        card.style.transform = `translateY(-16px) scale(1.06) rotateX(${rx}deg) rotateY(${ry}deg)`;
    });
});
</script>

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
                        return;
                    }
                } catch(e) {}
            }
        }
    }, true);

    // =========================================================================
    //  GLOBAL STATE
    // =========================================================================
    const API_BASE        = '';
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
    let v10Cache             = null;
    let v10SrvIdx            = 0;
    let v10QualIdx           = 0;
    let v10StallTimer        = null;
    let v10LastTime          = -1;
    let v10FilteredServers   = [];

    // ── Subtitle State ───────────────────────────────────────────────────────
    let roSubtitleCues       = [];
    let roSubActive          = false;
    let roActiveSubLabel     = null;
    let roEnglishSubFile     = null;

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
        const splashTimeout = setTimeout(() => {
            document.getElementById('splashScreen').classList.add('hidden');
        }, 3000);
        fetchBannersForSplash().then(() => {
            clearTimeout(splashTimeout);
            document.getElementById('splashScreen').classList.add('hidden');
        });
    });

    // =========================================================================
    //  HOME INIT
    // =========================================================================
    async function initHome() {
        const fragment = document.createDocumentFragment();
        fullAnimeList.forEach(anime => fragment.appendChild(createCard(anime, true)));
        document.getElementById('famousGrid').appendChild(fragment);
        initHeroSlider([]);
        startAsyncImageLoading();
        fetchDynamicHomeData();
        fetchRecentlyAdded();
    }

    async function fetchBannersForSplash() {
        try {
            const resp = await fetch(`${API_BASE}/api/v1/banners`);
            if (!resp.ok) return;
            const data = await resp.json();
            const list = Array.isArray(data) ? data : [];
            if (list.length > 0) initHeroSlider(list);
        } catch(e) { console.warn('Banner fetch failed:', e); }
    }

    async function fetchDynamicHomeData() {
        try {
            const resp = await fetch(`${API_BASE}/home/thumbnails`);
            if (!resp.ok) return;
            const data = await resp.json();
            const list = Array.isArray(data) ? data : (data.results || []);
            if (list.length) renderDynamicThumbnails(list);
        } catch(e) { console.error('Error fetching dynamic home data:', e); }
    }

    // =========================================================================
    //  RECENTLY ADDED
    // =========================================================================
    async function fetchRecentlyAdded() {
        const section = document.getElementById('recentlyAddedSection');
        const grid    = document.getElementById('recentlyAddedGrid');
        grid.innerHTML = '';
        const skelFrag = document.createDocumentFragment();
        for (let i = 0; i < 9; i++) {
            const sk = document.createElement('div');
            sk.className = 'card-skeleton';
            sk.innerHTML = `<div class="card-skeleton-img"></div>
                            <div class="card-skeleton-line"></div>
                            <div class="card-skeleton-line short" style="margin-bottom:8px"></div>`;
            skelFrag.appendChild(sk);
        }
        grid.appendChild(skelFrag);
        section.style.display = 'block';
        try {
            const resp = await fetch(`${API_BASE}/home/recently-added`);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const list = await resp.json();
            if (!Array.isArray(list) || list.length === 0) { section.style.display = 'none'; return; }
            renderRecentlyAdded(list);
        } catch(e) {
            console.warn('Recently added fetch failed:', e);
            section.style.display = 'none';
        }
    }

    function renderRecentlyAdded(animeList) {
        const grid    = document.getElementById('recentlyAddedGrid');
        const section = document.getElementById('recentlyAddedSection');
        grid.innerHTML = '';
        const fragment = document.createDocumentFragment();
        animeList.forEach((anime, i) => { fragment.appendChild(createRecentCard(anime, i)); });
        grid.appendChild(fragment);
        section.style.display = 'block';
        startAsyncImageLoading();
    }

    function createRecentCard(anime, idx) {
        const card = document.createElement('div');
        card.className = 'card search-revealed';
        card.style.animationDelay = `${Math.min(idx * 40, 500)}ms`;
        const imgWrapper = document.createElement('div');
        imgWrapper.className = 'card-img-wrapper loading';
        const badge = document.createElement('span');
        badge.className   = 'card-new-badge';
        badge.textContent = 'NEW';
        imgWrapper.appendChild(badge);
        const img = document.createElement('img');
        img.loading = 'lazy';
        img.onload  = () => { imgWrapper.classList.remove('loading'); img.classList.add('loaded'); };
        img.onerror = () => { imgWrapper.classList.remove('loading'); imgWrapper.style.background = 'var(--tertiary-black)'; };
        const imageUrl = anime.poster || anime.image || anime.thumbnail || '';
        if (imageUrl) { img.dataset.src = imageUrl; observeNewImage(img); }
        imgWrapper.appendChild(img);
        const overlay = document.createElement('div');
        overlay.className = 'card-overlay';
        const watchBtn = document.createElement('button');
        watchBtn.className   = 'card-watch-btn';
        watchBtn.textContent = '\u25b6 WATCH NOW';
        watchBtn.addEventListener('click', (e) => { e.stopPropagation(); openRecentAnime(anime); });
        overlay.appendChild(watchBtn);
        imgWrapper.appendChild(overlay);
        const info = document.createElement('div');
        info.className = 'card-info';
        const title = document.createElement('div');
        title.className   = 'card-title';
        title.textContent = anime.title;
        const meta = document.createElement('div');
        meta.className   = 'card-meta';
        meta.textContent = 'Series \u2022 HD';
        info.appendChild(title);
        info.appendChild(meta);
        card.appendChild(imgWrapper);
        card.appendChild(info);
        card.addEventListener('click', () => openRecentAnime(anime));
        return card;
    }

    async function openRecentAnime(anime) {
        const animeObj = { anime_id: anime.anime_id, title: anime.title };
        fetchEpisodes(animeObj);
    }

    function renderDynamicThumbnails(animeList) {
        const grid    = document.getElementById('dynamicGrid');
        const section = document.getElementById('dynamicSection');
        const fragment = document.createDocumentFragment();
        animeList.forEach(anime => fragment.appendChild(createCard(anime, false)));
        grid.appendChild(fragment);
        section.style.display = 'block';
    }

    function initHeroSlider(banners) {
        const container = document.getElementById('heroSliderContainer');
        container.innerHTML = '';
        heroSlides = [];
        if (heroAutoInterval) { clearInterval(heroAutoInterval); heroAutoInterval = null; }
        const slides = (banners && banners.length > 0)
            ? banners.slice(0, 8)
            : fullAnimeList.slice(0, 5).map(a => ({ title: a.title, anime_id: null, img_url: a.image }));
        heroSlideIndex = 0;
        slides.forEach((slide, index) => {
            const el = document.createElement('div');
            el.className = `hero-slide ${index === 0 ? 'active' : ''}`;
            el.style.backgroundImage = `url('${slide.img_url || slide.image || ''}')`;
            const overlay = document.createElement('div');
            overlay.className = 'hero-slide-overlay';
            if (slide.title) {
                const safeTitle = slide.title.replace(/'/g, "\\'");
                const watchAction = slide.anime_id
                    ? `startStreamingFromBanner('${slide.anime_id}', '${safeTitle}')`
                    : `quickOpenEpisodes('${safeTitle}')`;
                overlay.innerHTML = `
                    <h2 class="hero-slide-title">${slide.title}</h2>
                    <span class="hero-slide-ep">FEATURED &bull; HD</span>
                    <button class="hero-watch-btn" onclick="${watchAction}">&#9654; WATCH NOW</button>`;
            }
            el.appendChild(overlay);
            container.appendChild(el);
            heroSlides.push(el);
        });
        if (heroSlides.length > 1) {
            heroAutoInterval = setInterval(() => {
                heroSlideIndex = (heroSlideIndex + 1) % heroSlides.length;
                updateHeroSlide();
            }, 6000);
        }
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
    //  START STREAMING FROM BANNER
    // =========================================================================
    async function startStreamingFromBanner(animeId, title) {
        const overlay   = document.getElementById('episodeOverlay');
        const container = document.getElementById('episodesContainer');
        container.innerHTML = `<div class="episode-loading"><div class="episode-spinner"></div><div class="episode-loading-text">LOADING EPISODES...</div></div>`;
        document.getElementById('overlayTitle').textContent = title;
        overlay.classList.add('active');
        try {
            const resp = await fetch(`${API_BASE}/anime/episode/${animeId}`);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const data = await resp.json();
            const episodes = data.episodes || [];
            if (!episodes.length) throw new Error('No episodes');
            const anime = { anime_id: animeId, title };
            renderEpisodes(anime, episodes);
        } catch(e) {
            container.innerHTML = `<div style="text-align:center;padding:30px;"><p style="color:var(--accent-pink);margin-bottom:16px;">&#9888; Could not load episodes.</p><button onclick="startStreamingFromBanner('${animeId}','${title.replace(/'/g,"\\'")}');" style="padding:8px 20px;background:var(--accent-pink);color:white;border:none;border-radius:6px;cursor:pointer;font-weight:700;">RETRY</button></div>`;
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
        roResetSubtitleState();
        const pillRow = document.getElementById('serverPillRow');
        pillRow.innerHTML = '<span class="server-pill-label">Servers</span>';
        pillRow.style.display = 'none';
    }

    // =========================================================================
    //  SUBTITLE SYSTEM
    // =========================================================================
    function roResetSubtitleState() {
        roSubtitleCues   = [];
        roSubActive      = false;
        roActiveSubLabel = null;
        roEnglishSubFile = null;
        document.getElementById('roSubtitleOverlay').innerHTML = '';
        roRenderSubMenu();
    }

    function parseVTT(text) {
        if (!text) return [];
        text = text.replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        const cues  = [];
        const lines = text.split('\n');
        let i = 0;
        while (i < lines.length) {
            const line = lines[i].trim();
            if (line.includes('-->')) {
                const match = line.match(
                    /(\d{1,2}:\d{2}:\d{2}[.,]\d{3}|\d{1,2}:\d{2}[.,]\d{3})\s+-->\s+(\d{1,2}:\d{2}:\d{2}[.,]\d{3}|\d{1,2}:\d{2}[.,]\d{3})/
                );
                if (match) {
                    const start = vttTimeToSec(match[1]);
                    const end   = vttTimeToSec(match[2]);
                    i++;
                    const textLines = [];
                    while (i < lines.length && lines[i].trim() !== '') {
                        textLines.push(lines[i]);
                        i++;
                    }
                    const cueText = textLines
                        .join('\n')
                        .replace(/<\d{2}:\d{2}:\d{2}\.\d{3}>/g, '')
                        .replace(/<[^>]+>/g, '')
                        .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&nbsp;/g, ' ')
                        .trim();
                    if (cueText) cues.push({ start, end, text: cueText });
                }
            }
            i++;
        }
        return cues;
    }

    function vttTimeToSec(t) {
        t = t.replace(',', '.');
        const parts = t.split(':');
        if (parts.length === 3) return parseFloat(parts[0]) * 3600 + parseFloat(parts[1]) * 60 + parseFloat(parts[2]);
        if (parts.length === 2) return parseFloat(parts[0]) * 60 + parseFloat(parts[1]);
        return 0;
    }

    function roRenderSubMenu() {
        const btn      = document.getElementById('roSubBtn');
        const dropdown = document.getElementById('roSubDropdown');
        if (!btn || !dropdown) return;
        if (!roEnglishSubFile) { btn.style.display = 'none'; dropdown.classList.remove('open'); return; }
        btn.style.display = 'flex';
        btn.innerHTML = `<svg viewBox="0 0 24 24"><path d="M20 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 14H4V6h16v12zM6 10h2v2H6zm0 4h8v2H6zm10 0h2v2h-2zm-6-4h8v2h-8z"/></svg>`;
        btn.onclick = roToggleSubMenu;
        dropdown.innerHTML = '';
        const makeItem = (label, isActive) => {
            const div = document.createElement('div');
            div.className = 'ro-sub-dropdown-item' + (isActive ? ' active-sub' : '');
            const dot = document.createElement('span');
            dot.className = 'ro-sub-status-dot' + (isActive ? ' active' : '');
            if (label === 'Off') dot.style.display = 'none';
            div.appendChild(dot);
            const text = document.createElement('span');
            text.textContent = label;
            div.appendChild(text);
            return div;
        };
        const offItem = makeItem('Off', roActiveSubLabel === null);
        offItem.addEventListener('click', (e) => { e.stopPropagation(); roSwitchSubtitle(null, null); });
        dropdown.appendChild(offItem);
        const engItem = makeItem('English', roActiveSubLabel === 'English');
        engItem.addEventListener('click', (e) => { e.stopPropagation(); roSwitchSubtitle(roEnglishSubFile, 'English'); });
        dropdown.appendChild(engItem);
    }

    function roToggleSubMenu(e) {
        if (e) e.stopPropagation();
        const dropdown = document.getElementById('roSubDropdown');
        if (dropdown) dropdown.classList.toggle('open');
    }

    document.addEventListener('click', (e) => {
        if (!e.target.closest('#roSubMenuWrap')) {
            const dd = document.getElementById('roSubDropdown');
            if (dd) dd.classList.remove('open');
        }
    });

    function roRenderSubtitle(currentTime) {
        const overlay = document.getElementById('roSubtitleOverlay');
        if (!roSubActive || roSubtitleCues.length === 0) { overlay.innerHTML = ''; return; }
        const cue = roSubtitleCues.find(c => currentTime >= c.start && currentTime <= c.end);
        if (cue) {
            overlay.innerHTML = cue.text.split('\n').filter(l => l.trim())
                .map(l => `<div><span class="ro-sub-line">${l}</span></div>`).join('');
        } else {
            overlay.innerHTML = '';
        }
    }

    async function roSwitchSubtitle(fileUrl, label) {
        document.getElementById('roSubDropdown').classList.remove('open');
        if (!fileUrl) {
            roActiveSubLabel = null; roSubtitleCues = []; roSubActive = false;
            document.getElementById('roSubtitleOverlay').innerHTML = '';
            roRenderSubMenu(); return;
        }
        if (label === roActiveSubLabel && roSubActive && roSubtitleCues.length > 0) return;
        roActiveSubLabel = label; roSubtitleCues = []; roSubActive = false;
        document.getElementById('roSubtitleOverlay').innerHTML = '';
        const proxyUrl = `${API_BASE}/proxy?url=${encodeURIComponent(fileUrl)}`;
        let txt = null;
        try {
            const r = await fetch(fileUrl);
            if (r.ok) txt = await r.text();
        } catch(e) {}
        if (!txt) {
            try {
                const r = await fetch(proxyUrl);
                if (r.ok) txt = await r.text();
                else throw new Error(`Proxy ${r.status}`);
            } catch(e) {
                roActiveSubLabel = null; roRenderSubMenu(); return;
            }
        }
        const cues = parseVTT(txt);
        if (cues.length > 0) { roSubtitleCues = cues; roSubActive = true; }
        else { roActiveSubLabel = null; }
        roRenderSubMenu();
    }

    async function roAutoSelectDefaultSubtitle() {
        if (!roEnglishSubFile) return;
        await roSwitchSubtitle(roEnglishSubFile, 'English');
    }

    function v10OnTimeUpdate() {
        roRenderSubtitle(document.getElementById('roVideoEl').currentTime);
    }

    function v10StartWatchdog() {
        clearInterval(v10StallTimer);
        v10StallTimer = setInterval(() => { v10LastTime = document.getElementById('roVideoEl').currentTime; }, 5000);
    }

    // =========================================================================
    //  BANNED CDN CHECK
    // =========================================================================
    function isBannedCDN(url) {
        const l = url.toLowerCase();
        return l.includes('storm') || l.includes('strom') || l.includes('douvid');
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
                v10SrvIdx  = i; v10QualIdx = 0;
                roRenderServerPills();
                roResetSubtitleState();
                if (v10Cache && v10Cache.subtitles && v10Cache.subtitles.length > 0) {
                    const sub =
                        v10Cache.subtitles.find(s => s.file && s.label === 'English') ||
                        v10Cache.subtitles.find(s => s.file && s.default === true)    ||
                        v10Cache.subtitles.find(s => s.file);
                    if (sub) { roEnglishSubFile = sub.file; roRenderSubMenu(); setTimeout(() => roAutoSelectDefaultSubtitle(), 300); }
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
    //  V10 HLS ATTACH
    // =========================================================================
    function v10AttachHLS(resumeTime) {
        resumeTime = resumeTime || 0;
        if (!v10FilteredServers.length || v10SrvIdx >= v10FilteredServers.length) {
            document.getElementById('streamStatusBadge').textContent = '\u26a0 Video not available yet. Try another episode.';
            document.getElementById('streamStatusBadge').className   = 'stream-status error';
            roShowSpinner(false); return;
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
            playUrl = `${API_BASE}/stream.m3u8?src=${encodeURIComponent(rawUrl)}`;
        }

        clearInterval(v10StallTimer);
        if (hlsInstance) { hlsInstance.destroy(); hlsInstance = null; }

        badge.textContent = 'Loading ' + (server.serverLabel || server.serverName) + '...';
        badge.className   = 'stream-status loading';
        roShowSpinner(true);
        roUpdateServerPill(v10SrvIdx, 'loading');

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

        pw.classList.remove('megaplay-mode');
        iframeWrap.innerHTML = ''; iframeWrap.style.display = 'none';
        ['roCtrl','roCenterPlay','roFlashL','roFlashR'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.visibility = '';
        });
        video.style.display = 'block';
        video.removeEventListener('timeupdate', v10OnTimeUpdate);
        video.addEventListener('timeupdate', v10OnTimeUpdate);

        const onReady = () => {
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

        if (typeof Hls !== 'undefined' && Hls.isSupported()) {
            hlsInstance = new Hls({
                enableWorker:            true,
                lowLatencyMode:          false,
                maxBufferLength:         30,
                maxMaxBufferLength:      90,
                backBufferLength:        60,
                startLevel:              -1,
                fragLoadingTimeOut:      20000,
                manifestLoadingTimeOut:  20000,
                fragLoadingMaxRetry:     4,
                manifestLoadingMaxRetry: 4,
            });
            hlsInstance.loadSource(playUrl);
            hlsInstance.attachMedia(video);
            hlsInstance.on(Hls.Events.MANIFEST_PARSED, onReady);
            hlsInstance.on(Hls.Events.ERROR, (_, d) => {
                if (d.fatal || [403, 404].includes(d.response?.code)) {
                    roUpdateServerPill(v10SrvIdx, 'failed');
                    badge.textContent = '\u26a0 Server failed \u2014 please select another server';
                    badge.className   = 'stream-status error';
                    roShowSpinner(false); return;
                }
                if (d.type === Hls.ErrorTypes.NETWORK_ERROR) hlsInstance.startLoad();
                else if (d.type === Hls.ErrorTypes.MEDIA_ERROR) hlsInstance.recoverMediaError();
            });
        } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
            video.src = playUrl;
            video.addEventListener('loadedmetadata', () => { onReady(); }, { once: true });
            video.addEventListener('error', () => {
                roUpdateServerPill(v10SrvIdx, 'failed');
                badge.textContent = '\u26a0 Server failed \u2014 please select another server';
                badge.className   = 'stream-status error';
                roShowSpinner(false);
            }, { once: true });
        } else {
            badge.textContent = '\u26a0 HLS not supported in this browser';
            badge.className   = 'stream-status error';
            roShowSpinner(false);
        }
    }

    // =========================================================================
    //  LOAD STREAM
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
        video.pause(); video.src = ''; video.style.display = 'none';
        iframeWrap.innerHTML = ''; iframeWrap.style.display = 'none';
        pw.classList.remove('megaplay-mode');
        roShowSpinner(true);
        document.getElementById('roCenterPlay').classList.add('visible');
        roResetSubtitleState();

        const cacheKey   = episodeId + '_sub';
        const needsFetch = !v10Cache || v10Cache._cacheKey !== cacheKey;

        if (needsFetch) {
            v10SrvIdx = 0; v10QualIdx = 0;
            try {
                badge.textContent = 'Fetching stream data...';
                const res = await fetch(`${API_BASE}/api/v10/stream/${episodeId}?type=sub`);
                if (!res.ok) throw new Error('HTTP ' + res.status);
                v10Cache           = await res.json();
                v10Cache._cacheKey = cacheKey;
            } catch(err) {
                badge.textContent = '\u26a0 Could not load stream. Please retry.';
                badge.className   = 'stream-status error';
                roShowSpinner(false); return;
            }
        }

        v10FilteredServers = (v10Cache.servers || []).filter(s => !isBannedCDN(s.m3u8Url));

        if (!v10FilteredServers.length) {
            badge.textContent = '\u26a0 Video not available yet. No playable servers.';
            badge.className   = 'stream-status error';
            roShowSpinner(false); return;
        }

        roRenderServerPills();

        roEnglishSubFile = null;
        if (v10Cache.subtitles && v10Cache.subtitles.length > 0) {
            const sub =
                v10Cache.subtitles.find(s => s.file && s.label === 'English') ||
                v10Cache.subtitles.find(s => s.file && s.default === true)    ||
                v10Cache.subtitles.find(s => s.file);
            if (sub) {
                roEnglishSubFile = sub.file;
                roRenderSubMenu();
                setTimeout(() => roAutoSelectDefaultSubtitle(), 300);
            } else { roRenderSubMenu(); }
        } else { roRenderSubMenu(); }

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
        viewAllMode = false; episodesListRendered = false;
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
        dropdown.style.cssText = 'position:absolute;top:100%;left:0;right:0;background:rgba(13,8,24,0.97);backdrop-filter:blur(20px);border:1px solid rgba(0,212,255,0.3);border-top:none;border-radius:0 0 8px 8px;max-height:300px;overflow-y:auto;z-index:1001;display:none;box-shadow:0 8px 24px rgba(0,212,255,0.15);';
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
        } catch(error) {}
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

    const pw = document.getElementById('playerWrapper');
    pw.addEventListener('mousemove',  roShowControls);
    pw.addEventListener('mouseenter', roShowControls);
    pw.addEventListener('mouseleave', () => { clearTimeout(roCtrlTimeout); roHideControlsIfPlaying(); });
    pw.addEventListener('touchstart', () => { roShowControls(); }, { passive: true });
    pw.addEventListener('touchmove',  roShowControls, { passive: true });

    document.getElementById('roPlayBtn').addEventListener('click', roTogglePlay);
    document.getElementById('roCenterPlay').addEventListener('click', (e) => { e.stopPropagation(); roTogglePlay(); });
    document.getElementById('roVideoEl').addEventListener('click',  () => { if (!roIsDragging) roTogglePlay(); });
    document.getElementById('roVideoEl').addEventListener('play',   roSyncIcons);
    document.getElementById('roVideoEl').addEventListener('pause',  roSyncIcons);
    document.getElementById('roVideoEl').addEventListener('ended',  roSyncIcons);

    document.getElementById('roVideoEl').addEventListener('waiting', () => roShowSpinner(true));
    document.getElementById('roVideoEl').addEventListener('canplay', () => roShowSpinner(false));
    document.getElementById('roVideoEl').addEventListener('playing', () => roShowSpinner(false));

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
            document.getElementById('roProgBuf').style.width = (v.buffered.end(v.buffered.length - 1) / v.duration * 100) + '%';
        }
    });

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
    document.getElementById('roProg').addEventListener('touchstart', e => { roIsDragging = true; roSeekFromEvent(e.touches[0]); }, { passive: true });
    document.addEventListener('touchmove', e => { if (roIsDragging) roSeekFromEvent(e.touches[0]); }, { passive: true });
    document.addEventListener('touchend',  () => { roIsDragging = false; });

    document.getElementById('roRwdBtn').addEventListener('click', () => {
        roVideo().currentTime = Math.max(0, roVideo().currentTime - 10);
        roFlashSkip(document.getElementById('roFlashL')); roShowControls();
    });
    document.getElementById('roFwdBtn').addEventListener('click', () => {
        const v = roVideo();
        v.currentTime = Math.min(v.duration || Infinity, v.currentTime + 10);
        roFlashSkip(document.getElementById('roFlashR')); roShowControls();
    });

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

<footer style="text-align:center;padding:20px;color:var(--text-tertiary);font-size:11px;border-top:1px solid rgba(45,31,74,0.5);margin-top:40px;font-family:'Orbitron',sans-serif;letter-spacing:2px;text-transform:uppercase;position:relative;z-index:1;">
    Made by aniket with struggle &nbsp;·&nbsp; <span style="color:rgba(0,212,255,0.5);">∞ infinity</span>
</footer>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# APP LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await http_client.start()
    logger.info("AniStream v10.6 — File1 Streaming Logic — Online")
    yield
    await http_client.stop()
    executor.shutdown()

app = FastAPI(lifespan=lifespan, title="AniStream v10.6")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=512)

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return HTMLResponse(HTML)

@app.get("/api/v10/stream/{episode_id}")
async def route_unified_stream(
    episode_id: str,
    type: str = Query(default="sub", pattern="^(sub|dub)$"),
):
    res = await get_unified_stream(episode_id, type)
    return Response(msgspec.json.encode(res), media_type="application/json")

@app.get("/api/servers")
async def api_servers_compat(episodeId: int, type: str = "sub"):
    try:
        result = await get_unified_stream(str(episodeId), type)
        servers = [
            {"id": s.serverName, "name": s.serverLabel, "m3u8url": s.m3u8Url}
            for s in result.servers
        ]
        return {"servers": servers}
    except HTTPException as e:
        return JSONResponse({"error": e.detail, "servers": []}, status_code=e.status_code)

@app.get("/proxy")
async def route_proxy(request: Request, url: str = "", referer: str = ""):
    if not url:
        raise HTTPException(400, "Missing url param")
    decoded_url = unquote(url)
    resp        = await _proxy_url(decoded_url)
    body        = resp.body.decode("utf-8", errors="replace")
    if is_m3u8(decoded_url, body):
        base_local = str(request.base_url).rstrip("/")
        return Response(
            content    = rewrite_m3u8(body, decoded_url, base_local),
            media_type = "application/vnd.apple.mpegurl",
            headers    = {
                "Access-Control-Allow-Origin": "*",
                "Content-Type":               "application/vnd.apple.mpegurl; charset=utf-8",
            },
        )
    return resp

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

@app.get("/api/v1/banners")
async def route_banners():
    res = await get_banners()
    return Response(msgspec.json.encode(res), media_type="application/json")
BASE = "https://nine.mewcdn.online"
DATE = "3/29/2026%2012:00"

@app.get("/api/servers/{episodeId}/{type}")
async def api_servers(episodeId: int, type: str = "sub"):
    results = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
        url = f"{BASE}/ajax/episode/servers?episodeId={episodeId}&type={type}-{DATE}"
        try:
            resp = await client.get(url)
            data = resp.json()
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=502)

        soup = BeautifulSoup(data.get('html', ''), 'lxml')
        items = soup.find_all('div', class_='item server-item')
        server_ids = [
            (item.get('data-id'), item.get_text(strip=True))
            for item in items
            if item.get('data-id') and item.get('data-type') == type
        ]

        for sid, name in server_ids:
            try:
                src_url = f"{BASE}/ajax/episode/sources?id={sid}&type={type}-{DATE}"
                r = await client.get(src_url)
                link = r.json().get('link', '')
                if not link:
                    continue

                m3u8url = None
                if 'rapid-cloud.co' in link or 'megacloud' in link:
                    embed_id = link.split('/')[-1].split('?')[0]
                    sources_resp = await client.get(
                        f"https://rapid-cloud.co/embed-2/v2/e-1/getSources?id={embed_id}",
                        headers={
                            "Accept": "*/*",
                            "Referer": link,
                            "X-Requested-With": "XMLHttpRequest",
                            "User-Agent": HEADERS["User-Agent"],
                        }
                    )
                    srcs = sources_resp.json().get('sources', [])
                    if srcs:
                        m3u8url = srcs[0].get('file', '')

                if m3u8url:
                    results.append({"id": sid, "name": name, "m3u8url": m3u8url})
            except Exception:
                continue

    return {"servers": results}


@app.get("/stream")
async def stream_m3u8(request: Request, src: str):

    master_url = unquote(src) if src else ""
    if not master_url:
        return Response("No src provided", status_code=400)

    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(None, lambda: fetch(master_url))
    if resp.status_code != 200:
        return Response(content=f"Failed: {resp.status_code}", status_code=502)

    base_local = str(request.base_url).rstrip("/")
    rewritten = rewrite_m3u8(resp.text, master_url, base_local)
    return Response(
        content=rewritten,
        media_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/vnd.apple.mpegurl; charset=utf-8",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ★ NEW: Recently Added route
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/home/recently-added")
async def route_recently_added():
    """
    Returns a list of recently-added anime scraped from the MEW home page.
    Each item: { anime_id, title, poster }
    """
    res = await get_recently_added()
    return Response(msgspec.json.encode(res), media_type="application/json")


@app.get("/health")
async def health():
    return {
        "status":  "ok",
        "version": "10.6",
        "cache":   cache.stats(),
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
