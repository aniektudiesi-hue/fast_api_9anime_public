"""
RO-ANIME Backend v3
"""
from __future__ import annotations
import asyncio, base64, json, logging, os, re, time, sqlite3, hashlib, secrets, uuid, shutil
from dataclasses import asdict, dataclass, field
from html import unescape
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import quote, quote_plus, unquote, urljoin, urlparse

import httpx
from cachetools import TTLCache
from curl_cffi import requests as cffi_requests
from fastapi import FastAPI, HTTPException, Query, Request, Depends, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse

from app.config import settings

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("ro-anime")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
MAL_CLIENT_ID = settings.mal_client_id
MAL_BASE      = settings.mal_base
MAL_HEADERS   = {"X-MAL-CLIENT-ID": MAL_CLIENT_ID}
ANILIST_GQL   = settings.anilist_gql
MEGAPLAY_BASE = settings.megaplay_base
SOURCES_EP    = f"{MEGAPLAY_BASE}/stream/getSources"
VIDWISH_BASE  = settings.vidwish_base
VIDWISH_SOURCES_EP = f"{VIDWISH_BASE}/stream/getSources"
# Edge proxy base for HLS playlists, chunks, captions, and HD media.
CLOUDFLARE_PROXY_BASE = settings.cloudflare_proxy_base
IMPERSONATE   = "chrome131_android"
ANDROID_UA    = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Mobile Safari/537.36"
)
MOON_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_SEASONAL_FIELDS = (
    "id,title,main_picture,alternative_titles,"
    "num_episodes,start_season,mean,status,rank,popularity"
)
_SEARCH_FIELDS = (
    "id,title,main_picture,alternative_titles,"
    "num_episodes,start_season,mean,status"
)

GQL_SPRING2026 = """
query {
  Page(perPage: 50) {
    media(
      season: SPRING
      seasonYear: 2026
      type: ANIME
      sort: POPULARITY_DESC
      isAdult: false
    ) {
      id
      idMal
      title { english romaji }
      bannerImage
      coverImage { extraLarge large }
      episodes
      averageScore
      popularity
      status
      nextAiringEpisode { episode }
    }
  }
}
"""

GQL_ANIME_EPS = """
query($malId: Int) {
  Media(idMal: $malId, type: ANIME) {
    status
    episodes
    nextAiringEpisode { episode }
  }
}
"""

GQL_ANIME_EPS_BATCH = """
query($ids: [Int], $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(idMal_in: $ids, type: ANIME) {
      idMal
      status
      episodes
      nextAiringEpisode { episode }
    }
  }
}
"""

NAV_HEADERS = {
    "User-Agent": ANDROID_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": '"Chromium";v="131", "Google Chrome";v="131", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?1",
    "Sec-Ch-Ua-Platform": '"Android"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": f"{MEGAPLAY_BASE}/",
}
API_HEADERS = {
    "User-Agent": ANDROID_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": MEGAPLAY_BASE,
    "Sec-Ch-Ua": '"Chromium";v="131", "Google Chrome";v="131", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?1",
    "Sec-Ch-Ua-Platform": '"Android"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{MEGAPLAY_BASE}/",
}
VIDWISH_NAV_HEADERS = {
    **NAV_HEADERS,
    "Referer": f"{MEGAPLAY_BASE}/",
    "Sec-Fetch-Dest": "iframe",
    "Sec-Fetch-Site": "cross-site",
}
VIDWISH_API_HEADERS = {
    **API_HEADERS,
    "Origin": VIDWISH_BASE,
    "Referer": f"{VIDWISH_BASE}/",
    "Sec-Fetch-Site": "same-origin",
}
PROXY_HEADERS = {
    "User-Agent": ANDROID_UA,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": MEGAPLAY_BASE,
    "Referer": f"{MEGAPLAY_BASE}/",
}

# Per-upstream policy. Several CDNs reject requests that carry the wrong
# Referer OR the wrong User-Agent. The Moon CDN (sprintcdn / r66nv9ed.com)
# specifically returns 404 to:
#   - the Android-Chrome UA we use globally for megaplay
#   - any request with Referer megaplay.buzz
# Map upstream host -> (referer, desktop_ua_or_none, curl_cffi_impersonate).
#
# Verified empirically against r66nv9ed.com:
#   Android UA + chrome131_android impersonate -> 404
#   Desktop UA + chrome120 impersonate         -> 200  (any Referer including none)
#
# Anything not matched falls back to the megaplay defaults.
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# host substring -> (referer, user_agent, impersonate_profile)
_UPSTREAM_POLICY: tuple[tuple[str, str, str, str], ...] = (
    ("watching.onl",      f"{VIDWISH_BASE}/", ANDROID_UA, IMPERSONATE),
    ("cinewave",          f"{MEGAPLAY_BASE}/", ANDROID_UA, IMPERSONATE),
    ("vidwish.live",      f"{VIDWISH_BASE}/", ANDROID_UA, IMPERSONATE),
    # Moon stack — DESKTOP UA mandatory
    ("r66nv9ed.com",      "https://bysesayeveum.com/", DESKTOP_UA, "chrome120"),
    ("sprintcdn",         "https://bysesayeveum.com/", DESKTOP_UA, "chrome120"),
    ("bysesayeveum.com",  "https://bysesayeveum.com/", DESKTOP_UA, "chrome120"),
    ("398fitus.com",      "https://bysesayeveum.com/", DESKTOP_UA, "chrome120"),
    # HD / workers stack
    ("workers.dev",       "https://9animetv.org.lv/", DESKTOP_UA, "chrome120"),
    ("owocdn.top",        "https://9animetv.org.lv/", DESKTOP_UA, "chrome120"),
    ("gogoanime.me.uk",   "https://9animetv.org.lv/", DESKTOP_UA, "chrome120"),
)


def _proxy_call_for(url: str) -> tuple[dict, str]:
    """Return (headers, impersonate_profile) tuned for *url*.

    The Origin/Referer/UA must match what the upstream's hotlink filter
    expects. Falls back to the megaplay defaults for anything we don't
    recognise.
    """
    host = (urlparse(url).hostname or "").lower()
    for needle, ref, ua, imp in _UPSTREAM_POLICY:
        if needle in host:
            origin = ref.rstrip("/")
            return {
                "User-Agent":      ua,
                "Accept":          "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin":          origin,
                "Referer":         ref,
            }, imp
    return PROXY_HEADERS, IMPERSONATE


# Backward-compat shim — older internal callers still use this name.
def _proxy_headers_for(url: str) -> dict:
    return _proxy_call_for(url)[0]

# ---------------------------------------------------------------------------
# CACHES
# ---------------------------------------------------------------------------
_home_cache    = TTLCache(maxsize=32,  ttl=3600)
_search_cache  = TTLCache(maxsize=400, ttl=600)
_suggest_cache = TTLCache(maxsize=400, ttl=300)
_anime_cache   = TTLCache(maxsize=600, ttl=1800)
_home_lock     = asyncio.Lock()
_toprated_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------
@dataclass
class SubtitleTrack:
    file: str; label: str; kind: str = "captions"; default: bool = False

@dataclass
class TimeRange:
    start: int; end: int

@dataclass
class StreamData:
    m3u8_url: str
    subtitles: list[SubtitleTrack] = field(default_factory=list)
    intro:  Optional[TimeRange] = None
    outro:  Optional[TimeRange] = None
    server_id: Optional[int] = None
    iframe_url: str = ""
    mal_id: str = ""; episode_num: str = ""
    internal_id: str = ""; real_id: str = ""; media_id: str = ""

    def to_dict(self) -> dict:
        return {
            "m3u8_url": self.m3u8_url,
            "subtitles": [asdict(s) for s in self.subtitles],
            "intro":  asdict(self.intro)  if self.intro  else None,
            "outro":  asdict(self.outro)  if self.outro  else None,
            "server_id": self.server_id,
            "iframe_url": self.iframe_url,
            "mal_id": self.mal_id, "episode_num": self.episode_num,
            "internal_id": self.internal_id,
            "real_id": self.real_id, "media_id": self.media_id,
        }

# ---------------------------------------------------------------------------
# EXCEPTIONS
# ---------------------------------------------------------------------------
class MegaPlayError(Exception): pass
class EpisodeNotFound(MegaPlayError): pass
class InvalidRequest(MegaPlayError): pass
class StreamBlocked(MegaPlayError): pass
class PlayerPageError(MegaPlayError): pass

# ---------------------------------------------------------------------------
# MEGAPLAY SCRAPER
# ---------------------------------------------------------------------------
def extract_player_data_attrs(page_html: str) -> dict[str, str]:
    return {
        key.lower(): unescape(value).replace("\\/", "/")
        for key, value in re.findall(
            r'\bdata-([\w-]+)\s*=\s*["\']([^"\']*)["\']',
            page_html or "",
            flags=re.IGNORECASE,
        )
    }


def extract_megaplay_iframe_url(page_html: str, page_url: str) -> str:
    for match in re.finditer(
        r'<iframe\b[^>]*\bsrc\s*=\s*["\']([^"\']+)["\']',
        page_html or "",
        flags=re.IGNORECASE,
    ):
        candidate = unescape(match.group(1)).replace("\\/", "/")
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        iframe_url = urljoin(page_url, candidate)
        parsed = urlparse(iframe_url)
        if (parsed.hostname or "").lower().endswith("vidwish.live") and "/stream/" in parsed.path:
            return iframe_url

    raw = (page_html or "").replace("\\/", "/")
    match = re.search(r'https?://(?:www\.)?vidwish\.live/stream/[^"\'<\s]+', raw, re.IGNORECASE)
    return unescape(match.group(0)) if match else ""


def extract_vidwish_source_id(page_html: str) -> str:
    return extract_player_data_attrs(page_html).get("id", "")


class MegaPlayScraper:
    def __init__(self, timeout=15, max_retries=3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = cffi_requests.Session()
        self._bootstrap()

    def _bootstrap(self):
        try:
            self.session.get(f"{MEGAPLAY_BASE}/",
                headers={**NAV_HEADERS, "Sec-Fetch-Site": "none", "Referer": ""},
                impersonate=IMPERSONATE, timeout=self.timeout)
        except Exception as e:
            logger.warning("bootstrap non-fatal: %s", e)

    def _fetch_player_page(self, mal_id, ep_num, stype):
        url = f"{MEGAPLAY_BASE}/stream/mal/{mal_id}/{ep_num}/{stype}"
        logger.info("player page -> %s", url)
        try:
            r = self.session.get(url, headers=NAV_HEADERS,
                impersonate=IMPERSONATE, timeout=self.timeout)
        except Exception as e:
            raise PlayerPageError(f"fetch failed: {e}")
        if r.status_code == 404:
            raise EpisodeNotFound(f"MAL={mal_id} ep={ep_num} type={stype}")
        if r.status_code != 200:
            raise PlayerPageError(f"HTTP {r.status_code}")
        attrs = extract_player_data_attrs(r.text)
        if "id" in attrs:
            attrs["_source"] = "megaplay"
            attrs["_player_url"] = url
            attrs["_fallback_url"] = url
            return attrs

        vidwish_url = extract_megaplay_iframe_url(r.text, url)
        if not vidwish_url:
            logger.warning("Megaplay data-id missing; using iframe fallback for %s", url)
            return {"_source": "iframe", "_player_url": url, "_fallback_url": url}

        logger.info("vidwish layer -> %s", vidwish_url)
        try:
            vr = self.session.get(vidwish_url,
                headers={**VIDWISH_NAV_HEADERS, "Referer": url},
                impersonate=IMPERSONATE, timeout=self.timeout)
        except Exception as e:
            raise PlayerPageError(f"Vidwish fetch failed: {e}")
        if vr.status_code == 404:
            raise EpisodeNotFound(f"MAL={mal_id} ep={ep_num} type={stype}")
        if vr.status_code != 200:
            raise PlayerPageError(f"Vidwish HTTP {vr.status_code}")

        attrs = extract_player_data_attrs(vr.text)
        if "id" not in attrs:
            logger.warning("Vidwish data-id missing; using iframe fallback for %s", vidwish_url)
            return {"_source": "iframe", "_player_url": vidwish_url, "_fallback_url": url}
        attrs["_source"] = "vidwish"
        attrs["_player_url"] = vidwish_url
        attrs["_fallback_url"] = url
        return attrs

    def _fetch_sources(self, attrs, stype):
        internal_id = attrs.get("id", "")
        if not internal_id:
            raise PlayerPageError("data-id missing")
        is_vidwish = attrs.get("_source") == "vidwish"
        source_url = (
            f"{VIDWISH_SOURCES_EP}?id={quote(internal_id, safe='')}&id={quote(internal_id, safe='')}"
            if is_vidwish else SOURCES_EP
        )
        source_params = None if is_vidwish else {"id": internal_id, "type": stype}
        source_headers = (
            {**VIDWISH_API_HEADERS, "Referer": attrs.get("_player_url") or f"{VIDWISH_BASE}/"}
            if is_vidwish else API_HEADERS
        )
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.session.get(source_url,
                    params=source_params,
                    headers=source_headers, impersonate=IMPERSONATE, timeout=self.timeout)
                if r.status_code == 200:  return r.json()
                if r.status_code == 400:  raise InvalidRequest(r.text[:200])
                if r.status_code == 403:  raise StreamBlocked(r.text[:200])
                if r.status_code == 404:  raise EpisodeNotFound(f"id={internal_id}")
                last_err = MegaPlayError(f"HTTP {r.status_code}")
            except (InvalidRequest, EpisodeNotFound, StreamBlocked):
                raise
            except Exception as e:
                last_err = e
                if attempt < self.max_retries:
                    time.sleep(0.5 * attempt)
        raise MegaPlayError(f"all retries failed: {last_err}")

    def get_stream(self, mal_id, episode_num, stream_type="sub"):
        attrs = self._fetch_player_page(mal_id, episode_num, stream_type)
        fallback_url = attrs.get("_fallback_url") or attrs.get("_player_url") or ""
        try:
            data  = self._fetch_sources(attrs, stream_type)
            return self._parse(data, mal_id, episode_num,
                               internal_id=attrs.get("id", ""),
                               real_id=attrs.get("realid", ""),
                               media_id=attrs.get("mediaid", ""),
                               fallback_url=fallback_url)
        except EpisodeNotFound:
            if not fallback_url:
                raise
            logger.warning("Source endpoint returned 404, using iframe fallback")
        except (InvalidRequest, StreamBlocked, PlayerPageError, MegaPlayError) as e:
            logger.warning("HLS extraction failed, returning Megaplay iframe fallback: %s", e)
        return StreamData(m3u8_url="", iframe_url=fallback_url,
            mal_id=mal_id, episode_num=episode_num,
            internal_id=attrs.get("id", ""), real_id=attrs.get("realid", ""),
            media_id=attrs.get("mediaid", ""))

    @staticmethod
    def _parse(data, mal_id, ep_num, internal_id, real_id, media_id, fallback_url=""):
        sources  = data.get("sources") or {}
        m3u8_url = sources.get("file") if isinstance(sources, dict) else None
        if not m3u8_url and isinstance(data.get("sources"), list) and data["sources"]:
            m3u8_url = data["sources"][0].get("file")
        if not m3u8_url:
            raise MegaPlayError(f"no m3u8: {str(data)[:200]}")
        subs = [
            SubtitleTrack(file=t["file"], label=t.get("label", "Unknown"),
                kind=t.get("kind", "captions"), default=bool(t.get("default", False)))
            for t in (data.get("tracks") or []) if t.get("file")
        ]
        def _tr(d):
            if not d: return None
            try: return TimeRange(int(d["start"]), int(d["end"]))
            except: return None
        return StreamData(m3u8_url=m3u8_url, subtitles=subs,
            intro=_tr(data.get("intro")), outro=_tr(data.get("outro")),
            server_id=data.get("server"), mal_id=mal_id, episode_num=ep_num,
            iframe_url=fallback_url,
            internal_id=internal_id, real_id=real_id, media_id=media_id)

    def close(self):
        try: self.session.close()
        except: pass

# ---------------------------------------------------------------------------
# M3U8 REWRITER
# ---------------------------------------------------------------------------
def _proxy_url(backend, cdn_url, route):
    return f"{backend}{route}?src={quote(cdn_url, safe='')}"

def _rewrite_attr_uri(line, base, backend, route):
    def _rep(m):
        return f'URI="{_proxy_url(backend, urljoin(base, m.group(1)), route)}"'
    return re.sub(r'URI="([^"]+)"', _rep, line)

def rewrite_m3u8(text, original_url, backend):
    base, out, next_is_pl = original_url, [], False
    for raw in text.splitlines():
        line = raw.rstrip("\r")
        if line.startswith("#EXT-X-KEY") or line.startswith("#EXT-X-MAP"):
            out.append(_rewrite_attr_uri(line, base, backend, "/proxy/chunk")); continue
        if line.startswith("#EXT-X-STREAM-INF") or line.startswith("#EXT-X-I-FRAME-STREAM-INF"):
            if "#EXT-X-I-FRAME-STREAM-INF" in line:
                line = _rewrite_attr_uri(line, base, backend, "/proxy/m3u8")
            out.append(line); next_is_pl = True; continue
        if line.startswith("#") or not line.strip():
            out.append(line); continue
        absolute = urljoin(base, line)
        route = "/proxy/m3u8" if next_is_pl else "/proxy/chunk"
        out.append(_proxy_url(backend, absolute, route)); next_is_pl = False
    return "\n".join(out) + "\n"

def _detect_mime(url, body):
    if body[:1] == b"\x47": return "video/mp2t"
    if body[4:8] in (b"ftyp", b"styp", b"moof", b"moov"): return "video/mp4"
    lower = url.lower().split("?")[0]
    if lower.endswith(".ts"):  return "video/mp2t"
    if lower.endswith(".m4s"): return "video/iso.segment"
    if lower.endswith(".mp4"): return "video/mp4"
    return "application/octet-stream"

def _is_valid_url(url):
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.hostname)
    except: return False

def _response_content_length(resp) -> int | None:
    try:
        val = resp.headers.get("content-length")
        return int(val) if val is not None else None
    except Exception:
        return None

def _parse_single_range(range_header: str, total_size: int | None = None) -> tuple[int, int | None] | None:
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", (range_header or "").strip())
    if not m:
        return None
    start_s, end_s = m.groups()
    if not start_s and not end_s:
        return None
    if start_s:
        start = int(start_s)
        end = int(end_s) if end_s else None
    else:
        if total_size is None:
            return None
        suffix_len = int(end_s)
        if suffix_len <= 0:
            return None
        start = max(total_size - suffix_len, 0)
        end = total_size - 1
    if end is not None and end < start:
        return None
    return start, end

def _range_response_headers(*, content_len: int, total_size: int | None,
                            requested: tuple[int, int | None] | None,
                            upstream_content_range: str | None = None) -> tuple[int, dict]:
    headers = {
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_len),
    }
    if upstream_content_range:
        headers["Content-Range"] = upstream_content_range
        return 206, headers
    if requested:
        start, end = requested
        if end is None:
            end = start + max(content_len - 1, 0)
        total = "*" if total_size is None else str(total_size)
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        return 206, headers
    return 200, headers

def _apply_local_range(body: bytes, range_header: str | None,
                       cache_control: str) -> tuple[bytes, int, dict]:
    headers = {
        "Cache-Control": cache_control,
        "Access-Control-Allow-Origin": "*",
        "Accept-Ranges": "bytes",
    }
    total_size = len(body)
    if not range_header:
        headers["Content-Length"] = str(total_size)
        return body, 200, headers
    parsed = _parse_single_range(range_header, total_size)
    if not parsed:
        raise HTTPException(416, "Invalid Range")
    start, end = parsed
    if start >= total_size:
        raise HTTPException(416, "Range not satisfiable")
    if end is None or end >= total_size:
        end = total_size - 1
    sliced = body[start:end + 1]
    headers["Content-Length"] = str(len(sliced))
    headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
    return sliced, 206, headers

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _fmt_card(node, *, ep_override: int = 0) -> dict:
    alt      = node.get("alternative_titles") or {}
    en_title = (alt.get("en") or "").strip()
    title    = en_title if en_title else node.get("title", "Unknown")
    pic      = node.get("main_picture") or {}
    img      = pic.get("large") or pic.get("medium") or ""
    num_eps  = ep_override or node.get("num_episodes") or 0
    return {
        "anime_id":     str(node.get("id", "")),
        "title":        title,
        "title_jp":     node.get("title", ""),
        "title_en":     en_title,
        "poster":       img,
        "image":        img,
        "thumbnail":    img,
        "num_episodes": num_eps,
        "score":        node.get("mean") or 0,
        "status":       node.get("status", ""),
    }

# Persistent HTTP client — reused for all MAL + AniList calls (no per-request handshake)
_http = httpx.AsyncClient(
    timeout=httpx.Timeout(connect=5.0, read=8.0, write=5.0, pool=5.0),
    limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
    follow_redirects=True,
)

async def _mal_get(path, **params):
    r = await _http.get(f"{MAL_BASE}{path}", headers=MAL_HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def _aired_count_from_anilist_media(media: dict) -> int:
    """AniList aired-count rule: next airing episode minus one."""
    next_ep = ((media or {}).get("nextAiringEpisode") or {}).get("episode")
    if next_ep:
        try:
            return max(int(next_ep) - 1, 0)
        except (TypeError, ValueError):
            return 0

    total = (media or {}).get("episodes") or 0
    if (media or {}).get("status") == "FINISHED" and total:
        try:
            return max(int(total), 0)
        except (TypeError, ValueError):
            return 0
    return 0

# ---------------------------------------------------------------------------
# ANILIST SEASONAL
# ---------------------------------------------------------------------------
async def _anilist_spring2026() -> list[dict]:
    """AniList spring 2026 — landscape banners + correct episode data."""
    ck = "anilist_spring2026"
    if ck in _home_cache:
        return _home_cache[ck]
    async with _home_lock:
        if ck in _home_cache:
            return _home_cache[ck]
        try:
            r = await _http.post(ANILIST_GQL,
                json={"query": GQL_SPRING2026},
                headers={"Content-Type": "application/json"})
            r.raise_for_status()
            media = r.json().get("data", {}).get("Page", {}).get("media", [])
            result = [m for m in media if m.get("idMal")]
            _home_cache[ck] = result
            logger.info("AniList spring2026: %d titles cached", len(result))
            return result
        except Exception as e:
            logger.warning("AniList spring2026 failed: %s", e)
            # Do NOT cache empty — let the next request retry
            return []

async def _anilist_ep_map() -> dict[int, int]:
    """Returns {mal_id: episode_count} using AniList data."""
    ck = "anilist_ep_map"
    if ck in _home_cache:
        return _home_cache[ck]
    media = await _anilist_spring2026()
    ep_map: dict[int, int] = {}
    for m in media:
        mid = m.get("idMal")
        if not mid:
            continue
        
        ep_map[int(mid)] = _aired_count_from_anilist_media(m)
    _home_cache[ck] = ep_map
    return ep_map

async def _anilist_episodes_for(mal_id: int) -> tuple[int, str]:
    """Resolve aired-episode count for *mal_id*.

    Returns (count, source). source is 'anilist', 'mal', or 'unknown'.
    Only successful (count > 0) results are cached, so a transient AniList
    outage no longer poisons the cache for 30 minutes with a fake 1-episode
    list.
    """
    ck = f"al_eps_{mal_id}"
    cached = _anime_cache.get(ck)
    if cached and cached[0] > 0:
        return cached

    count  = 0
    source = "unknown"

    # 1. AniList — best for currently-airing shows (nextAiringEpisode tracks
    #    the live aired count). Status FINISHED/RELEASING uses total episodes.
    try:
        r = await _http.post(
            ANILIST_GQL,
            json={"query": GQL_ANIME_EPS, "variables": {"malId": mal_id}},
            headers={"Content-Type": "application/json"},
            timeout=httpx.Timeout(connect=8.0, read=16.0, write=8.0, pool=8.0),
        )
        r.raise_for_status()
        data = r.json().get("data", {}).get("Media") or {}
        count = _aired_count_from_anilist_media(data)
        if count > 0:
            source = "anilist"
    except Exception as e:
        logger.warning("AniList eps mal_id=%s failed: %s", mal_id, e)

    # 2. MAL fallback — reliable for finished anime, fast (we already hold a
    #    persistent client to api.myanimelist.net). Skipped if AniList already
    #    answered.
    if count <= 0:
        try:
            mal = await _mal_get(f"/anime/{mal_id}", fields="num_episodes,status")
            num_eps    = mal.get("num_episodes") or 0
            mal_status = mal.get("status") or ""
            if num_eps > 0:
                count = num_eps
                source = "mal"
            elif mal_status == "currently_airing":
                # Airing show with unknown total — the only safe fallback is
                # 'unknown'. Don't fake it.
                pass
        except Exception as e:
            logger.warning("MAL eps mal_id=%s failed: %s", mal_id, e)

    if count > 0:
        # Long TTL when we know it's done (it never changes); short TTL when
        # we got the count from a live nextAiringEpisode (it shifts weekly).
        _anime_cache[ck] = (count, source)

    return count, source

async def _anilist_episode_map_for_ids(mal_ids: list[int]) -> dict[int, int]:
    """Resolve aired episode counts for many MAL ids in one AniList call."""
    ids = sorted({int(mid) for mid in mal_ids if mid})
    result: dict[int, int] = {}
    pending: list[int] = []

    for mid in ids:
        cached = _anime_cache.get(f"al_eps_{mid}")
        if cached and cached[0] > 0:
            result[mid] = cached[0]
        else:
            pending.append(mid)

    for i in range(0, len(pending), 50):
        chunk = pending[i:i + 50]
        if not chunk:
            continue
        try:
            r = await _http.post(
                ANILIST_GQL,
                json={
                    "query": GQL_ANIME_EPS_BATCH,
                    "variables": {
                        "ids": chunk,
                        "page": 1,
                        "perPage": len(chunk),
                    },
                },
                headers={"Content-Type": "application/json"},
                timeout=httpx.Timeout(connect=8.0, read=16.0, write=8.0, pool=8.0),
            )
            r.raise_for_status()
            media = r.json().get("data", {}).get("Page", {}).get("media", []) or []
            for item in media:
                try:
                    mid = int(item.get("idMal") or 0)
                except (TypeError, ValueError):
                    continue
                count = _aired_count_from_anilist_media(item)
                if mid and count > 0:
                    result[mid] = count
                    _anime_cache[f"al_eps_{mid}"] = (count, "anilist")
        except Exception as e:
            logger.warning("AniList batch eps failed for %d ids: %s", len(chunk), e)
            for mid in chunk:
                count, source = await _anilist_episodes_for(mid)
                if count > 0 and source == "anilist":
                    result[mid] = count

    return result

async def _episode_overrides_for_cards(nodes: list[dict]) -> dict[int, int]:
    """Use AniList live aired counts for MAL cards that need current counts."""
    candidates: list[int] = []
    for n in nodes:
        try:
            mid = int(n.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if not mid:
            continue
        status = n.get("status") or ""
        mal_count = n.get("num_episodes") or 0
        if status in ("currently_airing", "not_yet_aired") or not mal_count:
            candidates.append(mid)

    return await _anilist_episode_map_for_ids(candidates)

# ---------------------------------------------------------------------------
# MAL SEASONAL
# ---------------------------------------------------------------------------
async def _mal_seasonal() -> list[dict]:
    ck = "mal_spring2026"
    if ck in _home_cache:
        return _home_cache[ck]
    async with _home_lock:
        if ck in _home_cache:
            return _home_cache[ck]
        data = await _mal_get("/anime/season/2026/spring",
                              limit=100, fields=_SEASONAL_FIELDS, sort="anime_score")
        results = data.get("data", [])
        _home_cache[ck] = results
        logger.info("MAL spring2026: %d titles", len(results))
        return results

# ---------------------------------------------------------------------------
# INLINE FRONTEND  (read fresh on every request so edits are instant)
# ---------------------------------------------------------------------------
_HTML_PATH = Path(__file__).parent / "index.html"

# ---------------------------------------------------------------------------
# APP
# ---------------------------------------------------------------------------
app     = FastAPI(title="RO-ANIME v3")
scraper = MegaPlayScraper()

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=False,
                   allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
                   allow_headers=["*"])

# ---------------------------------------------------------------------------
# DATABASE  (PostgreSQL on Render via DATABASE_URL, SQLite locally)
# ---------------------------------------------------------------------------
_DATABASE_URL = settings.database_url
if _DATABASE_URL.startswith("postgres://"):
    _DATABASE_URL = "postgresql://" + _DATABASE_URL[len("postgres://"):]
IS_PG = _DATABASE_URL.startswith("postgresql")

_LEGACY_DB_PATH = Path(__file__).parent / "ro_anime_users.db"
_SQLITE_PATH_ENV = settings.sqlite_path
_SQLITE_DIR_ENV = settings.sqlite_dir
_RENDER_DISK_DIR = settings.render_disk_dir

if _SQLITE_PATH_ENV:
    _DB_PATH = Path(_SQLITE_PATH_ENV)
elif _SQLITE_DIR_ENV:
    _DB_PATH = Path(_SQLITE_DIR_ENV) / "ro_anime_users.db"
elif _RENDER_DISK_DIR.exists():
    _DB_PATH = _RENDER_DISK_DIR / "ro_anime_users.db"
else:
    _DB_PATH = _LEGACY_DB_PATH

if not IS_PG:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _DB_PATH != _LEGACY_DB_PATH and not _DB_PATH.exists() and _LEGACY_DB_PATH.exists():
        shutil.copy2(_LEGACY_DB_PATH, _DB_PATH)

if IS_PG:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg import errors as pg_errors


class _Conn:
    """Thin wrapper that exposes a SQLite-like API on top of psycopg or sqlite3."""
    def __init__(self):
        if IS_PG:
            self._c = psycopg.connect(_DATABASE_URL, row_factory=dict_row, autocommit=False)
        else:
            self._c = sqlite3.connect(_DB_PATH)
            self._c.row_factory = sqlite3.Row

    @staticmethod
    def _adapt(sql: str) -> str:
        if not IS_PG:
            return sql
        # SQLite → Postgres translations
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        sql = sql.replace("strftime('%s','now')", "EXTRACT(EPOCH FROM NOW())::BIGINT")
        # parameter placeholders
        sql = sql.replace("?", "%s")
        return sql

    def execute(self, sql: str, params=()):
        sql = self._adapt(sql)
        if IS_PG:
            cur = self._c.cursor()
            cur.execute(sql, params)
            return cur
        return self._c.execute(sql, params)

    def executescript(self, sql: str):
        if IS_PG:
            cur = self._c.cursor()
            cur.execute(self._adapt(sql))
        else:
            self._c.executescript(sql)

    def commit(self): self._c.commit()
    def close(self):  self._c.close()


def _db() -> _Conn:
    return _Conn()


_INTEGRITY_ERRORS = (sqlite3.IntegrityError,)
if IS_PG:
    _INTEGRITY_ERRORS = (sqlite3.IntegrityError, pg_errors.UniqueViolation, pg_errors.IntegrityError)


def _db_init() -> None:
    conn = _db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        username      TEXT    UNIQUE NOT NULL,
        password_hash TEXT    NOT NULL,
        token         TEXT    UNIQUE NOT NULL,
        created_at    INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS watch_history (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id      INTEGER NOT NULL REFERENCES users(id),
        mal_id       TEXT    NOT NULL,
        title        TEXT    NOT NULL,
        image_url    TEXT    DEFAULT '',
        episode      INTEGER NOT NULL,
        playback_pos REAL    DEFAULT 0,
        watched_at   INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(user_id, mal_id, episode)
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_hist_user ON watch_history(user_id, watched_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_hist_user_mal ON watch_history(user_id, mal_id);")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS watchlist (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id   INTEGER NOT NULL REFERENCES users(id),
        mal_id    TEXT    NOT NULL,
        title     TEXT    NOT NULL,
        image_url TEXT    DEFAULT '',
        episodes  INTEGER DEFAULT 0,
        added_at  INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(user_id, mal_id)
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_watchlist_user ON watchlist(user_id, added_at DESC);")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS downloads (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id       INTEGER NOT NULL REFERENCES users(id),
        mal_id        TEXT    NOT NULL,
        title         TEXT    NOT NULL,
        episode       INTEGER NOT NULL,
        image_url     TEXT    DEFAULT '',
        idb_key       TEXT    NOT NULL,
        size_bytes    INTEGER DEFAULT 0,
        has_subs      INTEGER DEFAULT 0,
        downloaded_at INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(user_id, mal_id, episode)
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_downloads_user ON downloads(user_id, downloaded_at DESC);")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS login_events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id       INTEGER,
        username      TEXT    DEFAULT '',
        event_type    TEXT    NOT NULL,
        success       INTEGER DEFAULT 0,
        ip_address    TEXT    DEFAULT '',
        user_agent    TEXT    DEFAULT '',
        device        TEXT    DEFAULT '',
        country       TEXT    DEFAULT '',
        region        TEXT    DEFAULT '',
        city          TEXT    DEFAULT '',
        timezone      TEXT    DEFAULT '',
        language      TEXT    DEFAULT '',
        created_at    INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_login_events_time ON login_events(created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_login_events_user ON login_events(username, created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_login_events_user_id ON login_events(user_id, created_at DESC);")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS visitor_events (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        visitor_key   TEXT    NOT NULL,
        user_id       INTEGER,
        username      TEXT    DEFAULT '',
        path          TEXT    DEFAULT '/',
        referrer      TEXT    DEFAULT '',
        ip_address    TEXT    DEFAULT '',
        user_agent    TEXT    DEFAULT '',
        device        TEXT    DEFAULT '',
        country       TEXT    DEFAULT '',
        region        TEXT    DEFAULT '',
        city          TEXT    DEFAULT '',
        timezone      TEXT    DEFAULT '',
        language      TEXT    DEFAULT '',
        screen        TEXT    DEFAULT '',
        created_at    INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_visitor_events_time ON visitor_events(created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_visitor_events_key ON visitor_events(visitor_key, created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_visitor_events_user ON visitor_events(user_id, created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_visitor_events_username ON visitor_events(username, created_at DESC);")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS chat_messages (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        room          TEXT    NOT NULL,
        user_id       INTEGER NOT NULL REFERENCES users(id),
        username      TEXT    NOT NULL,
        message       TEXT    NOT NULL,
        kind          TEXT    DEFAULT 'text',
        meta          TEXT    DEFAULT '{}',
        created_at    INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_chat_room_time ON chat_messages(room, created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_chat_user_time ON chat_messages(user_id, created_at DESC);")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS user_follows (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        follower_id    INTEGER NOT NULL REFERENCES users(id),
        following_id   INTEGER NOT NULL REFERENCES users(id),
        created_at     INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(follower_id, following_id)
    );
    """)
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_user_follows_follower ON user_follows(follower_id, created_at DESC);")
    conn.executescript("CREATE INDEX IF NOT EXISTS idx_user_follows_following ON user_follows(following_id, created_at DESC);")
    conn.commit()
    conn.close()


def _db_migrate() -> None:
    conn = _db()
    if IS_PG:
        for table, columns in {
            "login_events": {
                "country": "TEXT DEFAULT ''",
                "region": "TEXT DEFAULT ''",
                "city": "TEXT DEFAULT ''",
                "timezone": "TEXT DEFAULT ''",
                "language": "TEXT DEFAULT ''",
            },
            "visitor_events": {
                "country": "TEXT DEFAULT ''",
                "region": "TEXT DEFAULT ''",
                "city": "TEXT DEFAULT ''",
                "timezone": "TEXT DEFAULT ''",
                "language": "TEXT DEFAULT ''",
                "screen": "TEXT DEFAULT ''",
            },
        }.items():
            for name, definition in columns.items():
                conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {definition}")
        conn.commit()
        conn.close()
        return

    def ensure_column(table: str, name: str, definition: str) -> None:
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if name not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    ensure_column("watch_history", "playback_pos", "REAL DEFAULT 0")
    for table, columns in {
        "login_events": {
            "country": "TEXT DEFAULT ''",
            "region": "TEXT DEFAULT ''",
            "city": "TEXT DEFAULT ''",
            "timezone": "TEXT DEFAULT ''",
            "language": "TEXT DEFAULT ''",
        },
        "visitor_events": {
            "country": "TEXT DEFAULT ''",
            "region": "TEXT DEFAULT ''",
            "city": "TEXT DEFAULT ''",
            "timezone": "TEXT DEFAULT ''",
            "language": "TEXT DEFAULT ''",
            "screen": "TEXT DEFAULT ''",
        },
    }.items():
        for name, definition in columns.items():
            ensure_column(table, name, definition)
    conn.commit()
    conn.close()


logger.info("DB backend: %s", "PostgreSQL" if IS_PG else "SQLite")
_db_init()
_db_migrate()

# ---------------------------------------------------------------------------
# AUTH HELPERS
# ---------------------------------------------------------------------------
def _hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def _get_current_user(request: Request) -> dict:
    auth  = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(401, "Not authenticated")
    conn = _db()
    row  = conn.execute("SELECT id, username FROM users WHERE token=?", (token,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Invalid or expired token")
    return {"id": row["id"], "username": row["username"]}


_ADMIN_USERNAMES = {"kali"}
_ADMIN_ACCESS_KEY = os.getenv("ADMIN_ACCESS_KEY", "").strip()


def _row_dict(row) -> dict:
    if not row:
        return {}
    if isinstance(row, dict):
        return row
    return dict(row)


def _get_admin_user(request: Request) -> dict:
    user = _get_current_user(request)
    if user["username"].strip().lower() not in _ADMIN_USERNAMES:
        raise HTTPException(403, "Admin access required")
    if _ADMIN_ACCESS_KEY:
        supplied = request.headers.get("x-admin-key", "").strip()
        if not secrets.compare_digest(supplied, _ADMIN_ACCESS_KEY):
            raise HTTPException(403, "Admin key required")
    return user


def _client_ip(request: Request) -> str:
    for header in ("cf-connecting-ip", "x-real-ip", "x-forwarded-for"):
        value = request.headers.get(header, "")
        if value:
            return value.split(",")[0].strip()[:96]
    return (request.client.host if request.client else "")[:96]


def _first_header(request: Request, *names: str) -> str:
    for name in names:
        value = request.headers.get(name, "")
        if value:
            return unquote(value.split(",")[0].strip())[:160]
    return ""


_TZ_COUNTRY_HINTS = {
    "Asia/Calcutta": "IN",
    "Asia/Kolkata": "IN",
    "Asia/Tokyo": "JP",
    "Asia/Seoul": "KR",
    "Asia/Shanghai": "CN",
    "Asia/Singapore": "SG",
    "Asia/Dubai": "AE",
    "Europe/London": "GB",
    "Europe/Paris": "FR",
    "Europe/Berlin": "DE",
    "America/New_York": "US",
    "America/Chicago": "US",
    "America/Denver": "US",
    "America/Los_Angeles": "US",
    "America/Toronto": "CA",
    "America/Vancouver": "CA",
    "America/Sao_Paulo": "BR",
    "Australia/Sydney": "AU",
    "Australia/Melbourne": "AU",
    "Africa/Johannesburg": "ZA",
}


def _request_location(request: Request, payload: dict | None = None) -> dict:
    payload = payload or {}
    language = str(payload.get("language") or request.headers.get("accept-language", "")).split(",")[0].strip()[:80]
    timezone = str(payload.get("timezone") or _first_header(request, "x-vercel-ip-timezone", "cf-timezone"))[:120]
    country = _first_header(request, "cf-ipcountry", "x-vercel-ip-country", "x-appengine-country") or _TZ_COUNTRY_HINTS.get(timezone, "")
    city = _first_header(request, "x-vercel-ip-city", "x-appengine-city", "cf-ipcity")
    if not city and "/" in timezone:
        city = timezone.rsplit("/", 1)[-1].replace("_", " ")[:160]
    return {
        "country": country,
        "region": _first_header(request, "x-vercel-ip-country-region", "x-appengine-region", "cf-region"),
        "city": city,
        "timezone": timezone,
        "language": language,
        "screen": str(payload.get("screen") or "")[:80],
    }


def _location_label(row: dict) -> str:
    parts = [row.get("city"), row.get("region"), row.get("country")]
    label = ", ".join(str(part) for part in parts if part)
    return label or str(row.get("timezone") or row.get("language") or "Unknown")


def _device_label(user_agent: str) -> str:
    ua = (user_agent or "").lower()
    if "edg/" in ua or "edga/" in ua:
        browser = "Edge"
    elif "chrome/" in ua or "crios/" in ua:
        browser = "Chrome"
    elif "firefox/" in ua:
        browser = "Firefox"
    elif "safari/" in ua:
        browser = "Safari"
    else:
        browser = "Browser"

    if "android" in ua:
        os_name = "Android"
    elif "iphone" in ua or "ipad" in ua:
        os_name = "iOS"
    elif "windows" in ua:
        os_name = "Windows"
    elif "mac os" in ua or "macintosh" in ua:
        os_name = "macOS"
    elif "linux" in ua:
        os_name = "Linux"
    else:
        os_name = "Unknown"

    kind = "Mobile" if any(token in ua for token in ("mobile", "android", "iphone")) else "Desktop"
    return f"{kind} / {os_name} / {browser}"


def _optional_user_from_request(request: Request) -> dict | None:
    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip()
    if not token:
        return None
    conn = _db()
    row = conn.execute("SELECT id, username FROM users WHERE token=?", (token,)).fetchone()
    conn.close()
    return _row_dict(row) if row else None


def _record_login_event(username: str, event_type: str, success: bool, request: Request, user_id: int | None = None) -> None:
    ip = _client_ip(request)
    ua = request.headers.get("user-agent", "")[:1000]
    loc = _request_location(request)
    conn = _db()
    conn.execute(
        """INSERT INTO login_events
           (user_id, username, event_type, success, ip_address, user_agent, device, country, region, city, timezone, language)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            user_id,
            username[:255],
            event_type[:40],
            1 if success else 0,
            ip,
            ua,
            _device_label(ua),
            loc["country"],
            loc["region"],
            loc["city"],
            loc["timezone"],
            loc["language"],
        ),
    )
    conn.commit()
    conn.close()


def _record_visit_event(request: Request, path: str, referrer: str = "", user: dict | None = None, payload: dict | None = None) -> dict:
    ip = _client_ip(request)
    ua = request.headers.get("user-agent", "")[:1000]
    loc = _request_location(request, payload)
    visitor_key = hashlib.sha256(f"{ip}|{ua}".encode("utf-8")).hexdigest()[:32]
    conn = _db()
    conn.execute(
        """INSERT INTO visitor_events
           (visitor_key, user_id, username, path, referrer, ip_address, user_agent, device, country, region, city, timezone, language, screen)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            visitor_key,
            user.get("id") if user else None,
            (user.get("username", "") if user else "")[:255],
            (path or "/")[:500],
            (referrer or "")[:500],
            ip,
            ua,
            _device_label(ua),
            loc["country"],
            loc["region"],
            loc["city"],
            loc["timezone"],
            loc["language"],
            loc["screen"],
        ),
    )
    conn.commit()
    conn.close()
    return {"visitor_key": visitor_key, "ip_address": ip, "device": _device_label(ua), **loc}

# ---------------------------------------------------------------------------
# AUTH ENDPOINTS
# ---------------------------------------------------------------------------
@app.post("/auth/register")
async def auth_register(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or username).strip()
    if len(username) < 3:
        _record_login_event(username, "register", False, request)
        raise HTTPException(400, "Username must be at least 3 characters")
    token = secrets.token_hex(32)
    try:
        conn = _db()
        conn.execute(
            "INSERT INTO users (username, password_hash, token) VALUES (?,?,?)",
            (username, _hash_pw(password), token)
        )
        row = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
        conn.commit()
        conn.close()
    except _INTEGRITY_ERRORS:
        _record_login_event(username, "register", False, request)
        raise HTTPException(409, "Username already taken")
    _record_login_event(username, "register", True, request, _row_dict(row).get("id"))
    return {"token": token, "username": username}

@app.post("/auth/login")
async def auth_login(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if not password:
        _record_login_event(username, "login", False, request)
        raise HTTPException(400, "Password required. Use recover if this is an old username.")
    conn = _db()
    row  = conn.execute(
        "SELECT id, token FROM users WHERE username=? AND password_hash=?",
        (username, _hash_pw(password))
    ).fetchone()
    conn.close()
    if not row:
        _record_login_event(username, "login", False, request)
        raise HTTPException(401, "Invalid username or password")
    _record_login_event(username, "login", True, request, row["id"])
    return {"token": row["token"], "username": username}


@app.post("/auth/recover")
async def auth_recover(request: Request):
    """Recover an existing username without deleting any data.

    This is intentionally separate from normal login so future accounts can use
    strict password auth while older users can still regain their preserved
    history/watchlist and then set a password immediately.
    """
    data = await request.json()
    username = (data.get("username") or "").strip()
    new_password = (data.get("new_password") or data.get("password") or "").strip()
    if len(username) < 3:
        _record_login_event(username, "recover", False, request)
        raise HTTPException(400, "Username must be at least 3 characters")
    conn = _db()
    row = conn.execute("SELECT id, token FROM users WHERE username=?", (username,)).fetchone()
    if not row:
        conn.close()
        _record_login_event(username, "recover", False, request)
        raise HTTPException(404, "Username not found")
    if len(new_password) >= 4:
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (_hash_pw(new_password), row["id"]))
        conn.commit()
    conn.close()
    _record_login_event(username, "recover", True, request, row["id"])
    return {
        "token": row["token"],
        "username": username,
        "recovered": True,
        "password_set": len(new_password) >= 4,
    }

@app.get("/auth/me")
async def auth_me(request: Request):
    return _get_current_user(request)


# ---------------------------------------------------------------------------
# REAL-TIME CHAT
# ---------------------------------------------------------------------------
_CHAT_ROOMS: dict[str, set[WebSocket]] = {}
_CHAT_USERS: dict[WebSocket, dict] = {}
_CHAT_LOCK = asyncio.Lock()


def _normalize_room(room: str) -> str:
    room = re.sub(r"[^a-zA-Z0-9:_\\- ]+", "", (room or "global").strip())[:80]
    return room or "global"


def _chat_row(row) -> dict:
    item = _row_dict(row)
    try:
        item["meta"] = json.loads(item.get("meta") or "{}")
    except Exception:
        item["meta"] = {}
    return item


async def _chat_broadcast(room: str, payload: dict) -> None:
    dead: list[WebSocket] = []
    async with _CHAT_LOCK:
        sockets = list(_CHAT_ROOMS.get(room, set()))
    for socket in sockets:
        try:
            await socket.send_json(payload)
        except Exception:
            dead.append(socket)
    if dead:
        async with _CHAT_LOCK:
            for socket in dead:
                _CHAT_ROOMS.get(room, set()).discard(socket)
                _CHAT_USERS.pop(socket, None)


async def _chat_online_payload(room: str) -> dict:
    async with _CHAT_LOCK:
        users = [_CHAT_USERS[socket] for socket in _CHAT_ROOMS.get(room, set()) if socket in _CHAT_USERS]
    unique = {}
    for user in users:
        unique[user["id"]] = user
    return {"type": "online", "room": room, "users": list(unique.values()), "count": len(unique)}


def _save_chat_message(room: str, user: dict, message: str, kind: str = "text", meta: dict | None = None) -> dict:
    message = str(message or "").strip()
    if not message:
        raise ValueError("Message required")
    if len(message) > 800:
        message = message[:800]
    kind = re.sub(r"[^a-z_\\-]", "", str(kind or "text").lower())[:32] or "text"
    meta_text = json.dumps(meta or {}, separators=(",", ":"))[:2000]
    conn = _db()
    conn.execute(
        """INSERT INTO chat_messages (room, user_id, username, message, kind, meta)
           VALUES (?,?,?,?,?,?)""",
        (room, user["id"], user["username"], message, kind, meta_text),
    )
    row = conn.execute(
        """SELECT id, room, user_id, username, message, kind, meta, created_at
           FROM chat_messages WHERE room=? AND user_id=? ORDER BY id DESC LIMIT 1""",
        (room, user["id"]),
    ).fetchone()
    conn.commit()
    conn.close()
    return _chat_row(row)


@app.get("/chat/rooms")
async def chat_rooms(request: Request):
    _get_current_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT room, COUNT(*) AS messages, MAX(created_at) AS last_message_at
           FROM chat_messages GROUP BY room ORDER BY last_message_at DESC LIMIT 40"""
    ).fetchall()
    conn.close()
    defaults = [
        {"room": "global", "messages": 0, "last_message_at": None},
        {"room": "anime", "messages": 0, "last_message_at": None},
        {"room": "recommendations", "messages": 0, "last_message_at": None},
        {"room": "spoilers", "messages": 0, "last_message_at": None},
    ]
    seen = set()
    items = []
    for item in defaults + [_row_dict(row) for row in rows]:
        if item["room"] in seen:
            continue
        seen.add(item["room"])
        items.append(item)
    return {"items": items}


@app.get("/chat/online")
async def chat_online(request: Request, seconds: int = Query(240, ge=30, le=1800)):
    _get_current_user(request)
    since = int(time.time()) - seconds
    conn = _db()
    rows = conn.execute(
        """SELECT user_id AS id, username, MAX(created_at) AS last_seen_at,
                  MAX(path) AS path, MAX(device) AS device, MAX(country) AS country,
                  MAX(region) AS region, MAX(city) AS city
           FROM visitor_events
           WHERE user_id IS NOT NULL AND username<>'' AND created_at>=?
           GROUP BY user_id, username
           ORDER BY last_seen_at DESC
           LIMIT 200""",
        (since,),
    ).fetchall()
    conn.close()

    by_id = {str(row["id"]): _row_dict(row) for row in rows}
    for users in _CHAT_USERS.values():
        by_id[str(users["id"])] = {
            **by_id.get(str(users["id"]), {}),
            "id": users["id"],
            "username": users["username"],
            "last_seen_at": int(time.time()),
            "source": "chat",
        }
    return {"items": list(by_id.values()), "count": len(by_id)}


@app.get("/chat/messages/{room}")
async def chat_messages(room: str, request: Request, limit: int = Query(60, ge=1, le=200)):
    _get_current_user(request)
    room = _normalize_room(room)
    conn = _db()
    rows = conn.execute(
        """SELECT id, room, user_id, username, message, kind, meta, created_at
           FROM chat_messages WHERE room=? ORDER BY created_at DESC, id DESC LIMIT ?""",
        (room, limit),
    ).fetchall()
    conn.close()
    return {"items": list(reversed([_chat_row(row) for row in rows]))}


@app.post("/chat/messages/{room}")
async def chat_message_add(room: str, request: Request):
    user = _get_current_user(request)
    room = _normalize_room(room)
    data = await request.json()
    item = _save_chat_message(room, user, data.get("message", ""), data.get("kind", "text"), data.get("meta") or {})
    await _chat_broadcast(room, {"type": "message", "room": room, "message": item})
    return {"ok": True, "message": item}


@app.get("/chat/users/search")
async def chat_user_search(request: Request, q: str = Query("", min_length=1), limit: int = Query(20, ge=1, le=50)):
    current = _get_current_user(request)
    query = f"%{q.strip()}%"
    conn = _db()
    rows = conn.execute(
        """SELECT u.id, u.username, u.created_at,
                  (SELECT MAX(created_at) FROM visitor_events v WHERE v.user_id=u.id) AS last_seen_at,
                  EXISTS(SELECT 1 FROM user_follows f WHERE f.follower_id=? AND f.following_id=u.id) AS following
           FROM users u
           WHERE u.username LIKE ? AND u.id<>?
           ORDER BY u.username ASC LIMIT ?""",
        (current["id"], query, current["id"], limit),
    ).fetchall()
    conn.close()
    return {"items": [_row_dict(row) for row in rows]}


@app.post("/chat/users/{user_id}/follow")
async def chat_follow_user(user_id: int, request: Request):
    current = _get_current_user(request)
    if current["id"] == user_id:
        raise HTTPException(400, "You cannot follow yourself")
    conn = _db()
    exists = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
    if not exists:
        conn.close()
        raise HTTPException(404, "User not found")
    conn.execute(
        "INSERT INTO user_follows (follower_id, following_id) VALUES (?,?) ON CONFLICT(follower_id, following_id) DO NOTHING",
        (current["id"], user_id),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/chat/users/{user_id}/follow")
async def chat_unfollow_user(user_id: int, request: Request):
    current = _get_current_user(request)
    conn = _db()
    conn.execute("DELETE FROM user_follows WHERE follower_id=? AND following_id=?", (current["id"], user_id))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/chat/following")
async def chat_following(request: Request):
    current = _get_current_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT u.id, u.username,
                  (SELECT MAX(created_at) FROM visitor_events v WHERE v.user_id=u.id) AS last_seen_at
           FROM user_follows f
           JOIN users u ON u.id=f.following_id
           WHERE f.follower_id=?
           ORDER BY f.created_at DESC LIMIT 100""",
        (current["id"],),
    ).fetchall()
    conn.close()
    return {"items": [_row_dict(row) for row in rows]}


@app.websocket("/ws/chat/{room}")
async def chat_ws(websocket: WebSocket, room: str):
    room = _normalize_room(room)
    token = websocket.query_params.get("token", "").strip()
    user = _user_from_token(token)
    if not user:
        await websocket.close(code=4401)
        return

    await websocket.accept()
    async with _CHAT_LOCK:
        _CHAT_ROOMS.setdefault(room, set()).add(websocket)
        _CHAT_USERS[websocket] = {"id": user["id"], "username": user["username"]}
    await _chat_broadcast(room, await _chat_online_payload(room))
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            if not isinstance(data, dict):
                continue
            if data.get("type") == "ping":
                await websocket.send_json({"type": "pong", "room": room})
                continue
            item = _save_chat_message(room, user, data.get("message", ""), data.get("kind", "text"), data.get("meta") or {})
            await _chat_broadcast(room, {"type": "message", "room": room, "message": item})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning("Chat websocket error: %s", e)
    finally:
        async with _CHAT_LOCK:
            _CHAT_ROOMS.get(room, set()).discard(websocket)
            _CHAT_USERS.pop(websocket, None)
        await _chat_broadcast(room, await _chat_online_payload(room))


# ---------------------------------------------------------------------------
# VISITOR ANALYTICS + ADMIN
# ---------------------------------------------------------------------------
@app.post("/analytics/visit")
async def analytics_visit(request: Request):
    try:
        data = await request.json()
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}
    path = str(data.get("path") or request.headers.get("referer") or "/")
    referrer = str(data.get("referrer") or request.headers.get("referer") or "")
    user = _optional_user_from_request(request)
    saved = _record_visit_event(request, path, referrer, user, data)
    return {"ok": True, "visitor_key": saved["visitor_key"]}


@app.get("/admin/overview")
async def admin_overview(request: Request):
    admin = _get_admin_user(request)
    since_24h = int(time.time()) - 86400
    conn = _db()
    total_users = _row_dict(conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()).get("c", 0)
    total_visits = _row_dict(conn.execute("SELECT COUNT(*) AS c FROM visitor_events").fetchone()).get("c", 0)
    unique_visitors = _row_dict(conn.execute("SELECT COUNT(DISTINCT visitor_key) AS c FROM visitor_events").fetchone()).get("c", 0)
    visits_24h = _row_dict(conn.execute("SELECT COUNT(*) AS c FROM visitor_events WHERE created_at>=?", (since_24h,)).fetchone()).get("c", 0)
    unique_24h = _row_dict(conn.execute("SELECT COUNT(DISTINCT visitor_key) AS c FROM visitor_events WHERE created_at>=?", (since_24h,)).fetchone()).get("c", 0)
    login_success = _row_dict(conn.execute("SELECT COUNT(*) AS c FROM login_events WHERE success=1").fetchone()).get("c", 0)
    login_failed = _row_dict(conn.execute("SELECT COUNT(*) AS c FROM login_events WHERE success=0").fetchone()).get("c", 0)
    known_locations = _row_dict(conn.execute(
        "SELECT COUNT(DISTINCT country || '|' || region || '|' || city || '|' || timezone) AS c FROM visitor_events WHERE country<>'' OR region<>'' OR city<>'' OR timezone<>''"
    ).fetchone()).get("c", 0)
    top_paths = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT path, COUNT(*) AS visits, COUNT(DISTINCT visitor_key) AS unique_visitors
               FROM visitor_events GROUP BY path ORDER BY visits DESC LIMIT 10"""
        ).fetchall()
    ]
    top_locations = [
        {**_row_dict(row), "label": _location_label(_row_dict(row))}
        for row in conn.execute(
            """SELECT country, region, city, timezone, COUNT(*) AS visits, COUNT(DISTINCT visitor_key) AS unique_visitors
               FROM visitor_events
               GROUP BY country, region, city, timezone
               ORDER BY visits DESC LIMIT 10"""
        ).fetchall()
    ]
    top_devices = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT device, COUNT(*) AS visits, COUNT(DISTINCT visitor_key) AS unique_visitors
               FROM visitor_events GROUP BY device ORDER BY visits DESC LIMIT 10"""
        ).fetchall()
    ]
    conn.close()
    return {
        "admin": admin["username"],
        "total_users": total_users,
        "total_visits": total_visits,
        "unique_visitors": unique_visitors,
        "visits_24h": visits_24h,
        "unique_24h": unique_24h,
        "login_success": login_success,
        "login_failed": login_failed,
        "known_locations": known_locations,
        "top_paths": top_paths,
        "top_locations": top_locations,
        "top_devices": top_devices,
    }


@app.get("/admin/users")
async def admin_users(request: Request, limit: int = Query(200, ge=1, le=1000)):
    _get_admin_user(request)
    conn = _db()
    rows = conn.execute(
        """WITH
             hist AS (
               SELECT user_id, COUNT(*) AS history_count, MAX(watched_at) AS last_watched_at
               FROM watch_history GROUP BY user_id
             ),
             watch AS (
               SELECT user_id, COUNT(*) AS watchlist_count, MAX(added_at) AS last_watchlist_at
               FROM watchlist GROUP BY user_id
             ),
             down AS (
               SELECT user_id, COUNT(*) AS downloads_count
               FROM downloads GROUP BY user_id
             ),
             login AS (
               SELECT user_id, MAX(created_at) AS last_login_at
               FROM login_events WHERE success=1 GROUP BY user_id
             ),
             visit_counts AS (
               SELECT user_id, MAX(created_at) AS last_seen_at
               FROM visitor_events GROUP BY user_id
             ),
             last_visit AS (
               SELECT user_id, ip_address, device, country, region, city, timezone
               FROM (
                 SELECT user_id, ip_address, device, country, region, city, timezone,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
                 FROM visitor_events
                 WHERE user_id IS NOT NULL
               ) ranked
               WHERE rn=1
             )
           SELECT
             u.id, u.username, u.created_at,
             COALESCE(hist.history_count, 0) AS history_count,
             COALESCE(watch.watchlist_count, 0) AS watchlist_count,
             COALESCE(down.downloads_count, 0) AS downloads_count,
             hist.last_watched_at,
             watch.last_watchlist_at,
             login.last_login_at,
             visit_counts.last_seen_at,
             last_visit.ip_address AS last_ip,
             last_visit.device AS last_device,
             last_visit.country,
             last_visit.region,
             last_visit.city,
             last_visit.timezone
           FROM users u
           LEFT JOIN hist ON hist.user_id=u.id
           LEFT JOIN watch ON watch.user_id=u.id
           LEFT JOIN down ON down.user_id=u.id
           LEFT JOIN login ON login.user_id=u.id
           LEFT JOIN visit_counts ON visit_counts.user_id=u.id
           LEFT JOIN last_visit ON last_visit.user_id=u.id
           ORDER BY COALESCE(visit_counts.last_seen_at, login.last_login_at, u.created_at) DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return {"items": [_row_dict(row) for row in rows]}


@app.get("/admin/users/{user_id}/activity")
async def admin_user_activity(user_id: int, request: Request, limit: int = Query(300, ge=1, le=2000)):
    _get_admin_user(request)
    conn = _db()
    user = _row_dict(conn.execute(
        """SELECT
              u.id, u.username, u.created_at,
              (SELECT COUNT(*) FROM watch_history h WHERE h.user_id=u.id) AS history_count,
              (SELECT COUNT(*) FROM watchlist w WHERE w.user_id=u.id) AS watchlist_count,
              (SELECT COUNT(*) FROM downloads d WHERE d.user_id=u.id) AS downloads_count,
              (SELECT MAX(created_at) FROM visitor_events v WHERE v.user_id=u.id) AS last_seen_at,
              (SELECT MAX(created_at) FROM login_events l WHERE l.user_id=u.id AND l.success=1) AS last_login_at
           FROM users u WHERE u.id=?""",
        (user_id,),
    ).fetchone())
    if not user:
        conn.close()
        raise HTTPException(404, "User not found")

    totals = _row_dict(conn.execute(
        """SELECT
             (SELECT COUNT(*) FROM watch_history WHERE user_id=?) AS history,
             (SELECT COUNT(*) FROM watchlist WHERE user_id=?) AS watchlist,
             (SELECT COUNT(*) FROM downloads WHERE user_id=?) AS downloads,
             (SELECT COUNT(*) FROM login_events WHERE user_id=? OR username=?) AS logins,
             (SELECT COUNT(*) FROM visitor_events WHERE user_id=? OR username=?) AS visits
        """,
        (user_id, user_id, user_id, user_id, user["username"], user_id, user["username"]),
    ).fetchone())

    history = [_row_dict(row) for row in conn.execute(
        """SELECT mal_id, title, image_url, episode, playback_pos, watched_at
           FROM watch_history WHERE user_id=? ORDER BY watched_at DESC LIMIT ?""",
        (user_id, limit),
    ).fetchall()]
    watchlist = [_row_dict(row) for row in conn.execute(
        """SELECT mal_id, title, image_url, episodes, added_at
           FROM watchlist WHERE user_id=? ORDER BY added_at DESC LIMIT ?""",
        (user_id, limit),
    ).fetchall()]
    downloads = [_row_dict(row) for row in conn.execute(
        """SELECT mal_id, title, episode, image_url, size_bytes, has_subs, downloaded_at
           FROM downloads WHERE user_id=? ORDER BY downloaded_at DESC LIMIT ?""",
        (user_id, limit),
    ).fetchall()]
    logins = [_row_dict(row) for row in conn.execute(
        """SELECT id, event_type, success, ip_address, device, country, region, city, timezone, language, created_at
           FROM login_events WHERE user_id=? OR username=?
           ORDER BY created_at DESC LIMIT ?""",
        (user_id, user["username"], limit),
    ).fetchall()]
    visits = [_row_dict(row) for row in conn.execute(
        """SELECT id, path, referrer, ip_address, device, country, region, city, timezone, language, screen, created_at
           FROM visitor_events WHERE user_id=? OR username=?
           ORDER BY created_at DESC LIMIT ?""",
        (user_id, user["username"], limit),
    ).fetchall()]
    conn.close()
    return {
        "user": user,
        "history": history,
        "watchlist": watchlist,
        "downloads": downloads,
        "logins": logins,
        "visits": visits,
        "totals": totals,
    }


@app.get("/admin/logins")
async def admin_logins(request: Request, limit: int = Query(200, ge=1, le=1000)):
    _get_admin_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT id, user_id, username, event_type, success, ip_address, device, country, region, city, timezone, language, user_agent, created_at
           FROM login_events ORDER BY created_at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return {"items": [_row_dict(row) for row in rows]}


@app.get("/admin/visits")
async def admin_visits(request: Request, limit: int = Query(300, ge=1, le=2000)):
    _get_admin_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT id, visitor_key, user_id, username, path, referrer, ip_address, device, country, region, city, timezone, language, screen, user_agent, created_at
           FROM visitor_events ORDER BY created_at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return {"items": [_row_dict(row) for row in rows]}


@app.get("/admin/search-visibility")
async def admin_search_visibility(request: Request):
    _get_admin_user(request)
    return {
        "note": "Google ranking data should be verified through Google Search Console. These links help you inspect live visibility without scraping Google.",
        "links": [
            {"label": "Google Search Console", "url": "https://search.google.com/search-console"},
            {"label": "Indexed pages", "url": "https://www.google.com/search?q=site%3Aanimetvplus.xyz"},
            {"label": "Brand keyword", "url": "https://www.google.com/search?q=animetvplus"},
            {"label": "Free anime keyword", "url": "https://www.google.com/search?q=animetvplus+free+anime"},
            {"label": "Hindi anime keyword", "url": "https://www.google.com/search?q=animetvplus+hindi+anime"},
        ],
    }

# ---------------------------------------------------------------------------
# HISTORY
# ---------------------------------------------------------------------------
def _normalize_history_payload(d: dict) -> dict:
    mal_id = str(d.get("mal_id") or d.get("anime_id") or "").strip()
    if not mal_id:
        raise ValueError("mal_id is required")
    title = str(d.get("title") or f"Anime {mal_id}").strip() or f"Anime {mal_id}"
    image_url = str(d.get("image_url") or d.get("poster") or d.get("thumbnail") or "").strip()
    episode = int(float(d.get("episode") or d.get("episode_num") or 1))
    playback_pos = float(d.get("playback_pos") or d.get("progress") or d.get("timestamp") or 0)
    return {
        "mal_id": mal_id,
        "title": title,
        "image_url": image_url,
        "episode": max(1, episode),
        "playback_pos": max(0.0, playback_pos),
    }


def _save_history_for_user(user_id: int, d: dict) -> dict:
    payload = _normalize_history_payload(d)
    conn = _db()
    try:
        # Keep one visible history row per anime; the newest episode/resume point wins.
        conn.execute(
            "DELETE FROM watch_history WHERE user_id=? AND mal_id=?",
            (user_id, payload["mal_id"])
        )
        conn.execute(
            """INSERT INTO watch_history (user_id, mal_id, title, image_url, episode, playback_pos, watched_at)
               VALUES (?,?,?,?,?,?,strftime('%s','now'))
            """,
            (user_id, payload["mal_id"], payload["title"], payload["image_url"],
             payload["episode"], payload["playback_pos"])
        )
        conn.commit()
        return payload
    finally:
        conn.close()


@app.post("/user/history")
async def history_add(request: Request):
    user = _get_current_user(request)
    try:
        saved = _save_history_for_user(user["id"], await request.json())
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, **saved}

@app.get("/user/history")
async def history_get(request: Request):
    user = _get_current_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT mal_id, title, image_url, episode, playback_pos, watched_at
           FROM watch_history WHERE user_id=?
           ORDER BY watched_at DESC LIMIT 500""",
        (user["id"],)
    ).fetchall()
    conn.close()
    out = []
    seen = set()
    for r in rows:
        item = dict(r)
        mal_id = item.get("mal_id")
        if mal_id in seen:
            continue
        seen.add(mal_id)
        out.append(item)
        if len(out) >= 200:
            break
    return out

@app.delete("/user/history")
async def history_clear(request: Request):
    user = _get_current_user(request)
    conn = _db()
    conn.execute("DELETE FROM watch_history WHERE user_id=?", (user["id"],))
    conn.commit(); conn.close()
    return {"ok": True}


@app.websocket("/ws/user/history")
async def history_ws(websocket: WebSocket):
    """Receive live watch progress updates for synced resume history."""
    token = websocket.query_params.get("token", "").strip()
    user = _user_from_token(token)
    if not user:
        await websocket.close(code=4401)
        return

    await websocket.accept()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("history payload must be an object")
                saved = _save_history_for_user(user["id"], data)
                await websocket.send_json({"ok": True, **saved})
            except Exception as e:
                await websocket.send_json({"ok": False, "error": str(e)})
    except WebSocketDisconnect:
        return
    except Exception as e:
        logger.warning("History websocket error: %s", e)
        try:
            await websocket.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# WATCHLIST
# ---------------------------------------------------------------------------
@app.post("/user/watchlist")
async def watchlist_add(request: Request):
    user = _get_current_user(request)
    d    = await request.json()
    conn = _db()
    conn.execute(
        """INSERT INTO watchlist (user_id, mal_id, title, image_url, episodes)
           VALUES (?,?,?,?,?)
           ON CONFLICT(user_id, mal_id) DO UPDATE SET
               title     = EXCLUDED.title,
               image_url = EXCLUDED.image_url,
               episodes  = EXCLUDED.episodes""",
        (user["id"], d["mal_id"], d["title"], d.get("image_url",""), int(d.get("episodes",0)))
    )
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/user/watchlist/{mal_id}")
async def watchlist_remove(mal_id: str, request: Request):
    user = _get_current_user(request)
    conn = _db()
    conn.execute("DELETE FROM watchlist WHERE user_id=? AND mal_id=?", (user["id"], mal_id))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/user/watchlist")
async def watchlist_get(request: Request):
    user = _get_current_user(request)
    conn = _db()
    rows = conn.execute(
        "SELECT mal_id, title, image_url, episodes, added_at FROM watchlist WHERE user_id=? ORDER BY added_at DESC",
        (user["id"],)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------------------------------------------------------------------------
# DOWNLOADS METADATA  (actual video bytes live in browser IndexedDB)
# ---------------------------------------------------------------------------
@app.post("/user/downloads")
async def downloads_save(request: Request):
    user = _get_current_user(request)
    d    = await request.json()
    conn = _db()
    conn.execute(
        """INSERT INTO downloads
           (user_id, mal_id, title, episode, image_url, idb_key, size_bytes, has_subs)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id, mal_id, episode) DO UPDATE SET
               title      = EXCLUDED.title,
               image_url  = EXCLUDED.image_url,
               idb_key    = EXCLUDED.idb_key,
               size_bytes = EXCLUDED.size_bytes,
               has_subs   = EXCLUDED.has_subs""",
        (user["id"], d["mal_id"], d["title"], int(d["episode"]),
         d.get("image_url",""), d["idb_key"], int(d.get("size_bytes",0)), int(d.get("has_subs",0)))
    )
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/user/downloads/{mal_id}/{episode}")
async def downloads_delete(mal_id: str, episode: int, request: Request):
    user = _get_current_user(request)
    conn = _db()
    conn.execute(
        "DELETE FROM downloads WHERE user_id=? AND mal_id=? AND episode=?",
        (user["id"], mal_id, episode)
    )
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/user/downloads")
async def downloads_get(request: Request):
    user = _get_current_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT mal_id, title, episode, image_url, idb_key, size_bytes, has_subs, downloaded_at
           FROM downloads WHERE user_id=? ORDER BY downloaded_at DESC""",
        (user["id"],)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def _backend_base(req: Request) -> str:
    scheme = req.headers.get("x-forwarded-proto") or req.url.scheme
    host   = req.headers.get("x-forwarded-host")  or req.url.netloc
    return f"{scheme}://{host}"

def _stream_proxy_base(req: Request) -> str:
    return CLOUDFLARE_PROXY_BASE or _backend_base(req)

def _requires_backend_proxy(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(needle in host for needle in (
        "r66nv9ed.com",
        "sprintcdn",
        "bysesayeveum.com",
        "398fitus.com",
    ))

def _playlist_proxy_base(req: Request, upstream_url: str) -> str:
    # Moon's CDN rejects Cloudflare Worker fetches but accepts our curl_cffi
    # backend proxy with the desktop Chrome impersonation profile.
    if _requires_backend_proxy(upstream_url):
        return _backend_base(req)
    return _stream_proxy_base(req)

# --- root -------------------------------------------------------------------
@app.get("/")
async def root():
    return HTMLResponse(content=_HTML_PATH.read_text(encoding="utf-8"), media_type="text/html")

def _asset_video_path(filename: str) -> Path:
    video_path = Path(__file__).parent / "assets" / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail="Video asset not found")
    return video_path

def _cosmic_void_animation_path() -> Path:
    return _asset_video_path("cosmic_void_animation.mp4")

def _opening_transition_path() -> Path:
    return _asset_video_path("opening_transition.mp4")

@app.head("/assets/cosmic_void_animation.mp4")
async def cosmic_void_animation_head():
    video_path = _cosmic_void_animation_path()
    return Response(
        status_code=200,
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=86400",
            "Content-Length": str(video_path.stat().st_size),
        },
    )

@app.get("/assets/cosmic_void_animation.mp4")
async def cosmic_void_animation():
    video_path = _cosmic_void_animation_path()
    return FileResponse(
        video_path,
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=86400",
        },
    )

@app.head("/assets/opening_transition.mp4")
async def opening_transition_head():
    video_path = _opening_transition_path()
    return Response(
        status_code=200,
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=86400",
            "Content-Length": str(video_path.stat().st_size),
        },
    )

@app.get("/assets/opening_transition.mp4")
async def opening_transition():
    video_path = _opening_transition_path()
    return FileResponse(
        video_path,
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=86400",
        },
    )

# --- banners ----------------------------------------------------------------
@app.get("/api/v1/banners")
async def get_banners():
    ck = "resp_banners"
    if ck in _home_cache:
        return _home_cache[ck]
    media = await _anilist_spring2026()
    result = []
    for m in media:
        banner = m.get("bannerImage") or ""
        cover  = (m.get("coverImage") or {}).get("extraLarge") or ""
        img    = banner or cover
        if not img:
            continue
        titles = m.get("title") or {}
        title  = (titles.get("english") or titles.get("romaji") or "").strip()
        aired  = _aired_count_from_anilist_media(m)
        result.append({
            "anime_id":      str(m.get("idMal", m["id"])),
            "title":         title,
            "img_url":       img,
            "banner":        banner,
            "cover":         cover,
            "episode_count": aired,
            "score":         round((m.get("averageScore") or 0) / 10, 1),
        })
    result = result[:10]
    if result:  # only cache when we actually got banners
        _home_cache[ck] = result
    return result

# --- thumbnails -------------------------------------------------------------
@app.get("/home/thumbnails")
async def home_thumbnails():
    ck = "resp_thumbnails"
    if ck in _home_cache:
        return _home_cache[ck]
    try:
        entries = await _mal_seasonal()
        nodes = [e["node"] for e in entries]
        ep_map  = await _episode_overrides_for_cards(nodes)
    except Exception as e:
        raise HTTPException(502, f"Fetch failed: {e}")
    result = [_fmt_card(n, ep_override=ep_map.get(int(n.get("id", 0)), 0))
              for n in nodes]
    _home_cache[ck] = result
    return result

# --- recently added ---------------------------------------------------------
@app.get("/home/recently-added")
async def recently_added():
    ck = "resp_recent"
    if ck in _home_cache:
        return _home_cache[ck]
    try:
        entries = await _mal_seasonal()
        nodes = [e["node"] for e in entries]
        ep_map  = await _episode_overrides_for_cards(nodes)
    except Exception as e:
        raise HTTPException(502, f"Fetch failed: {e}")
    airing = [n for n in nodes
              if n.get("status") in ("currently_airing", "not_yet_aired")]
    result = [_fmt_card(n, ep_override=ep_map.get(int(n.get("id", 0)), 0))
              for n in airing[:30]]
    _home_cache[ck] = result
    return result

# --- top-rated --------------------------------------------------------------
@app.get("/home/top-rated")
async def home_top_rated():
    ck = "resp_toprated"
    if ck in _home_cache:
        return _home_cache[ck]
    async with _toprated_lock:
        if ck in _home_cache:
            return _home_cache[ck]
        try:
            data = await _mal_get("/anime/ranking", ranking_type="all", limit=50,
                                  fields="id,title,main_picture,num_episodes,mean,alternative_titles,status")
        except Exception as e:
            raise HTTPException(502, f"MAL ranking failed: {e}")
        nodes = [e["node"] for e in data.get("data", [])]
        ep_map = await _episode_overrides_for_cards(nodes)
        result = [_fmt_card(n, ep_override=ep_map.get(int(n.get("id", 0)), 0)) for n in nodes]
        _home_cache[ck] = result
        return result

# --- search -----------------------------------------------------------------
@app.get("/search/{query}")
async def search_anime(query: str):
    ck = f"search:{query.lower()}"
    if ck in _search_cache:
        return {"results": _search_cache[ck]}

    async def _try(q: str):
        return await _mal_get("/anime", q=q, limit=25, fields=_SEARCH_FIELDS)

    try:
        data = await _try(query)
        nodes = [e["node"] for e in data.get("data", [])]

        # MAL silently returns 0 results for queries longer than ~70 chars.
        # If we got nothing AND the query is suspiciously long, retry with a
        # shortened version (first 4-5 alphabetic words) so users who clicked
        # a suggestion (which inserts the full English title) still get hits.
        if not nodes and len(query) > 50:
            words = re.findall(r"[A-Za-z0-9']+", query)
            if len(words) > 4:
                short = " ".join(words[:5])
                logger.info("Search retry: '%s' -> '%s'", query, short)
                data  = await _try(short)
                nodes = [e["node"] for e in data.get("data", [])]

    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"MAL {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, f"Search failed: {str(e)[:120]}")
    q_lo  = query.lower()
    def _rank(n):
        alt = n.get("alternative_titles") or {}
        en  = (alt.get("en") or "").lower()
        jp  = (n.get("title") or "").lower()
        if en == q_lo or jp == q_lo: return 0
        if en.startswith(q_lo):      return 1
        if jp.startswith(q_lo):      return 2
        if q_lo in en:               return 3
        if q_lo in jp:               return 4
        return 5
    nodes.sort(key=_rank)
    ep_map = await _episode_overrides_for_cards(nodes)
    result = [_fmt_card(n, ep_override=ep_map.get(int(n.get("id", 0)), 0)) for n in nodes]
    _search_cache[ck] = result
    return {"results": result}

# --- suggest ----------------------------------------------------------------
@app.get("/suggest/{query}")
async def suggest_anime(query: str):
    if len(query) < 2:
        return {"results": []}
    ck = f"suggest:{query.lower()}"
    if ck in _suggest_cache:
        return {"results": _suggest_cache[ck]}
    try:
        data = await _mal_get("/anime", q=query, limit=10,
                              fields=_SEARCH_FIELDS)
    except Exception:
        return {"results": []}
    nodes = [e["node"] for e in data.get("data", [])]
    ep_map = await _episode_overrides_for_cards(nodes)
    results = []
    for n in nodes:
        card = _fmt_card(n, ep_override=ep_map.get(int(n.get("id", 0)), 0))
        card["image_url"] = card.get("poster", "")
        card["episode_count"] = card.get("num_episodes", 0)
        results.append(card)
    _suggest_cache[ck] = results
    return {"results": results}

# --- episodes ---------------------------------------------------------------
@app.get("/anime/episode/{mal_id}")
async def get_episodes(mal_id: str, hint: int = 0):
    """Episode list for an anime.

    *hint* — if the frontend already knows the count from a homepage card or
    search result, it can pass it as a query param so we serve immediately
    without waiting on AniList/MAL. We still kick off a refresh in the
    background to upgrade the cache.
    """
    ck = f"episodes:{mal_id}"
    cached = _anime_cache.get(ck)
    if cached and cached.get("num_episodes", 0) > 0:
        return cached

    num_eps, source = await _anilist_episodes_for(int(mal_id))

    if num_eps <= 0 and hint > 0:
        num_eps, source = hint, "hint"

    if num_eps <= 0:
        # Don't fabricate a fake 1-episode list — that confuses users with an
        # empty player. Surface the failure so the frontend retries.
        raise HTTPException(503, "Episode count unavailable, please retry")

    episodes = [{"episode_number": i} for i in range(1, num_eps + 1)]
    result   = {"anime_id": mal_id, "num_episodes": num_eps,
                "episodes": episodes, "source": source}
    _anime_cache[ck] = result
    return result

# --- stream -----------------------------------------------------------------
@app.get("/api/stream/{mal_id}/{episode_num}")
async def get_stream(request: Request, mal_id: str, episode_num: str,
                     type: Literal["sub", "dub"] = "sub",
                     embed: bool = False):
    loop = asyncio.get_running_loop()
    if embed:
        data = StreamData(
            m3u8_url="",
            iframe_url=(
                f"{MEGAPLAY_BASE}/stream/mal/"
                f"{quote(str(mal_id), safe='')}/{quote(str(episode_num), safe='')}/{type}"
            ),
            mal_id=mal_id,
            episode_num=episode_num,
        )
    else:
        ck = f"megaplay:{mal_id}:{episode_num}:{type}"
        data: StreamData | None = _anime_cache.get(ck)
        if data is None:
            try:
                data = await loop.run_in_executor(
                    None, scraper.get_stream, mal_id, episode_num, type)
                _anime_cache[ck] = data
            except EpisodeNotFound:
                raise HTTPException(404, "Episode not found on MegaPlay")
            except InvalidRequest as e:
                raise HTTPException(400, str(e))
            except StreamBlocked as e:
                raise HTTPException(503, f"Upstream blocked: {e}")
            except PlayerPageError as e:
                raise HTTPException(502, f"Player page error: {e}")
            except MegaPlayError as e:
                raise HTTPException(500, str(e))

    backend = _stream_proxy_base(request)
    result  = data.to_dict()
    if data.m3u8_url:
        result["m3u8_url"] = _proxy_url(backend, data.m3u8_url, "/proxy/m3u8")
    for sub in result["subtitles"]:
        if sub.get("file"):
            sub["file"] = _proxy_url(backend, sub["file"], "/proxy/vtt")
    return result

# --- m3u8 proxy -------------------------------------------------------------
@app.get("/proxy/m3u8")
async def proxy_m3u8(request: Request, src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    headers, impersonate = _proxy_call_for(url)
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=headers, impersonate=impersonate, timeout=15))
    except Exception as e:
        raise HTTPException(502, f"Upstream fetch failed: {e}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    rewritten = rewrite_m3u8(r.text, url, _playlist_proxy_base(request, url))
    body, status_code, out_headers = _apply_local_range(
        rewritten.encode("utf-8"),
        request.headers.get("range"),
        "no-cache",
    )
    return Response(content=body, media_type="application/vnd.apple.mpegurl",
                    status_code=status_code, headers=out_headers)

@app.head("/proxy/m3u8")
async def proxy_m3u8_head(request: Request, src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    headers, impersonate = _proxy_call_for(url)
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=headers, impersonate=impersonate, timeout=15))
    except Exception as e:
        raise HTTPException(502, f"Upstream fetch failed: {e}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    rewritten = rewrite_m3u8(r.text, url, _playlist_proxy_base(request, url))
    _, status_code, out_headers = _apply_local_range(
        rewritten.encode("utf-8"),
        request.headers.get("range"),
        "no-cache",
    )
    return Response(content=b"", media_type="application/vnd.apple.mpegurl",
                    status_code=status_code, headers=out_headers)

# --- chunk proxy ------------------------------------------------------------
@app.get("/proxy/chunk")
async def proxy_chunk(request: Request, src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    headers, impersonate = _proxy_call_for(url)
    req_range = request.headers.get("range", "").strip()
    requested = _parse_single_range(req_range) if req_range else None
    fetch_headers = {**headers}
    if requested:
        fetch_headers["Range"] = req_range
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=fetch_headers, impersonate=impersonate, timeout=30))
    except Exception as e:
        raise HTTPException(502, f"Upstream failed: {type(e).__name__}")
    if r.status_code not in (200, 206):
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    body = r.content
    if not body: raise HTTPException(502, "Empty body from upstream")
    total_size = _response_content_length(r)
    content_range = r.headers.get("content-range")
    if r.status_code == 206 and content_range:
        m = re.match(r"bytes\s+\d+-\d+/(\d+|\*)", content_range)
        if m and m.group(1).isdigit():
            total_size = int(m.group(1))
    elif requested and r.status_code == 200:
        total_size = len(body)
        parsed = _parse_single_range(req_range, total_size)
        if not parsed:
            raise HTTPException(416, "Invalid Range")
        start, end = parsed
        if start >= total_size:
            raise HTTPException(416, "Range not satisfiable")
        if end is None or end >= total_size:
            end = total_size - 1
        body = body[start:end + 1]
        requested = (start, end)
    status_code, out_headers = _range_response_headers(
        content_len=len(body),
        total_size=total_size,
        requested=requested if req_range else None,
        upstream_content_range=content_range if r.status_code == 206 else None,
    )
    return Response(content=body, media_type=_detect_mime(url, body),
                    status_code=status_code, headers=out_headers)

@app.head("/proxy/chunk")
async def proxy_chunk_head(request: Request, src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    headers, impersonate = _proxy_call_for(url)
    req_range = request.headers.get("range", "").strip()
    fetch_headers = {**headers, "Range": req_range or "bytes=0-0"}
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=fetch_headers, impersonate=impersonate, timeout=30))
    except Exception as e:
        raise HTTPException(502, f"Upstream failed: {type(e).__name__}")
    if r.status_code not in (200, 206):
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    total_size = _response_content_length(r)
    content_range = r.headers.get("content-range")
    if content_range:
        m = re.match(r"bytes\s+\d+-\d+/(\d+|\*)", content_range)
        if m and m.group(1).isdigit():
            total_size = int(m.group(1))
    requested = _parse_single_range(req_range, total_size) if req_range else None
    status_code = 200
    out_headers = {
        "Cache-Control": "public, max-age=3600",
        "Access-Control-Allow-Origin": "*",
        "Accept-Ranges": "bytes",
    }
    if req_range:
        if r.status_code == 206 and content_range:
            status_code = 206
            out_headers["Content-Range"] = content_range
            out_headers["Content-Length"] = str(len(r.content))
        elif requested:
            start, end = requested
            if total_size is not None:
                if start >= total_size:
                    raise HTTPException(416, "Range not satisfiable")
                if end is None or end >= total_size:
                    end = total_size - 1
                out_headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
                out_headers["Content-Length"] = str(end - start + 1)
                status_code = 206
    elif total_size is not None:
        out_headers["Content-Length"] = str(total_size)
    content_type = _detect_mime(url, r.content)
    return Response(content=b"", media_type=content_type,
                    status_code=status_code, headers=out_headers)

# --- vtt proxy --------------------------------------------------------------
@app.get("/proxy/vtt")
async def proxy_vtt(request: Request, src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=PROXY_HEADERS, impersonate=IMPERSONATE, timeout=15))
    except Exception as e:
        raise HTTPException(502, f"Upstream failed: {e}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    body, status_code, out_headers = _apply_local_range(
        r.text.encode("utf-8"),
        request.headers.get("range"),
        "public, max-age=3600",
    )
    return Response(content=body, media_type="text/vtt; charset=utf-8",
                    status_code=status_code, headers=out_headers)

@app.head("/proxy/vtt")
async def proxy_vtt_head(request: Request, src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=PROXY_HEADERS, impersonate=IMPERSONATE, timeout=15))
    except Exception as e:
        raise HTTPException(502, f"Upstream failed: {e}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    _, status_code, out_headers = _apply_local_range(
        r.text.encode("utf-8"),
        request.headers.get("range"),
        "public, max-age=3600",
    )
    return Response(content=b"", media_type="text/vtt; charset=utf-8",
                    status_code=status_code, headers=out_headers)

# --- image proxy ------------------------------------------------------------
@app.get("/proxy/img")
async def proxy_img(src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    try:
        r = await _http.get(url, headers={"User-Agent": ANDROID_UA,
                                           "Referer": "https://myanimelist.net/"})
        if r.status_code != 200:
            raise HTTPException(r.status_code, "Image unavailable")
        ct = r.headers.get("content-type", "image/jpeg")
        return Response(content=r.content, media_type=ct,
                        headers={"Cache-Control": "public, max-age=86400",
                                 "Access-Control-Allow-Origin": "*"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Image fetch failed: {str(e)[:80]}")

# --- health -----------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "cached_keys": list(_home_cache.keys())}

# ---------------------------------------------------------------------------
# PARALLEL HLS DOWNLOAD MANAGER
# ---------------------------------------------------------------------------
_DL_HEADERS = {
    "User-Agent": MOON_UA,
    "Referer":    "https://9anime.org.lv/",
    "Origin":     "https://9anime.org.lv",
}

# In-memory task store  { task_id → task_dict }
_dl_tasks: dict[str, dict] = {}
_DL_CONCURRENCY = 1000    # simultaneous segment fetches per task; HTTP/2 multiplexes them over a few sockets
_DL_TASK_TTL    = 7200    # seconds before a completed task is auto-purged

async def _resolve_segments(m3u8_url: str) -> list[str]:
    """Fetch m3u8, resolve master→variant playlist if needed, return .ts URLs."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as cl:
        r = await cl.get(m3u8_url, headers=_DL_HEADERS)
    if r.status_code != 200:
        raise ValueError(f"m3u8 returned {r.status_code}")

    def _parse_non_comment(text: str) -> list[str]:
        return [l.strip() for l in text.splitlines()
                if l.strip() and not l.strip().startswith("#")]

    text  = r.text
    base  = m3u8_url.rsplit("/", 1)[0] + "/"
    lines = _parse_non_comment(text)

    # Master playlist? Pick the highest-bandwidth variant (last EXT-X-STREAM-INF entry)
    if "#EXT-X-STREAM-INF" in text:
        all_lines   = text.splitlines()
        best_bw     = -1
        best_uri    = ""
        for i, line in enumerate(all_lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                bw = 0
                bw_m = re.search(r"BANDWIDTH=(\d+)", line)
                if bw_m:
                    bw = int(bw_m.group(1))
                # next non-empty, non-comment line is the URI
                for j in range(i + 1, len(all_lines)):
                    nxt = all_lines[j].strip()
                    if nxt and not nxt.startswith("#"):
                        if bw >= best_bw:
                            best_bw  = bw
                            best_uri = nxt
                        break
        if best_uri:
            variant = best_uri if best_uri.startswith("http") else base + best_uri
            async with httpx.AsyncClient(follow_redirects=True, timeout=20) as cl:
                r2 = await cl.get(variant, headers=_DL_HEADERS)
            text  = r2.text
            base  = variant.rsplit("/", 1)[0] + "/"
            lines = _parse_non_comment(text)
    elif lines and ".m3u8" in lines[0].split("?")[0]:
        # Simple variant (no EXT-X-STREAM-INF tags)
        variant = lines[0] if lines[0].startswith("http") else base + lines[0]
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as cl:
            r2 = await cl.get(variant, headers=_DL_HEADERS)
        text  = r2.text
        base  = variant.rsplit("/", 1)[0] + "/"
        lines = _parse_non_comment(text)

    return [l if l.startswith("http") else base + l for l in lines if l]

async def _run_dl_task(task_id: str) -> None:
    task = _dl_tasks.get(task_id)
    if not task:
        return
    try:
        # ── 1. resolve segments ──────────────────────────────────────────────
        task["status"] = "probing"
        segments = await _resolve_segments(task["m3u8_url"])
        if not segments:
            raise ValueError("Playlist has no segments")
        total = len(segments)
        task.update({"total": total, "done": 0, "bytes_done": 0, "status": "downloading"})

        # ── 2. parallel fetch — ONE shared client, semaphore caps concurrency ──
        sem      = asyncio.Semaphore(_DL_CONCURRENCY)
        buffers: list[bytes | None] = [None] * total

        # HTTP/2 multiplexes thousands of streams over a small connection pool,
        # so we cap real sockets at 64 even when the semaphore allows 1000 in-flight.
        async with httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(
                max_connections=64,
                max_keepalive_connections=32,
            ),
        ) as cl:
            async def fetch_one(idx: int, url: str) -> None:
                async with sem:
                    for attempt in range(3):
                        try:
                            resp = await cl.get(url, headers=_DL_HEADERS)
                            if resp.status_code == 200:
                                buffers[idx]        = resp.content
                                task["done"]       += 1
                                task["bytes_done"] += len(resp.content)
                                return
                        except Exception:
                            if attempt < 2:
                                await asyncio.sleep(0.4 * (attempt + 1))
                    task["done"] += 1   # count failed segs so % stays sane

            await asyncio.gather(*[fetch_one(i, url) for i, url in enumerate(segments)])

        # ── 3. merge ─────────────────────────────────────────────────────────
        task["status"] = "merging"
        merged = b"".join(b for b in buffers if b is not None)

        # ── 4. subtitle (best-effort) ─────────────────────────────────────────
        sub_text: str | None = None
        if task.get("subtitle_url"):
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=15) as cl:
                    sr = await cl.get(task["subtitle_url"], headers=_DL_HEADERS)
                if sr.status_code == 200:
                    sub_text = sr.text
            except Exception:
                pass

        task.update({
            "status":     "done",
            "video_data": merged,
            "sub_data":   sub_text,
            "size_bytes": len(merged),
            "has_subs":   sub_text is not None,
            "done_at":    time.time(),
        })
        logger.info(f"[DL] task {task_id} done — {len(merged)//1024//1024} MB, {total} segs")

    except Exception as exc:
        task["status"] = "error"
        task["error"]  = str(exc)[:200]
        logger.error(f"[DL] task {task_id} failed: {exc}")

def _dl_purge_old() -> None:
    """Remove tasks that finished more than TTL seconds ago."""
    now = time.time()
    dead = [tid for tid, t in _dl_tasks.items()
            if t.get("done_at") and (now - t["done_at"]) > _DL_TASK_TTL]
    for tid in dead:
        del _dl_tasks[tid]

# ── endpoints ────────────────────────────────────────────────────────────────

@app.post("/api/dl/start")
async def dl_start(request: Request, background_tasks: BackgroundTasks):
    user = _get_current_user(request)
    d    = await request.json()
    m3u8 = d.get("m3u8_url", "").strip()
    if not m3u8:
        raise HTTPException(400, "m3u8_url required")

    _dl_purge_old()

    task_id = uuid.uuid4().hex[:10]
    _dl_tasks[task_id] = {
        "task_id":      task_id,
        "user_id":      user["id"],
        "status":       "queued",
        "done":         0,
        "total":        0,
        "bytes_done":   0,
        "size_bytes":   0,
        "m3u8_url":     m3u8,
        "subtitle_url": d.get("subtitle_url"),
        "mal_id":       d.get("mal_id", ""),
        "title":        d.get("title", ""),
        "episode":      int(d.get("episode", 0)),
        "image_url":    d.get("image_url", ""),
        "idb_key":      d.get("idb_key", ""),
        "video_data":   None,
        "sub_data":     None,
        "has_subs":     False,
        "error":        None,
        "started_at":   time.time(),
        "done_at":      None,
    }
    background_tasks.add_task(_run_dl_task, task_id)
    return {"task_id": task_id}

@app.get("/api/dl/status")
async def dl_status(request: Request):
    user = _get_current_user(request)
    out  = []
    for tid, t in list(_dl_tasks.items()):
        if t["user_id"] != user["id"]:
            continue
        total = t["total"] or 1
        pct   = min(99, round(t["done"] / total * 100)) if t["status"] == "downloading" \
                else (100 if t["status"] == "done" else 0)
        out.append({
            "task_id":    tid,
            "status":     t["status"],
            "progress":   pct,
            "done":       t["done"],
            "total":      t["total"],
            "bytes_done": t["bytes_done"],
            "size_bytes": t["size_bytes"],
            "mal_id":     t["mal_id"],
            "title":      t["title"],
            "episode":    t["episode"],
            "image_url":  t["image_url"],
            "idb_key":    t["idb_key"],
            "has_subs":   t["has_subs"],
            "error":      t["error"],
        })
    return out

def _user_from_token(token: str) -> dict | None:
    """Look up a user by raw token (used by WebSocket auth)."""
    if not token:
        return None
    conn = _db()
    row  = conn.execute("SELECT id, username FROM users WHERE token=?", (token,)).fetchone()
    conn.close()
    if not row:
        return None
    return {"id": row["id"], "username": row["username"]}


def _build_dl_status_for_user(user_id: int) -> list[dict]:
    out = []
    for tid, t in list(_dl_tasks.items()):
        if t["user_id"] != user_id:
            continue
        total = t["total"] or 1
        pct   = min(99, round(t["done"] / total * 100)) if t["status"] == "downloading" \
                else (100 if t["status"] == "done" else 0)
        out.append({
            "task_id":    tid,
            "status":     t["status"],
            "progress":   pct,
            "done":       t["done"],
            "total":      t["total"],
            "bytes_done": t["bytes_done"],
            "size_bytes": t["size_bytes"],
            "mal_id":     t["mal_id"],
            "title":      t["title"],
            "episode":    t["episode"],
            "image_url":  t["image_url"],
            "idb_key":    t["idb_key"],
            "has_subs":   t["has_subs"],
            "error":      t["error"],
        })
    return out


@app.websocket("/api/dl/ws")
async def dl_ws(websocket: WebSocket):
    """Real-time download progress stream. Client connects with ?token=<auth_token>."""
    token = websocket.query_params.get("token", "").strip()
    user  = _user_from_token(token)
    if not user:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    last_payload: str | None = None
    try:
        while True:
            statuses = _build_dl_status_for_user(user["id"])
            payload  = json.dumps(statuses, separators=(",", ":"))
            # Only push when something changed — keeps the wire quiet but UI live
            if payload != last_payload:
                await websocket.send_text(payload)
                last_payload = payload
            # If everything is done/error and there's at least one task, slow down
            active = any(s["status"] in ("queued", "probing", "downloading", "merging")
                         for s in statuses)
            await asyncio.sleep(0.4 if active else 1.5)
    except WebSocketDisconnect:
        return
    except Exception as e:
        logger.warning("DL websocket error: %s", e)
        try: await websocket.close()
        except Exception: pass


@app.get("/api/dl/file/{task_id}")
async def dl_file(task_id: str, request: Request):
    user = _get_current_user(request)
    task = _dl_tasks.get(task_id)
    if not task or task["user_id"] != user["id"]:
        raise HTTPException(404, "Task not found")
    if task["status"] != "done" or task["video_data"] is None:
        raise HTTPException(400, f"Not ready ({task['status']})")
    data = task["video_data"]
    return Response(
        content=data,
        media_type="video/mp2t",
        headers={
            "Content-Length":        str(len(data)),
            "X-Has-Subs":            "1" if task["has_subs"] else "0",
            "Access-Control-Allow-Origin": "*",
        }
    )

@app.get("/api/dl/subtitle/{task_id}")
async def dl_subtitle_file(task_id: str, request: Request):
    user = _get_current_user(request)
    task = _dl_tasks.get(task_id)
    if not task or task["user_id"] != user["id"] or not task.get("sub_data"):
        raise HTTPException(404, "No subtitle")
    return Response(content=task["sub_data"], media_type="text/vtt; charset=utf-8")

@app.delete("/api/dl/task/{task_id}")
async def dl_task_cleanup(task_id: str, request: Request):
    """Call after frontend has saved the file to IndexedDB to free server memory."""
    user = _get_current_user(request)
    task = _dl_tasks.get(task_id)
    if task and task["user_id"] == user["id"]:
        # Free large buffers immediately, keep metadata briefly
        task["video_data"] = None
        task["sub_data"]   = None
        task["done_at"]    = task.get("done_at") or time.time()
        # Full remove after 60 s
        async def _remove():
            await asyncio.sleep(60)
            _dl_tasks.pop(task_id, None)
        asyncio.create_task(_remove())
    return {"ok": True}

# ---------------------------------------------------------------------------
# SLUG  — search 9anime.org.lv (same way our /suggest/ hits MAL)
# ---------------------------------------------------------------------------
_9ANIME_SEARCH = "https://9anime.org.lv/?s="
_NAV_SKIP      = {'Anime List', 'Home', 'TV', 'Movie', 'Special', 'ONA', 'OVA', ''}

# Roman numeral map for season suffixes (COTE-style)
_ROMANS = {1: "", 2: "ii", 3: "iii", 4: "iv", 5: "v", 6: "vi", 7: "vii", 8: "viii"}

def _slugify(text: str) -> str:
    """
    Refined slug algorithm for 9anime.org.lv.
    - Strip punctuation that the site omits entirely (:  ‘  ‘  !  ?  .  (  )  +  *)
    - Replace remaining non-alphanumeric runs with a single hyphen
    """
    s = text.lower()
    s = re.sub(r"[:\u2018\u2019!\?\.\(\)\+\*]", "", s)   # remove, don’t space
    s = re.sub(r"[^a-z0-9]+", "-", s)                 # everything else → hyphen
    s = re.sub(r"-+", "-", s)                          # collapse doubles
    return s.strip("-")


def _get_slug_variations(title: str) -> list[str]:
    """
    Generate every plausible slug the site might use for *title*.

    Two major patterns coexist on 9anime.org.lv:
      A) title-season-X   (e.g. re-zero-kara-hajimeru-isekai-seikatsu-season-3)
      B) title-II / III … (e.g. youkoso-jitsuryoku-shijou-shugi-no-kyoushitsu-e-iv)

    Both are tried so the URL-validator can pick the one that actually resolves.
    """
    variations: list[str] = []
    seen: set[str] = set()

    def _add(s: str) -> None:
        s = s.strip("-")
        if s and s not in seen:
            seen.add(s)
            variations.append(s)

    base = _slugify(title)
    _add(base)

    season_m = re.search(r"\bseason\s*(\d+)\b", title, re.IGNORECASE)
    if season_m:
        num = int(season_m.group(1))
        # strip "Season N" from title to get the clean base
        clean = re.sub(r"\bseason\s*\d+\b", "", title, flags=re.IGNORECASE).strip(" -,")
        clean_slug = _slugify(clean)

        # Pattern A — explicit "season-N" suffix
        _add(f"{clean_slug}-season-{num}")

        # Pattern B — Roman numeral suffix (skip I since that’s just the plain title)
        roman = _ROMANS.get(num, "")
        if roman:
            _add(f"{clean_slug}-{roman}")
        else:
            # Season 1 ≙ no suffix on many sites
            _add(clean_slug)

    return variations

async def _search_9anime_slug(title: str) -> str | None:
    """
    GET 9anime.org.lv/?s={title}, parse HTML result links, return best slug.
    Mirrors exactly what the site's own search bar does.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None

    url = f"{_9ANIME_SEARCH}{quote_plus(title)}"
    try:
        r = await _http.get(url, headers={"User-Agent": MOON_UA},
                            timeout=httpx.Timeout(connect=5.0, read=10.0,
                                                  write=5.0, pool=5.0))
        if r.status_code != 200:
            return None
    except Exception as e:
        logger.warning("9anime search request failed: %s", e)
        return None

    soup    = BeautifulSoup(r.text, "html.parser")
    results = []
    seen    = set()

    articles = soup.find_all("article", class_="bs")
    for idx, art in enumerate(articles):
        a = art.find("a", href=True)
        if not a: continue
        href = a["href"]
        m = re.search(r'/anime/([^/?#]+)/?', href)
        if not m: continue
        slug = m.group(1)
        if slug in seen: continue
        seen.add(slug)
        tt_div = a.find(class_="tt")
        text = tt_div.get_text(" ", strip=True) if tt_div else a.get_text(" ", strip=True)
        results.append({"title": text, "slug": slug, "pos": idx})

    if not results:
        return None

    q_lo = title.lower()
    q_slug = _slugify(title)
    q_words = [w for w in re.sub(r'[^a-z0-9]+', ' ', q_lo).split() if len(w) > 1]

    def _score(item: dict) -> float:
        t = item["title"].lower()
        s = item["slug"].lower()
        t_clean = re.sub(r'[^a-z0-9]+', ' ', t)
        t_words = t_clean.split()
        
        score = 0.0
        if t == q_lo: score += 5000
        if s == q_slug: score += 4000
        
        # Word coverage
        if all(w in t_words for w in q_words):
            score += 1000
        else:
            matches = sum(1 for w in q_words if w in t_words)
            score += (matches / len(q_words)) * 500 if q_words else 0
        
        # Position boost (site's own relevance)
        score += (100 - item["pos"]) * 10
        
        # Penalize extra words
        score -= (len(t_words) - len(q_words)) * 50
        
        if "dub" in t_words and "dub" not in q_words:
            score -= 500
            
        return score

    results.sort(key=_score, reverse=True)
    best = results[0]
    logger.info("9anime search '%s' → slug=%s (title=%s)", title, best["slug"], best["title"])
    return best["slug"]

_SLUG_OVERRIDES: dict[str, str] = {
    "21": "one-piece",
    "59708": "classroom-of-the-elite-iv",   # Classroom of the Elite Season 4
}

async def _moon_get_slug(mal_id: str, episode: str) -> str | None:
    """
    Resolve the 9anime.org.lv anime slug for *mal_id*.

    Resolution order (stops at first hit):
      0. Hardcoded override table (_SLUG_OVERRIDES) — instant, no network.
      1. Cache hit — return immediately.
      2. 9anime.org.lv site-search for each Jikan title (EN / JP / synonyms).
      3. URL validation — generate every _get_slug_variations() candidate and
         GET the real episode URL; the first 200-OK wins.
      4. Local slugify fallback (not cached so next call retries steps 2-3).

    Returns '{anime-slug}-episode-{episode}'.
    """
    # ── 0. Hardcoded overrides ─────────────────────────────────────────────────
    if mal_id in _SLUG_OVERRIDES:
        return f"{_SLUG_OVERRIDES[mal_id]}-episode-{episode}"

    slug_ck = f"slug:{mal_id}"
    if slug_ck in _anime_cache:
        return f"{_anime_cache[slug_ck]}-episode-{episode}"

    # ── 1. Fetch all titles from Jikan ────────────────────────────────────────
    try:
        r = await _http.get(f"https://api.jikan.moe/v4/anime/{mal_id}",
                            headers={"Accept": "application/json"})
        r.raise_for_status()
        d        = r.json().get("data", {})
        title_en = (d.get("title_english") or "").strip()
        title_jp = (d.get("title") or "").strip()
        synonyms = [s.strip() for s in (d.get("title_synonyms") or []) if s.strip()]
    except Exception as e:
        logger.warning("Slug Jikan failed: %s", e)
        return None

    # Ordered: EN first (closest to what the site typically shows), then JP, then synonyms
    all_titles = [t for t in [title_en, title_jp] + synonyms if t]
    if not all_titles:
        return None

    # ── 2. Site-search on 9anime.org.lv ───────────────────────────────────────
    slug: str | None = None
    for title in all_titles:
        slug = await _search_9anime_slug(title)
        if slug:
            logger.info("Slug via site-search mal_id=%s: %s", mal_id, slug)
            break

    # ── 3. URL validation with generated variations ───────────────────────────
    if not slug:
        nine_hdrs = {"User-Agent": MOON_UA, "Accept": "text/html"}
        nine_to   = httpx.Timeout(connect=4.0, read=7.0, write=4.0, pool=4.0)
        outer_break = False
        for title in all_titles:
            for variation in _get_slug_variations(title):
                test_url = f"https://9anime.org.lv/{variation}-episode-{episode}/"
                try:
                    res = await _http.get(test_url, headers=nine_hdrs, timeout=nine_to)
                    if res.status_code == 200 and "Page not found" not in res.text:
                        slug = variation
                        logger.info("Slug via URL-validate mal_id=%s: %s → %s",
                                    mal_id, title, slug)
                        outer_break = True
                        break
                except Exception:
                    pass
            if outer_break:
                break

    # ── 4. Local fallback (not cached) ────────────────────────────────────────
    if not slug:
        primary = title_en or title_jp
        slug = _get_slug_variations(primary)[0] if primary else ""
        logger.info("Slug local-fallback (not cached) mal_id=%s: %s", mal_id, slug)
    else:
        _anime_cache[slug_ck] = slug   # only cache confirmed slugs

    return f"{slug}-episode-{episode}"

# ---------------------------------------------------------------------------
# MOON + HD scraper pipeline  (pure httpx, no browser)
#
# Everything we need is in static HTML responses. No JS execution required.
#
#   1. /watch/{slug}                       -> mirror <select> with base64
#                                             option values; each decodes to
#                                             an <iframe src="..."> tag.
#   2. HD iframe is gogoanime.me.uk/mp4    -> static HTML response of that
#                                             page contains the workers.dev
#                                             video URL directly.
#   3. Moon iframe is bysesayeveum/e/<id>  -> video_id is the path tail.
#   4. Moon playback API on 398fitus.com   -> AES-GCM payload, decrypted
#                                             locally with pycryptodome.
# ---------------------------------------------------------------------------
_scrape_locks: dict[str, asyncio.Lock] = {}

def _get_scrape_lock(slug: str) -> asyncio.Lock:
    if slug not in _scrape_locks:
        _scrape_locks[slug] = asyncio.Lock()
    return _scrape_locks[slug]


_WATCH_URL_TMPLS = (
    "https://9animetv.org.lv/{}/",
    "https://9animetv.org.lv/watch/{}",
)
_MIRROR_RE      = re.compile(r'<select[^>]*class="mirror"[^>]*>(.*?)</select>', re.S)
_OPTION_RE      = re.compile(r'<option[^>]*value="([^"]+)"[^>]*>([^<]*)</option>')
_BIG_VALUE_RE   = re.compile(r'<option[^>]*value="[A-Za-z0-9+/=]{20,}"')
_WORKERS_RE     = re.compile(r'https://[^"\'<>\s]*workers\.dev[^"\'<>\s]+')
_MOON_ID_RE     = re.compile(r'/e/([A-Za-z0-9_-]+)')


async def _fetch_watch_page(slug_with_ep: str, read_timeout: float = 10.0):
    """Fetch a real 9anime watch page, trying both current and legacy URL shapes."""
    headers = {"User-Agent": MOON_UA, "Accept": "text/html"}
    timeout = httpx.Timeout(connect=4.0, read=read_timeout, write=4.0, pool=4.0)
    last_error = None
    for tmpl in _WATCH_URL_TMPLS:
        url = tmpl.format(slug_with_ep)
        try:
            r = await _http.get(
                url,
                headers=headers,
                timeout=timeout,
                follow_redirects=True,
            )
            if r.status_code == 200 and _MIRROR_RE.search(r.text):
                return r
            last_error = f"HTTP {r.status_code} for {url}"
        except Exception as e:
            last_error = e
    if last_error:
        logger.warning("Watch page fetch failed for %s: %s", slug_with_ep, last_error)
    return None


async def _verify_watch_url(slug_with_ep: str) -> bool:
    """True iff the watch page returns a populated mirror selector."""
    r = await _fetch_watch_page(slug_with_ep, read_timeout=8.0)
    if not r:
        return False
    m = _MIRROR_RE.search(r.text)
    if not m:
        return False
    return bool(_BIG_VALUE_RE.search(m.group(1)))


async def _resolve_real_slug(mal_id: str, episode: str) -> str | None:
    """Return a fully-qualified slug (with -episode-N) verified against the
    actual /watch page. Returns None if nothing matches.
    """
    # 0. Hardcoded override
    if mal_id in _SLUG_OVERRIDES:
        cand = f"{_SLUG_OVERRIDES[mal_id]}-episode-{episode}"
        if await _verify_watch_url(cand):
            return cand

    # 1. Cache (verify before returning — site may have rerouted)
    ck = f"slug:{mal_id}"
    cached = _anime_cache.get(ck)
    if cached:
        cand = f"{cached}-episode-{episode}"
        if await _verify_watch_url(cand):
            return cand
        try: del _anime_cache[ck]
        except KeyError: pass

    # 2. Generate variations from Jikan titles
    try:
        r = await _http.get(f"https://api.jikan.moe/v4/anime/{mal_id}",
                            headers={"Accept": "application/json"})
        r.raise_for_status()
        d = r.json().get("data", {})
        title_en = (d.get("title_english") or "").strip()
        title_jp = (d.get("title") or "").strip()
        synonyms = [s.strip() for s in (d.get("title_synonyms") or []) if s.strip()]
    except Exception as e:
        logger.warning("Slug Jikan lookup mal_id=%s failed: %s", mal_id, e)
        return None

    titles = [t for t in [title_en, title_jp] + synonyms if t]
    if not titles:
        return None

    seen: set[str] = set()
    candidates: list[str] = []
    for title in titles:
        for v in _get_slug_variations(title):
            if v not in seen:
                seen.add(v)
                candidates.append(v)

    # Test in batches of 6 in parallel
    for i in range(0, len(candidates), 6):
        batch = candidates[i:i+6]
        results = await asyncio.gather(
            *[_verify_watch_url(f"{c}-episode-{episode}") for c in batch],
            return_exceptions=True,
        )
        for c, ok in zip(batch, results):
            if ok is True:
                _anime_cache[ck] = c
                logger.info("Slug verified mal_id=%s: %s", mal_id, c)
                return f"{c}-episode-{episode}"

    # 3. Site search fallback
    for title in titles:
        slug = await _search_9anime_slug(title)
        if not slug:
            continue
        cand = f"{slug}-episode-{episode}"
        if await _verify_watch_url(cand):
            _anime_cache[ck] = slug
            logger.info("Slug via search mal_id=%s: %s", mal_id, slug)
            return cand

    logger.warning("Slug resolution exhausted for mal_id=%s", mal_id)
    return None


def _decode_iframe_from_option_value(b64: str) -> str | None:
    try:
        decoded = base64.b64decode(b64 + "==").decode("utf-8", "ignore")
    except Exception:
        return None
    m = re.search(r'src=["\']([^"\']+)["\']', decoded)
    return m.group(1) if m else None


async def _fetch_watch_servers(slug_with_ep: str) -> dict[str, str]:
    """Single httpx GET on the watch page; returns {'hd': iframe, 'moon': iframe, 'omega': iframe}."""
    r = await _fetch_watch_page(slug_with_ep)
    if not r:
        return {}

    out: dict[str, str] = {}
    m = _MIRROR_RE.search(r.text)
    if not m:
        return out

    for opt in _OPTION_RE.finditer(m.group(1)):
        val   = opt.group(1)
        label = re.sub(r'<[^>]+>', '', opt.group(2)).strip().lower()
        if not val:
            continue
        iframe = _decode_iframe_from_option_value(val)
        if not iframe:
            continue
        iframe_lo = iframe.lower()
        if "moon" in label:
            out["moon"] = iframe
        elif "omega" in label:
            out["omega"] = iframe
        elif re.search(r'\bhd\b', label) or "gogoanime" in iframe_lo or "workers.dev" in iframe_lo:
            out["hd"] = iframe

    return out


async def _fetch_hd_stream_url(hd_iframe_src: str) -> str | None:
    """gogoanime.me.uk/mp4.php?id=X -> workers.dev video URL.

    The gogoanime page is a thin static wrapper that embeds the workers.dev
    URL directly inside an <iframe src="...">. No JS execution needed.
    """
    try:
        r = await _http.get(
            hd_iframe_src,
            headers={
                "User-Agent": MOON_UA,
                "Referer":    "https://9animetv.org.lv/",
            },
            timeout=httpx.Timeout(connect=4.0, read=10.0, write=4.0, pool=4.0),
            follow_redirects=True,
        )
        if r.status_code != 200:
            logger.warning("HD iframe HTTP %s for %s", r.status_code, hd_iframe_src)
            return None
    except Exception as e:
        logger.warning("HD iframe fetch failed: %s", e)
        return None

    m = _WORKERS_RE.search(r.text)
    if not m:
        return None
    return m.group(0).replace("&amp;", "&")


def _moon_video_id_from_iframe(moon_iframe_src: str) -> str | None:
    m = _MOON_ID_RE.search(moon_iframe_src)
    return m.group(1) if m else None




# ---------------------------------------------------------------------------
# MOON CRYPTO HELPERS
# ---------------------------------------------------------------------------
def _b64url_decode(data: str) -> bytes:
    padding = '=' * (4 - len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)

def _decrypt_aes_gcm(payload_b64: str, key: bytes, iv_b64: str):
    try:
        from Crypto.Cipher import AES
    except ImportError:
        logger.error("pycryptodome not installed — Moon decryption disabled. "
                     "Add 'pycryptodome' to requirements.txt.")
        return None
    try:
        full      = _b64url_decode(payload_b64)
        iv        = _b64url_decode(iv_b64)
        cipher    = AES.new(key, AES.MODE_GCM, nonce=iv)
        decrypted = cipher.decrypt_and_verify(full[:-16], full[-16:])
        return decrypted.decode("utf-8")
    except Exception as e:
        logger.warning("Moon AES-GCM decrypt failed: %s", e)
        return None

def _find_url(obj) -> str | None:
    if isinstance(obj, str):
        if obj.startswith("http") and (".m3u8" in obj or "stream" in obj or "video" in obj):
            return obj
    elif isinstance(obj, dict):
        for k in ("url", "stream", "src", "file", "hls", "source"):
            v = obj.get(k)
            if isinstance(v, str) and v.startswith("http"):
                return v
        for v in obj.values():
            r = _find_url(v)
            if r: return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_url(item)
            if r: return r
    return None

def _moon_fetch_playback_sync(video_id: str) -> dict | None:
    url          = f"https://398fitus.com/api/videos/{video_id}/embed/playback"
    subtitle_url = f"https://398fitus.com/api/videos/{video_id}/embed/timeslider"
    headers = {
        "Accept":               "*/*",
        "Accept-Encoding":      "gzip, deflate, br, zstd",
        "Accept-Language":      "en-US,en;q=0.9,en-IN;q=0.8",
        "Connection":           "keep-alive",
        "Content-Type":         "application/json",
        "Cookie":               "byse_viewer_id=c009923f810d4c4b9d2999ca139a4; byse_device_id=Syp8ffvQ8KSxOf4ra-Wu2g",
        "Host":                 "398fitus.com",
        "Origin":               "https://398fitus.com",
        "Referer":              f"https://398fitus.com/ed4/{video_id}",
        "Sec-Fetch-Dest":       "empty",
        "Sec-Fetch-Mode":       "cors",
        "Sec-Fetch-Site":       "same-origin",
        "Sec-Fetch-Storage-Access": "active",
        "User-Agent":           "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0",
        "X-Embed-Origin":       "9animetv.org.lv",
        "X-Embed-Parent":       f"https://bysesayeveum.com/e/{video_id}",
        "X-Embed-Referer":      "https://9animetv.org.lv/",
        "sec-ch-ua":            '"Microsoft Edge";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile":     "?0",
        "sec-ch-ua-platform":   '"Windows"',
    }
    try:
        fingerprint = {
            "token": secrets.token_urlsafe(24),
            "viewer_id": uuid.uuid4().hex,
            "device_id": uuid.uuid4().hex,
            "confidence": 1,
        }
        resp = cffi_requests.post(
            url,
            headers=headers,
            json={"fingerprint": fingerprint},
            impersonate="chrome136",
        )
        if resp.status_code != 200:
            logger.warning("Moon playback HTTP %s", resp.status_code)
            return None
        api  = resp.json()
        pb   = api.get("playback", {})
        key_parts = pb.get("key_parts", [])
        if not key_parts:
            return None
        primary_key = b"".join([_b64url_decode(p) for p in key_parts])
        keys: dict[str, bytes] = {"primary": primary_key}
        for k, v in (pb.get("decrypt_keys") or {}).items():
            keys[k] = _b64url_decode(v)
        payload = pb.get("payload")
        iv      = pb.get("iv")
        if not payload or not iv:
            return None
        for _, key in keys.items():
            raw = _decrypt_aes_gcm(payload, key, iv)
            if raw:
                dec      = json.loads(raw)
                edge_url = _find_url(dec)
                if edge_url:
                    return {"url": edge_url, "subtitle_url": subtitle_url,
                            "video_id": video_id, "raw": dec}
        logger.warning("Moon decryption exhausted all keys")
        return None
    except Exception as e:
        logger.warning("Moon playback failed: %s", e)
        return None

# --- moon stream endpoint ---------------------------------------------------
@app.get("/api/moon/{mal_id}/{episode_num}")
async def get_moon_stream(mal_id: str, episode_num: str, request: Request):
    ck = f"moon:{mal_id}:{episode_num}"
    cached = _anime_cache.get(ck)
    if cached:
        return cached

    slug = await _resolve_real_slug(mal_id, episode_num)
    if not slug:
        raise HTTPException(404, "Moon: anime not found on 9animetv")

    servers = await _fetch_watch_servers(slug)
    moon_iframe = servers.get("moon")
    if not moon_iframe:
        raise HTTPException(404, "Moon: server not listed for this episode")

    video_id = _moon_video_id_from_iframe(moon_iframe)
    if not video_id:
        raise HTTPException(502, f"Moon: could not parse video_id from {moon_iframe}")

    backend = _stream_proxy_base(request)
    moon_url = (
        f"{backend}/proxy/moon/{quote(video_id, safe='')}/m3u8"
        f"?fast=1"
    )
    preload_url = (
        f"{backend}/proxy/moon/{quote(video_id, safe='')}/warm"
        f"?segments=2"
    )
    response = {
        "url":          moon_url,
        "m3u8_url":     moon_url,
        # Subtitle URL kept raw — its endpoint shape (398fitus timeslider) is
        # already CORS-friendly for the browser; proxying it through /proxy/vtt
        # would break the JSON-vs-VTT content type assumption.
        "subtitle_url": f"https://398fitus.com/api/videos/{video_id}/embed/timeslider",
        "video_id":     video_id,
        "server":       "moon",
        "preload_url":  preload_url,
    }
    _anime_cache[ck] = response
    return response

# --- hd1 stream endpoint ----------------------------------------------------
@app.get("/api/hd1/{mal_id}/{episode_num}")
async def get_hd1_stream(mal_id: str, episode_num: str, request: Request):
    ck = f"hd1:{mal_id}:{episode_num}"
    cached = _anime_cache.get(ck)
    if cached:
        return cached

    slug = await _resolve_real_slug(mal_id, episode_num)
    if not slug:
        raise HTTPException(404, "HD1: anime not found on 9animetv")

    servers = await _fetch_watch_servers(slug)
    hd_iframe = servers.get("hd")
    if not hd_iframe:
        raise HTTPException(404, "HD1: server not listed for this episode")

    mp4_url = await _fetch_hd_stream_url(hd_iframe)
    if not mp4_url:
        raise HTTPException(502, "HD1: workers.dev URL extraction from gogoanime failed")

    hd_url = _proxy_url(_stream_proxy_base(request), mp4_url, "/proxy/chunk") if CLOUDFLARE_PROXY_BASE else mp4_url
    response = {"url": hd_url, "server": "hd1"}
    _anime_cache[ck] = response
    return response

# ---------------------------------------------------------------------------
# STARTUP / SHUTDOWN
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def _startup():
    async def _warm():
        try:
            media = await _anilist_spring2026()
            await _anilist_ep_map()
            await _mal_seasonal()
            logger.info("Warmup done: %d AniList titles cached", len(media))
        except Exception as e:
            logger.warning("Warmup non-fatal: %s", e)
    asyncio.create_task(_warm())

@app.on_event("shutdown")
async def _shutdown():
    scraper.close()
    await _http.aclose()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
