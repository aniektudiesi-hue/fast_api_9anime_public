
from __future__ import annotations
import asyncio, logging, re, time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
from urllib.parse import quote, unquote, urljoin, urlparse

import httpx
from cachetools import TTLCache
from curl_cffi import requests as cffi_requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("ro-anime")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
MAL_CLIENT_ID = "b7718fb862ae30f4ba64bc9a4f90d2ea"
MAL_BASE      = "https://api.myanimelist.net/v2"
MAL_HEADERS   = {"X-MAL-CLIENT-ID": MAL_CLIENT_ID}
ANILIST_GQL   = "https://graphql.anilist.co"
MEGAPLAY_BASE = "https://megaplay.buzz"
SOURCES_EP    = f"{MEGAPLAY_BASE}/stream/getSources"
IMPERSONATE   = "chrome131_android"
ANDROID_UA    = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Mobile Safari/537.36"
)

_SEASONAL_FIELDS = (
    "id,title,main_picture,alternative_titles,"
    "num_episodes,start_season,mean,status,rank,popularity"
)
_SEARCH_FIELDS = (
    "id,title,main_picture,alternative_titles,"
    "num_episodes,start_season,mean,status"
)

# AniList GraphQL — spring 2026 sorted by popularity
# bannerImage = wide landscape art (what Crunchyroll/HIDIVE/AnimePlanet reference)
# nextAiringEpisode gives currently-aired count for ongoing shows
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
PROXY_HEADERS = {
    "User-Agent": ANDROID_UA,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": MEGAPLAY_BASE,
    "Referer": f"{MEGAPLAY_BASE}/",
}

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
    mal_id: str = ""; episode_num: str = ""
    internal_id: str = ""; real_id: str = ""; media_id: str = ""

    def to_dict(self) -> dict:
        return {
            "m3u8_url": self.m3u8_url,
            "subtitles": [asdict(s) for s in self.subtitles],
            "intro":  asdict(self.intro)  if self.intro  else None,
            "outro":  asdict(self.outro)  if self.outro  else None,
            "server_id": self.server_id,
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
# SCRAPER
# ---------------------------------------------------------------------------
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
        attrs = dict(re.findall(r'\bdata-([\w-]+)\s*=\s*["\']([^"\']+)["\']', r.text))
        if "id" not in attrs:
            raise PlayerPageError(f"data-id missing ({len(r.text)} bytes)")
        return attrs

    def _fetch_sources(self, internal_id, stype):
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.session.get(SOURCES_EP,
                    params={"id": internal_id, "type": stype},
                    headers=API_HEADERS, impersonate=IMPERSONATE, timeout=self.timeout)
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
        data  = self._fetch_sources(attrs["id"], stream_type)
        return self._parse(data, mal_id, episode_num,
                           internal_id=attrs["id"],
                           real_id=attrs.get("realid", ""),
                           media_id=attrs.get("mediaid", ""))

    @staticmethod
    def _parse(data, mal_id, ep_num, internal_id, real_id, media_id):
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
        if line.startswith("#EXT-X-MEDIA"):
            out.append(_rewrite_attr_uri(line, base, backend, "/proxy/m3u8")); continue
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

# ---------------------------------------------------------------------------
# ANILIST SEASONAL  (primary source for banners + episode counts)
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
            _home_cache[ck] = []
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
        
        # New episode logic from FILE 2
        status = m.get("status")
        total_episodes = m.get("episodes")
        next_airing = m.get("nextAiringEpisode")
        
        if next_airing:
            count = next_airing['episode'] - 1
        elif status == 'FINISHED':
            count = total_episodes if total_episodes else 0
        elif status == 'RELEASING':
            count = total_episodes if total_episodes else 0
        elif status == 'NOT_YET_RELEASED':
            count = 0
        else:
            count = 0
            
        ep_map[int(mid)] = count
    _home_cache[ck] = ep_map
    return ep_map

# ---------------------------------------------------------------------------
# MAL SEASONAL  (for thumbnails / recently-added)
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
# INLINE FRONTEND
# ---------------------------------------------------------------------------
HTML_CONTENT = Path(__file__).parent.joinpath("index.html").read_text(encoding="utf-8")

# ---------------------------------------------------------------------------
# APP
# ---------------------------------------------------------------------------
app     = FastAPI(title="RO-ANIME v3")
scraper = MegaPlayScraper()

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=False,
                   allow_methods=["GET", "OPTIONS"],
                   allow_headers=["*"])

def _backend_base(req: Request) -> str:
    scheme = req.headers.get("x-forwarded-proto") or req.url.scheme
    host   = req.headers.get("x-forwarded-host")  or req.url.netloc
    return f"{scheme}://{host}"

# --- root -------------------------------------------------------------------
@app.get("/")
async def root():
    return HTMLResponse(content=HTML_CONTENT, media_type="text/html")

# --- banners  (AniList landscape — what streaming giants use) ---------------
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
        
        # New episode logic from FILE 2
        status = m.get("status")
        total_episodes = m.get("episodes")
        next_airing = m.get("nextAiringEpisode")
        
        if next_airing:
            count = next_airing['episode'] - 1
        elif status == 'FINISHED':
            count = total_episodes if total_episodes else 0
        elif status == 'RELEASING':
            count = total_episodes if total_episodes else 0
        elif status == 'NOT_YET_RELEASED':
            count = 0
        else:
            count = 0
            
        result.append({
            "anime_id":      str(m.get("idMal", m["id"])),
            "title":         title,
            "img_url":       img,
            "banner":        banner,
            "cover":         cover,
            "episode_count": count,
            "score":         round((m.get("averageScore") or 0) / 10, 1),
        })
    result = result[:10]
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
        ep_map  = await _anilist_ep_map()
    except Exception as e:
        raise HTTPException(502, f"Fetch failed: {e}")
    result = [_fmt_card(e["node"], ep_override=ep_map.get(int(e["node"].get("id", 0)), 0))
              for e in entries]
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
        ep_map  = await _anilist_ep_map()
    except Exception as e:
        raise HTTPException(502, f"Fetch failed: {e}")
    airing = [e for e in entries
              if e["node"].get("status") in ("currently_airing", "not_yet_aired")]
    result = [_fmt_card(e["node"], ep_override=ep_map.get(int(e["node"].get("id", 0)), 0))
              for e in airing[:30]]
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
                                  fields="id,title,main_picture,num_episodes,mean,alternative_titles")
        except Exception as e:
            raise HTTPException(502, f"MAL ranking failed: {e}")
        result = [_fmt_card(e["node"]) for e in data.get("data", [])]
        _home_cache[ck] = result
        return result

# --- search -----------------------------------------------------------------
@app.get("/search/{query}")
async def search_anime(query: str):
    ck = f"search:{query.lower()}"
    if ck in _search_cache:
        return {"results": _search_cache[ck]}
    try:
        data = await _mal_get("/anime", q=query, limit=25, fields=_SEARCH_FIELDS)
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"MAL {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, f"Search failed: {str(e)[:120]}")
    nodes = [e["node"] for e in data.get("data", [])]
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
    result = [_fmt_card(n) for n in nodes]
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
                              fields="id,title,alternative_titles")
    except Exception:
        return {"results": []}
    results = []
    for e in data.get("data", []):
        n  = e["node"]; alt = n.get("alternative_titles") or {}
        en = (alt.get("en") or "").strip()
        results.append({"title": en if en else n.get("title", ""),
                        "anime_id": str(n.get("id", ""))})
    _suggest_cache[ck] = results
    return {"results": results}

# ---------------------------------------------------------------------------
# ANI-EPISODE LOGIC (Integrated from FILE 2)
# ---------------------------------------------------------------------------
GQL_ANIME_EPS = """
query($malId: Int) {
  Media(idMal: $malId, type: ANIME) {
    status
    episodes
    nextAiringEpisode {
      episode
    }
  }
}
"""

async def _anilist_episodes_for(mal_id: int) -> int:
    """AniList per-anime query using new logic from FILE 2."""
    ck = f"al_eps_{mal_id}"
    if ck in _anime_cache:
        return _anime_cache[ck]
    try:
        r = await _http.post(ANILIST_GQL,
            json={"query": GQL_ANIME_EPS, "variables": {"malId": mal_id}},
            headers={"Content-Type": "application/json"})
        r.raise_for_status()
        data = r.json().get("data", {}).get("Media") or {}
        
        # Logic from FILE 2 get_aired_episodes
        status = data.get('status')
        total_episodes = data.get('episodes')
        next_airing = data.get('nextAiringEpisode')
        
        if next_airing:
            count = next_airing['episode'] - 1
        elif status == 'FINISHED':
            count = total_episodes if total_episodes else 0
        elif status == 'RELEASING':
            count = total_episodes if total_episodes else 0
        elif status == 'NOT_YET_RELEASED':
            count = 0
        else:
            count = 0
    except Exception as e:
        logger.warning("AniList per-anime eps failed mal_id=%s: %s", mal_id, e)
        count = 0
    _anime_cache[ck] = count
    return count


# --- episodes ---------------------------------------------------------------
@app.get("/anime/episode/{mal_id}")
async def get_episodes(mal_id: str):
    ck = f"episodes:{mal_id}"
    if ck in _anime_cache:
        return _anime_cache[ck]

    # Use the new logic to get episode count
    num_eps = await _anilist_episodes_for(int(mal_id))

    # Absolute floor — never return an empty episode list.
    if num_eps == 0:
        num_eps = 1
        logger.warning("Episode count unknown for mal_id=%s; defaulting to 1", mal_id)

    episodes = [{"episode_number": i} for i in range(1, num_eps + 1)]
    result   = {"anime_id": mal_id, "num_episodes": num_eps, "episodes": episodes}
    _anime_cache[ck] = result
    return result

# --- stream -----------------------------------------------------------------
@app.get("/api/stream/{mal_id}/{episode_num}")
async def get_stream(request: Request, mal_id: str, episode_num: str,
                     type: Literal["sub", "dub"] = "sub"):
    loop = asyncio.get_running_loop()
    try:
        data: StreamData = await loop.run_in_executor(
            None, scraper.get_stream, mal_id, episode_num, type)
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

    backend = _backend_base(request)
    result  = data.to_dict()
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
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=PROXY_HEADERS, impersonate=IMPERSONATE, timeout=15))
    except Exception as e:
        raise HTTPException(502, f"Upstream fetch failed: {e}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    rewritten = rewrite_m3u8(r.text, url, _backend_base(request))
    return Response(content=rewritten, media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"})

# --- chunk proxy ------------------------------------------------------------
@app.get("/proxy/chunk")
async def proxy_chunk(src: str = Query(...)):
    url = unquote(src)
    if not _is_valid_url(url): raise HTTPException(400, "Invalid src URL")
    loop = asyncio.get_running_loop()
    try:
        r = await loop.run_in_executor(None, lambda: cffi_requests.get(
            url, headers=PROXY_HEADERS, impersonate=IMPERSONATE, timeout=30))
    except Exception as e:
        raise HTTPException(502, f"Upstream failed: {type(e).__name__}")
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"CDN {r.status_code}")
    body = r.content
    if not body: raise HTTPException(502, "Empty body from upstream")
    return Response(content=body, media_type=_detect_mime(url, body),
                    headers={"Cache-Control": "public, max-age=3600",
                             "Access-Control-Allow-Origin": "*",
                             "Accept-Ranges": "bytes"})

# --- vtt proxy --------------------------------------------------------------
@app.get("/proxy/vtt")
async def proxy_vtt(src: str = Query(...)):
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
    return Response(content=r.text, media_type="text/vtt; charset=utf-8",
                    headers={"Cache-Control": "public, max-age=3600",
                             "Access-Control-Allow-Origin": "*"})

# --- image proxy (browser loads images direct from CDN; this is a fallback) -
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

# --- startup / shutdown -----------------------------------------------------
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
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
