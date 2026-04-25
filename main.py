"""
RO-ANIME Backend v3
"""
from __future__ import annotations
import asyncio, base64, json, logging, os, re, time, sqlite3, hashlib, secrets, uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import quote, quote_plus, unquote, urljoin, urlparse

import httpx
from cachetools import TTLCache
from curl_cffi import requests as cffi_requests
from fastapi import FastAPI, HTTPException, Query, Request, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, StreamingResponse

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
# MEGAPLAY SCRAPER
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
        
        status = m.get('status')
        total_episodes = m.get('episodes')
        next_airing = m.get('nextAiringEpisode')
        
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
# DATABASE  (SQLite – single file, lives next to main.py)
# ---------------------------------------------------------------------------
_DB_PATH = Path(__file__).parent / "ro_anime_users.db"

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

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
    CREATE INDEX IF NOT EXISTS idx_hist_user ON watch_history(user_id, watched_at DESC);
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
    conn.commit()
    conn.close()

def _db_migrate() -> None:
    """Add new columns / constraints to existing databases gracefully."""
    conn = _db()
    # Add playback_pos if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(watch_history)")}
    if "playback_pos" not in cols:
        conn.execute("ALTER TABLE watch_history ADD COLUMN playback_pos REAL DEFAULT 0")
        conn.commit()
    conn.close()

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

# ---------------------------------------------------------------------------
# AUTH ENDPOINTS
# ---------------------------------------------------------------------------
@app.post("/auth/register")
async def auth_register(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if len(username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")
    if len(password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")
    token = secrets.token_hex(32)
    try:
        conn = _db()
        conn.execute(
            "INSERT INTO users (username, password_hash, token) VALUES (?,?,?)",
            (username, _hash_pw(password), token)
        )
        conn.commit()
        conn.close()
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Username already taken")
    return {"token": token, "username": username}

@app.post("/auth/login")
async def auth_login(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    conn = _db()
    row  = conn.execute(
        "SELECT id, token FROM users WHERE username=? AND password_hash=?",
        (username, _hash_pw(password))
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Wrong username or password")
    return {"token": row["token"], "username": username}

@app.get("/auth/me")
async def auth_me(request: Request):
    return _get_current_user(request)

# ---------------------------------------------------------------------------
# HISTORY
# ---------------------------------------------------------------------------
@app.post("/user/history")
async def history_add(request: Request):
    user = _get_current_user(request)
    d    = await request.json()
    conn = _db()
    # INSERT OR REPLACE keeps one row per (user, anime, episode) — updates on re-watch
    conn.execute(
        """INSERT INTO watch_history (user_id, mal_id, title, image_url, episode, playback_pos, watched_at)
           VALUES (?,?,?,?,?,?,strftime('%s','now'))
           ON CONFLICT(user_id, mal_id, episode)
           DO UPDATE SET playback_pos=excluded.playback_pos,
                         watched_at=excluded.watched_at,
                         title=excluded.title,
                         image_url=excluded.image_url""",
        (user["id"], d["mal_id"], d["title"], d.get("image_url",""),
         int(d["episode"]), float(d.get("playback_pos", 0)))
    )
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/user/history")
async def history_get(request: Request):
    user = _get_current_user(request)
    conn = _db()
    rows = conn.execute(
        """SELECT mal_id, title, image_url, episode, playback_pos, watched_at
           FROM watch_history WHERE user_id=?
           ORDER BY watched_at DESC LIMIT 200""",
        (user["id"],)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.delete("/user/history")
async def history_clear(request: Request):
    user = _get_current_user(request)
    conn = _db()
    conn.execute("DELETE FROM watch_history WHERE user_id=?", (user["id"],))
    conn.commit(); conn.close()
    return {"ok": True}

# ---------------------------------------------------------------------------
# WATCHLIST
# ---------------------------------------------------------------------------
@app.post("/user/watchlist")
async def watchlist_add(request: Request):
    user = _get_current_user(request)
    d    = await request.json()
    conn = _db()
    conn.execute(
        """INSERT OR REPLACE INTO watchlist (user_id, mal_id, title, image_url, episodes)
           VALUES (?,?,?,?,?)""",
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
        """INSERT OR REPLACE INTO downloads
           (user_id, mal_id, title, episode, image_url, idb_key, size_bytes, has_subs)
           VALUES (?,?,?,?,?,?,?,?)""",
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

# --- root -------------------------------------------------------------------
@app.get("/")
async def root():
    return HTMLResponse(content=_HTML_PATH.read_text(encoding="utf-8"), media_type="text/html")

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
        total  = m.get("episodes") or 0
        nxt    = (m.get("nextAiringEpisode") or {}).get("episode", 0)
        aired  = max(nxt - 1, 0) if nxt else 0
        result.append({
            "anime_id":      str(m.get("idMal", m["id"])),
            "title":         title,
            "img_url":       img,
            "banner":        banner,
            "cover":         cover,
            "episode_count": total or aired,
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
_DL_CONCURRENCY = 30      # simultaneous segment fetches per task
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

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(
                max_connections=_DL_CONCURRENCY + 10,
                max_keepalive_connections=_DL_CONCURRENCY,
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
# MOON + HD1  — combined Playwright scraper (one browser visit per slug)
# ---------------------------------------------------------------------------
_scrape_locks: dict[str, asyncio.Lock] = {}

def _get_scrape_lock(slug: str) -> asyncio.Lock:
    if slug not in _scrape_locks:
        _scrape_locks[slug] = asyncio.Lock()
    return _scrape_locks[slug]

async def _combined_scrape(slug: str) -> tuple[str | None, str | None]:
    """Returns (moon_embed_url, hd1_mp4_url). Cached; one browser per slug max."""
    ck = f"scrape:{slug}"
    if ck in _anime_cache:
        return _anime_cache[ck]
    lock = _get_scrape_lock(slug)
    async with lock:
        if ck in _anime_cache:
            return _anime_cache[ck]
        result = await _do_combined_scrape(slug)
        _anime_cache[ck] = result
        return result

async def _do_combined_scrape(slug: str) -> tuple[str | None, str | None]:
    try:
        from playwright.async_api import async_playwright
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("playwright/bs4 missing — Moon+HD1 disabled")
        return None, None

    page_url = f"https://9animetv.org.lv/watch/{slug}"
    logger.info("Combined scrape: %s", page_url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx  = await browser.new_context(user_agent=MOON_UA)
        page = await ctx.new_page()

        hd1_url: list[str | None] = [None]

        def _is_hd1(url: str) -> bool:
            return "workers.dev" in url and ("url=" in url or ".mp4" in url.lower())

        async def _on_req(req):
            if not hd1_url[0] and _is_hd1(req.url):
                hd1_url[0] = req.url
        async def _on_resp(resp):
            if not hd1_url[0] and _is_hd1(resp.url):
                hd1_url[0] = resp.url

        page.on("request",  _on_req)
        page.on("response", _on_resp)

        moon_embed_url: str | None = None
        try:
            await page.goto(page_url, wait_until="domcontentloaded", timeout=60_000)

            try:
                await page.wait_for_selector(
                    'button:has-text("Moon"), option:has-text("Moon")', timeout=10_000)
            except Exception:
                pass

            try:
                soup    = BeautifulSoup(await page.content(), "html.parser")
                encoded = None
                for btn in soup.find_all("button"):
                    if "moon" in btn.get_text("", strip=True).lower():
                        encoded = btn.get("data-option-id")
                        break
                if not encoded:
                    sel = soup.find("select")
                    if sel:
                        opt = sel.find("option", string=re.compile(r"moon", re.I))
                        if opt:
                            encoded = opt.get("value")
                if encoded:
                    decoded = base64.b64decode(encoded + "==").decode("utf-8")
                    m = re.search(r'src=["\']([^"\']+)["\']', decoded)
                    moon_embed_url = m.group(1) if m else None
            except Exception as e:
                logger.warning("Moon embed parse: %s", e)

            if not hd1_url[0]:
                for sel in ('video', '.jw-display-icon-container', '[class*="play-btn"]',
                            '[class*="bigPlayBtn"]', '.vjs-big-play-button'):
                    try:
                        await page.click(sel, timeout=2_000)
                        break
                    except Exception:
                        continue

            for _ in range(25):
                if hd1_url[0]:
                    break
                await asyncio.sleep(1)

        except Exception as e:
            logger.warning("Combined scrape error: %s", e)
        finally:
            await browser.close()

        logger.info("Scrape done  moon=%s  hd1=%s", bool(moon_embed_url), bool(hd1_url[0]))
        return moon_embed_url, hd1_url[0]

async def _moon_get_video_id(embed_url: str) -> str | None:
    """Playwright: load embed page, use expect_response to get video ID instantly."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx  = await browser.new_context(user_agent=MOON_UA)
        page = await ctx.new_page()
        await page.set_extra_http_headers({
            "Referer": "https://9anime.org.lv/",
            "Origin":  "https://9anime.org.lv",
        })
        try:
            async with page.expect_response(
                lambda r: "api/videos" in r.url and "embed/settings" in r.url,
                timeout=35_000
            ) as resp_info:
                await page.goto(embed_url, wait_until="domcontentloaded", timeout=60_000)
            resp  = await resp_info.value
            parts = resp.url.split("/")
            return parts[-3] if len(parts) >= 3 else None
        except Exception as e:
            logger.warning("Moon video ID failed: %s", e)
            return None
        finally:
            await browser.close()

# ---------------------------------------------------------------------------
# MOON CRYPTO HELPERS
# ---------------------------------------------------------------------------
def _b64url_decode(data: str) -> bytes:
    padding = '=' * (4 - len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)

def _decrypt_aes_gcm(payload_b64: str, key: bytes, iv_b64: str):
    try:
        from Crypto.Cipher import AES
        full      = _b64url_decode(payload_b64)
        iv        = _b64url_decode(iv_b64)
        cipher    = AES.new(key, AES.MODE_GCM, nonce=iv)
        decrypted = cipher.decrypt_and_verify(full[:-16], full[-16:])
        return decrypted.decode("utf-8")
    except Exception:
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
        resp = cffi_requests.post(url, headers=headers, json={}, impersonate="chrome136")
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
async def get_moon_stream(mal_id: str, episode_num: str):
    ck = f"moon:{mal_id}:{episode_num}"
    if ck in _anime_cache:
        return _anime_cache[ck]

    slug = await _moon_get_slug(mal_id, episode_num)
    if not slug:
        raise HTTPException(502, "Moon: could not build slug from Jikan")

    # Shared scrape with HD1 — one browser visit (full episode slug needed)
    moon_embed_url, _ = await _combined_scrape(slug)
    if not moon_embed_url:
        raise HTTPException(404, "Moon: server not found on 9anime page")

    video_id = await _moon_get_video_id(moon_embed_url)
    if not video_id:
        raise HTTPException(502, "Moon: could not intercept video ID")

    loop   = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _moon_fetch_playback_sync, video_id)
    if not result:
        raise HTTPException(502, "Moon: playback decryption failed")

    response = {
        "url":          result["url"],
        "subtitle_url": result["subtitle_url"],
        "video_id":     result["video_id"],
        "server":       "moon",
    }
    _anime_cache[ck] = response
    return response

# --- hd1 stream endpoint ----------------------------------------------------
@app.get("/api/hd1/{mal_id}/{episode_num}")
async def get_hd1_stream(mal_id: str, episode_num: str):
    ck = f"hd1:{mal_id}:{episode_num}"
    if ck in _anime_cache:
        return _anime_cache[ck]

    slug = await _moon_get_slug(mal_id, episode_num)
    if not slug:
        raise HTTPException(502, "HD1: could not build slug from Jikan")

    # Shared scrape with Moon — one browser visit (full episode slug needed)
    _, mp4_url = await _combined_scrape(slug)
    if not mp4_url:
        raise HTTPException(404, "HD1: workers.dev MP4 URL not found on page")

    response = {"url": mp4_url, "server": "hd1"}
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
