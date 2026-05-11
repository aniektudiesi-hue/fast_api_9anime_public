# RO-ANIME Backend Architecture

This backend is currently a working monolith centered on `main.py`. The safest
path to a production-grade layout is incremental extraction: move one stable
concern at a time, keep endpoint contracts unchanged, and add tests before each
behavioral refactor.

## Current Runtime Shape

- `main.py` owns FastAPI startup, route registration, scraping, proxying, auth,
  user library storage, downloads metadata, and in-memory caches.
- The database layer supports PostgreSQL through `DATABASE_URL` and falls back
  to SQLite locally or on a Render disk.
- MegaPlay, Moon, and HD1 are intentionally sensitive to headers, referers, and
  proxy routing. Treat these code paths as contract-protected.
- The Cloudflare Worker URL is the active stream proxy base for playlists,
  chunks, captions, and HD media.

## First Extraction Boundary

- `app/config.py` now owns runtime settings while preserving the legacy defaults
  from `main.py`.
- `main.py` still exposes the same constants so existing helpers and tests do
  not break.
- Future modules should import configuration from `app.config` instead of adding
  more top-level constants to `main.py`.

## Protected Endpoint Contracts

These routes must remain stable for the frontend and existing apps:

- `GET /api/v1/banners`
- `GET /home/thumbnails`
- `GET /home/recently-added`
- `GET /home/top-rated`
- `GET /search/{query}`
- `GET /suggest/{query}`
- `GET /anime/episode/{mal_id}`
- `GET /api/stream/{mal_id}/{episode_num}`
- `GET /api/moon/{mal_id}/{episode_num}`
- `GET /api/hd1/{mal_id}/{episode_num}`
- `POST /auth/register`
- `POST /auth/login`
- `GET /auth/me`
- `POST /user/history`
- `GET /user/history`
- `DELETE /user/history`
- `POST /user/watchlist`
- `GET /user/watchlist`
- `DELETE /user/watchlist/{mal_id}`
- `POST /user/downloads`
- `GET /user/downloads`
- `DELETE /user/downloads/{mal_id}/{episode}`
- `GET /health`

## Refactor Rules

1. Add or update tests before changing stream, auth, or database behavior.
2. Preserve response shapes unless the frontend is updated in the same change.
3. Keep Moon and MegaPlay header policies covered by tests.
4. Prefer extracting pure helpers first; delay database and async rewrites until
   contract tests cover the current behavior.
5. Keep Cloudflare Worker routing and backend fallback behavior separate.

## Recommended Next Phases

1. Extract pure models and stream dataclasses into `app/models/stream.py`.
2. Move database connection and schema initialization into `app/database/`.
3. Move auth/user routes into `app/routes/` behind the same paths.
4. Move MegaPlay, Moon, and HD1 logic into service modules with mocked tests.
5. Replace the SQLite-like database shim with SQLAlchemy only after user data
   migration is tested against both SQLite and PostgreSQL.
