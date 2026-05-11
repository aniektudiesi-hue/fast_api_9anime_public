import sys
import unittest

from curl_cffi import requests as cffi_requests
from fastapi.testclient import TestClient


class FakeResponse:
    status_code = 200
    text = ""


class BootstrapSession:
    def get(self, *args, **kwargs):
        return FakeResponse()

    def close(self):
        pass


if "main" not in sys.modules:
    _real_session = cffi_requests.Session
    cffi_requests.Session = BootstrapSession
    try:
        import main
    finally:
        cffi_requests.Session = _real_session
else:
    import main


class BackendContractTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)

    def test_health_response_shape(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertIsInstance(payload["cached_keys"], list)

    def test_openapi_keeps_frontend_routes_available(self):
        response = self.client.get("/openapi.json")

        self.assertEqual(response.status_code, 200)
        paths = response.json()["paths"]
        expected_paths = {
            "/api/v1/banners",
            "/home/thumbnails",
            "/home/recently-added",
            "/home/top-rated",
            "/search/{query}",
            "/suggest/{query}",
            "/anime/episode/{mal_id}",
            "/api/stream/{mal_id}/{episode_num}",
            "/api/moon/{mal_id}/{episode_num}",
            "/api/hd1/{mal_id}/{episode_num}",
            "/auth/register",
            "/auth/login",
            "/auth/me",
            "/user/history",
            "/user/watchlist",
            "/user/downloads",
            "/health",
        }

        self.assertTrue(expected_paths.issubset(set(paths)))

    def test_config_preserves_cloudflare_worker_default(self):
        self.assertEqual(
            main.CLOUDFLARE_PROXY_BASE,
            "https://anime-tv-stream-proxy.kamuri-anime.workers.dev",
        )

    def test_moon_proxy_policy_uses_desktop_chrome_headers(self):
        headers, impersonate = main._proxy_call_for(
            "https://r66nv9ed.com/videos/example/segment.ts"
        )

        self.assertEqual(headers["Origin"], "https://bysesayeveum.com")
        self.assertEqual(headers["Referer"], "https://bysesayeveum.com/")
        self.assertIn("Windows NT 10.0", headers["User-Agent"])
        self.assertEqual(impersonate, "chrome120")

    def test_moon_endpoint_returns_worker_m3u8_without_network(self):
        mal_id = "900001"
        episode = "2"
        cache_key = f"moon:{mal_id}:{episode}"
        main._anime_cache.pop(cache_key, None)

        async def fake_resolve_real_slug(received_mal_id, received_episode):
            self.assertEqual(received_mal_id, mal_id)
            self.assertEqual(received_episode, episode)
            return "classroom-of-the-elite-iv-episode-2"

        async def fake_fetch_watch_servers(slug):
            self.assertEqual(slug, "classroom-of-the-elite-iv-episode-2")
            return {"moon": "https://bysesayeveum.com/e/fakeVideo_123"}

        def fake_moon_fetch_playback_sync(video_id):
            self.assertEqual(video_id, "fakeVideo_123")
            return {
                "video_id": video_id,
                "subtitle_url": "https://398fitus.com/api/subtitle?id=fakeVideo_123",
            }

        real_resolve = main._resolve_real_slug
        real_fetch_servers = main._fetch_watch_servers
        real_fetch_playback = main._moon_fetch_playback_sync
        main._resolve_real_slug = fake_resolve_real_slug
        main._fetch_watch_servers = fake_fetch_watch_servers
        main._moon_fetch_playback_sync = fake_moon_fetch_playback_sync
        try:
            response = self.client.get(f"/api/moon/{mal_id}/{episode}")
        finally:
            main._resolve_real_slug = real_resolve
            main._fetch_watch_servers = real_fetch_servers
            main._moon_fetch_playback_sync = real_fetch_playback
            main._anime_cache.pop(cache_key, None)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["server"], "moon")
        self.assertEqual(payload["video_id"], "fakeVideo_123")
        self.assertEqual(
            payload["url"],
            f"{main.CLOUDFLARE_PROXY_BASE}/proxy/moon/fakeVideo_123/m3u8",
        )
        self.assertEqual(
            payload["subtitle_url"],
            "https://398fitus.com/api/subtitle?id=fakeVideo_123",
        )


if __name__ == "__main__":
    unittest.main()
