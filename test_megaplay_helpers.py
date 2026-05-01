import unittest

from curl_cffi import requests as cffi_requests


class FakeResponse:
    def __init__(self, status_code=200, text="", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data

    def json(self):
        return self._json_data


class BootstrapSession:
    def get(self, *args, **kwargs):
        return FakeResponse()

    def close(self):
        pass


_real_session = cffi_requests.Session
cffi_requests.Session = BootstrapSession
try:
    import main
finally:
    cffi_requests.Session = _real_session

from main import (
    MEGAPLAY_BASE,
    VIDWISH_BASE,
    VIDWISH_SOURCES_EP,
    MegaPlayScraper,
    _proxy_call_for,
    extract_megaplay_iframe_url,
    extract_player_data_attrs,
    extract_vidwish_source_id,
)


class MegaplayHelperTests(unittest.TestCase):
    def test_extracts_vidwish_iframe_from_megaplay_wrapper(self):
        html = """
        <html><body>
          <iframe src="https://vidwish.live/stream/s-2/114619/sub" allowfullscreen></iframe>
        </body></html>
        """

        self.assertEqual(
            extract_megaplay_iframe_url(html, "https://megaplay.buzz/stream/mal/51180/1/sub"),
            "https://vidwish.live/stream/s-2/114619/sub",
        )

    def test_extracts_vidwish_data_id(self):
        html = """
        <div class="fix-area" id="megaplay-player" data-id="16359"
             data-realid="114619" data-mediaid="467"></div>
        """

        self.assertEqual(extract_vidwish_source_id(html), "16359")
        attrs = extract_player_data_attrs(html)
        self.assertEqual(attrs["realid"], "114619")
        self.assertEqual(attrs["mediaid"], "467")

    def test_parses_vidwish_sources_and_tracks(self):
        payload = {
            "sources": {"file": "https://fxpy7.watching.onl/anime/hash/master.m3u8"},
            "tracks": [
                {
                    "file": "https://fxpy7.watching.onl/anime/hash/eng.vtt",
                    "label": "English",
                    "kind": "captions",
                    "default": True,
                },
                {
                    "file": "https://fxpy7.watching.onl/anime/hash/thumbs.vtt",
                    "label": "Thumbnails",
                    "kind": "thumbnails",
                },
            ],
            "server": 4,
        }

        stream = MegaPlayScraper._parse(
            payload,
            "51180",
            "1",
            internal_id="16359",
            real_id="114619",
            media_id="467",
            fallback_url="https://megaplay.buzz/stream/mal/51180/1/sub",
        )

        self.assertEqual(stream.m3u8_url, "https://fxpy7.watching.onl/anime/hash/master.m3u8")
        self.assertEqual(stream.subtitles[0].label, "English")
        self.assertTrue(stream.subtitles[0].default)
        self.assertEqual(stream.server_id, 4)

    def test_resolves_megaplay_wrapper_through_vidwish_sources(self):
        class FakeSession:
            def __init__(self):
                self.calls = []

            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                if url == f"{MEGAPLAY_BASE}/stream/mal/51180/1/sub":
                    return FakeResponse(
                        text='<iframe src="https://vidwish.live/stream/s-2/114619/sub"></iframe>'
                    )
                if url == f"{VIDWISH_BASE}/stream/s-2/114619/sub":
                    return FakeResponse(
                        text='<div id="megaplay-player" data-id="16359" '
                             'data-realid="114619" data-mediaid="467"></div>'
                    )
                if url == f"{VIDWISH_SOURCES_EP}?id=16359&id=16359":
                    return FakeResponse(
                        json_data={
                            "sources": {"file": "https://fxpy7.watching.onl/anime/hash/master.m3u8"},
                            "tracks": [
                                {
                                    "file": "https://fxpy7.watching.onl/anime/hash/eng.vtt",
                                    "label": "English",
                                    "kind": "captions",
                                }
                            ],
                        }
                    )
                raise AssertionError(f"Unexpected URL: {url}")

        scraper = MegaPlayScraper.__new__(MegaPlayScraper)
        scraper.timeout = 1
        scraper.max_retries = 1
        scraper.session = FakeSession()

        stream = scraper.get_stream("51180", "1", "sub")

        self.assertEqual(stream.m3u8_url, "https://fxpy7.watching.onl/anime/hash/master.m3u8")
        self.assertEqual(stream.iframe_url, "https://megaplay.buzz/stream/mal/51180/1/sub")
        self.assertEqual(stream.subtitles[0].file, "https://fxpy7.watching.onl/anime/hash/eng.vtt")

        source_call_url, source_call = scraper.session.calls[2]
        self.assertEqual(source_call_url, f"{VIDWISH_SOURCES_EP}?id=16359&id=16359")
        self.assertEqual(source_call["headers"]["Origin"], VIDWISH_BASE)
        self.assertEqual(source_call["headers"]["Referer"], f"{VIDWISH_BASE}/stream/s-2/114619/sub")
        self.assertEqual(source_call["headers"]["X-Requested-With"], "XMLHttpRequest")

    def test_vidwish_cdn_proxy_headers_use_vidwish_referer(self):
        headers, impersonate = _proxy_call_for("https://fxpy7.watching.onl/anime/hash/master.m3u8")

        self.assertEqual(headers["Origin"], VIDWISH_BASE)
        self.assertEqual(headers["Referer"], f"{VIDWISH_BASE}/")
        self.assertEqual(impersonate, "chrome131_android")

    def test_vidwish_cinewave_proxy_headers_use_vidwish_referer(self):
        headers, impersonate = _proxy_call_for("https://s2.cinewave2.site/anime/hash/index-f1.m3u8")

        self.assertEqual(headers["Origin"], VIDWISH_BASE)
        self.assertEqual(headers["Referer"], f"{VIDWISH_BASE}/")
        self.assertEqual(impersonate, "chrome131_android")


if __name__ == "__main__":
    unittest.main()
