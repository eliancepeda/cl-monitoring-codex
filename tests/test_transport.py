import io
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError

from collector.transport import GetOnlyTransport


class FakeHttpResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class RawHttpResponse:
    def __init__(self, text, status=200):
        self.text = text
        self.status = status

    def read(self):
        return self.text.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TransportTests(unittest.TestCase):
    def test_get_builds_url_headers_and_parses_json(self):
        captured = {}

        def opener(request):
            captured["url"] = request.full_url
            captured["auth"] = request.headers["Authorization"]
            captured["accept"] = request.headers["Accept"]
            captured["method"] = request.get_method()
            return FakeHttpResponse({"status": "ok", "data": []})

        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=opener,
            sleeper=lambda _: None,
            clock=lambda: 1710000000.0,
        )

        response = transport.get("/api/projects", {"page": 1, "size": 10})

        self.assertEqual(
            captured["url"],
            "https://crawlab.example/api/projects?page=1&size=10",
        )
        self.assertEqual(captured["auth"], "secret-token")
        self.assertEqual(captured["accept"], "application/json")
        self.assertEqual(captured["method"], "GET")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.json_data, {"status": "ok", "data": []})
        self.assertEqual(response.meta.path, "/api/projects")

    def test_get_sets_json_data_to_none_for_invalid_json(self):
        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=lambda request: RawHttpResponse("not-json"),
            sleeper=lambda _: None,
            clock=lambda: 1710000000.0,
        )

        response = transport.get("/api/projects")

        self.assertEqual(response.status, 200)
        self.assertEqual(response.text, "not-json")
        self.assertIsNone(response.json_data)
        self.assertEqual(response.meta.path, "/api/projects")

    def test_request_rejects_non_get_methods(self):
        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=lambda request: FakeHttpResponse({"status": "ok"}),
            sleeper=lambda _: None,
        )

        with self.assertRaises(ValueError) as context:
            transport.request("POST", "/api/projects")

        self.assertEqual(str(context.exception), "Only GET is allowed for Crawlab discovery")

    def test_http_errors_do_not_echo_api_key(self):
        def opener(request):
            raise HTTPError(
                url=request.full_url,
                code=403,
                msg="Forbidden",
                hdrs=None,
                fp=io.BytesIO(b'{"error": "forbidden"}'),
            )

        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=opener,
            sleeper=lambda _: None,
        )

        with self.assertRaises(RuntimeError) as context:
            transport.get("/api/projects")

        self.assertIn("GET /api/projects failed with status 403", str(context.exception))
        self.assertNotIn("secret-token", str(context.exception))

    def test_default_opener_blocks_redirects_before_forwarding_auth(self):
        state = {"target_hits": 0, "target_auth": None}
        target_server = None

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header(
                    "Location",
                    f"http://127.0.0.1:{target_server.server_port}/target",
                )
                self.end_headers()

            def log_message(self, format, *args):
                pass

        class TargetHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                state["target_hits"] += 1
                state["target_auth"] = self.headers.get("Authorization")
                self.send_response(200)
                self.end_headers()

            def log_message(self, format, *args):
                pass

        redirect_server = HTTPServer(("127.0.0.1", 0), RedirectHandler)
        target_server = HTTPServer(("127.0.0.1", 0), TargetHandler)
        redirect_thread = threading.Thread(
            target=redirect_server.serve_forever,
            daemon=True,
        )
        target_thread = threading.Thread(
            target=target_server.serve_forever,
            daemon=True,
        )
        redirect_thread.start()
        target_thread.start()

        transport = GetOnlyTransport(
            base_url=f"http://127.0.0.1:{redirect_server.server_port}",
            api_key="secret-token",
            throttle_seconds=0,
            sleeper=lambda _: None,
        )

        try:
            with self.assertRaises(RuntimeError) as context:
                transport.get("/start")
        finally:
            redirect_server.shutdown()
            target_server.shutdown()
            redirect_server.server_close()
            target_server.server_close()
            redirect_thread.join(timeout=1)
            target_thread.join(timeout=1)

        self.assertIn("GET /start failed with status 302", str(context.exception))
        self.assertEqual(state["target_hits"], 0)
        self.assertIsNone(state["target_auth"])


if __name__ == "__main__":
    unittest.main()
