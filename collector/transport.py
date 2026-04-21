import json
from dataclasses import dataclass
from time import sleep, time
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener


@dataclass(frozen=True)
class ResponseMeta:
    method: str
    path: str
    query: dict[str, Any]
    status: int
    fetched_at: float


@dataclass(frozen=True)
class TransportResponse:
    status: int
    text: str
    json_data: Any
    meta: ResponseMeta


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _default_opener(request: Request) -> Any:
    return build_opener(_NoRedirectHandler).open(request)


class GetOnlyTransport:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        throttle_seconds: float = 0.5,
        opener: Callable[[Request], Any] | None = None,
        sleeper: Callable[[float], None] = sleep,
        clock: Callable[[], float] = time,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.throttle_seconds = throttle_seconds
        self.opener = _default_opener if opener is None else opener
        self.sleeper = sleeper
        self.clock = clock

    def request(
        self,
        method: str,
        path: str,
        query: Mapping[str, Any] | None = None,
    ) -> TransportResponse:
        if method != "GET":
            raise ValueError("Only GET is allowed for Crawlab discovery")

        normalized_query = dict(query or {})
        url = f"{self.base_url}{path}"
        if normalized_query:
            url = f"{url}?{urlencode(normalized_query)}"

        request = Request(
            url,
            headers={"Authorization": self.api_key, "Accept": "application/json"},
            method="GET",
        )

        if self.throttle_seconds > 0:
            self.sleeper(self.throttle_seconds)

        try:
            with self.opener(request) as response:
                text = response.read().decode("utf-8")
                status = getattr(response, "status", 200)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"GET {path} failed with status {exc.code}: {body[:200]}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"GET {path} failed: {exc.reason}") from exc

        try:
            json_data = json.loads(text) if text else None
        except json.JSONDecodeError:
            json_data = None

        return TransportResponse(
            status=status,
            text=text,
            json_data=json_data,
            meta=ResponseMeta(
                method="GET",
                path=path,
                query=normalized_query,
                status=status,
                fetched_at=self.clock(),
            ),
        )

    def get(
        self,
        path: str,
        query: Mapping[str, Any] | None = None,
    ) -> TransportResponse:
        return self.request("GET", path, query)
