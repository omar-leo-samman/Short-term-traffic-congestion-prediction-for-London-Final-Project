"""Shared HTTP utilities (requests.Session + retries + JSON parsing)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


JsonType = Union[Dict[str, Any], list]


class ApiError(RuntimeError):
    """Raised when an API request fails."""


@dataclass(frozen=True)
class HttpClient:
    """A thin wrapper around requests.Session with sensible defaults."""

    session: requests.Session
    timeout_seconds: float


def build_session(*, user_agent: str, total_retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    """Create a configured requests session with retries.

    Retries are useful for:
    - transient network issues
    - server 5xx errors
    - rate limiting (429) with Retry-After headers
    """

    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session


def request_json(
    http: HttpClient,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> JsonType:
    """GET a URL and parse JSON response."""

    try:
        resp = http.session.get(url, params=params, headers=headers, timeout=http.timeout_seconds)
    except requests.RequestException as exc:
        raise ApiError(f"Request failed for {url}: {exc}") from exc

    if resp.status_code >= 400:
        # Keep error messages short to avoid leaking data in logs.
        body_preview = (resp.text or "")[:500]
        raise ApiError(f"HTTP {resp.status_code} for {url}. Body: {body_preview}")

    try:
        return resp.json()
    except ValueError as exc:
        body_preview = (resp.text or "")[:500]
        raise ApiError(f"Failed to parse JSON for {url}. Body: {body_preview}") from exc
