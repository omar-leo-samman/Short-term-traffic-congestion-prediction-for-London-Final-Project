"""Transport for London (TfL) Unified API client.

We keep it minimal for Day 1 smoke tests:
- List roads
- Get disruptions for a road

Authentication:
- Append app_key as a query parameter.
- Some older docs mention app_id, but it may not be required anymore.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from ..http import ApiError, HttpClient, JsonType, request_json


class TflClient:
    def __init__(
        self,
        *,
        app_key: str,
        http: HttpClient,
        app_id: Optional[str] = None,
        base_url: str = "https://api.tfl.gov.uk",
    ) -> None:
        self.app_key = app_key
        self.app_id = app_id
        self.http = http
        self.base_url = base_url.rstrip("/")

    def _auth_params(self) -> dict:
        params = {"app_key": self.app_key}
        if self.app_id:
            params["app_id"] = self.app_id
        return params

    def list_roads(self) -> JsonType:
        """List TfL-managed roads."""
        url = f"{self.base_url}/Road"
        params = self._auth_params()
        return request_json(self.http, url, params=params)

    def road_disruptions(self, road_id: str) -> JsonType:
        """Get active disruptions for a road id (e.g., 'a2', 'a1')."""
        road_id = road_id.strip()
        if not road_id:
            raise ValueError("road_id must be non-empty")
        url = f"{self.base_url}/Road/{road_id}/Disruption"
        params = self._auth_params()
        return request_json(self.http, url, params=params)

    def road_status(self, road_id: str) -> JsonType:
        """Get status details for a road id."""
        road_id = road_id.strip()
        if not road_id:
            raise ValueError("road_id must be non-empty")
        url = f"{self.base_url}/Road/{road_id}"
        params = self._auth_params()
        return request_json(self.http, url, params=params)
