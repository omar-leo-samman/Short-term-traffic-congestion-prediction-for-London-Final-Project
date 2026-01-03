"""UK Department for Transport (DfT) Road Traffic API client.

Notes:
- The Road Traffic API is **not authenticated**.
- The base URL for endpoints is: https://roadtraffic.dft.gov.uk/api/
- Endpoints are paginated using query parameters like:
  - page[size]
  - page[number]
- Some endpoints allow filters like:
  - filter[name]
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..http import ApiError, HttpClient, JsonType, request_json


class DftRoadTrafficClient:
    def __init__(self, *, http: HttpClient, base_url: str = "https://roadtraffic.dft.gov.uk/api") -> None:
        self.http = http
        self.base_url = base_url.rstrip("/")

    def list_count_points(self, *, page_size: int = 5, page_number: int = 1) -> JsonType:
        url = f"{self.base_url}/count-points"
        params = {
            "page[size]": page_size,
            "page[number]": page_number,
        }
        return request_json(self.http, url, params=params)

    def list_local_authorities(self, *, page_size: int = 50, page_number: int = 1, name_filter: Optional[str] = None) -> JsonType:
        url = f"{self.base_url}/local-authorities"
        params: Dict[str, Any] = {
            "page[size]": page_size,
            "page[number]": page_number,
        }
        if name_filter:
            params["filter[name]"] = name_filter
        return request_json(self.http, url, params=params)
