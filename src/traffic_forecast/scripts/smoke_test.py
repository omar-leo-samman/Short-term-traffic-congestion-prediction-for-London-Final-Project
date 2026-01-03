"""Command-line smoke tests for Day 1.

Run from repo root:
    python -m traffic_forecast.scripts.smoke_test

This will:
- Load settings from environment/.env
- Call each provider once
- Print a short summary
- Write response JSON into sample_responses/ (without secrets)
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

from ..config import get_settings
from ..http import HttpClient, build_session
from ..clients.tomtom import TomTomClient
from ..clients.tfl import TflClient
from ..clients.dft import DftRoadTrafficClient


def _write_json(obj, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    settings = get_settings()

    http = HttpClient(
        session=build_session(user_agent=settings.user_agent),
        timeout_seconds=settings.http_timeout_seconds,
    )

    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Use a known central London coordinate for Day 1 smoke test.
    # (Trafalgar Square area)
    lat, lon = 51.5079, -0.1281

    # TomTom
    tomtom = TomTomClient(api_key=settings.tomtom_api_key, http=http)
    tomtom_payload = tomtom.flow_segment_data(lat=lat, lon=lon, zoom=10)
    tomtom_metrics = tomtom.parse_metrics(tomtom_payload)
    print("TomTom Flow Segment Data metrics:", tomtom_metrics)
    _write_json(tomtom_payload, Path("sample_responses") / f"tomtom_flow_segment_{now}.json")

    # TfL
    tfl = TflClient(app_key=settings.tfl_app_key, app_id=settings.tfl_app_id, http=http)
    roads = tfl.list_roads()
    _write_json(roads, Path("sample_responses") / f"tfl_roads_{now}.json")
    if isinstance(roads, list) and len(roads) > 0 and isinstance(roads[0], dict) and "id" in roads[0]:
        road_id = str(roads[0]["id"])
        disruptions = tfl.road_disruptions(road_id)
        _write_json(disruptions, Path("sample_responses") / f"tfl_disruptions_{road_id}_{now}.json")
        print(f"TfL disruptions fetched for road id={road_id!r}")
    else:
        print("TfL roads response did not match the expected list-of-dicts shape.")

    # DfT
    dft = DftRoadTrafficClient(http=http)
    local_auth = dft.list_local_authorities(page_size=5, page_number=1)
    _write_json(local_auth, Path("sample_responses") / f"dft_local_authorities_{now}.json")
    count_points = dft.list_count_points(page_size=5, page_number=1)
    _write_json(count_points, Path("sample_responses") / f"dft_count_points_{now}.json")
    print("DfT local authorities and count points fetched.")

    print("\nSmoke tests completed. Check sample_responses/ for output JSON.")


if __name__ == "__main__":
    main()
