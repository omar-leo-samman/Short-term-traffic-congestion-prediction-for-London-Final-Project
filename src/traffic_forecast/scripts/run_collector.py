"""Run the data collector (Day 2)."""

from __future__ import annotations

from pathlib import Path

from ..config import get_settings
from ..http import HttpClient, build_session
from ..clients.tomtom import TomTomClient
from ..clients.tfl import TflClient
from ..clients.dft import DftRoadTrafficClient
from ..data.collector import Collector, CollectorConfig
from ..data.points import LondonBBox


def main() -> None:
    s = get_settings()
    http = HttpClient(session=build_session(user_agent=s.user_agent), timeout_seconds=s.http_timeout_seconds)

    tomtom = TomTomClient(api_key=s.tomtom_api_key, http=http)
    tfl = TflClient(app_key=s.tfl_app_key, app_id=s.tfl_app_id, http=http)
    dft = DftRoadTrafficClient(http=http)

    cfg = CollectorConfig(
        num_points=s.collection_num_points,
        interval_seconds=s.collection_interval_seconds,
        duration_minutes=s.collection_duration_minutes,
        out_dir=Path("data"),
        london_bbox=LondonBBox(
            min_lat=s.london_bbox_min_lat,
            max_lat=s.london_bbox_max_lat,
            min_lon=s.london_bbox_min_lon,
            max_lon=s.london_bbox_max_lon,
        ),
    )
    Collector(tomtom=tomtom, tfl=tfl, dft=dft, cfg=cfg).run()


if __name__ == "__main__":
    main()
