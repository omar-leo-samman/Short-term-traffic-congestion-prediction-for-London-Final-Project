"""DfT count points selection helpers.

Goal:
- Build a stable list of monitoring points (lat/lon) around Greater London.
- Keep this *simple* for a 1-week project: use an approximate bounding box.

Outputs:
- data/metadata/points.csv
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from ..clients.dft import DftRoadTrafficClient


@dataclass(frozen=True)
class LondonBBox:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    def contains(self, lat: float, lon: float) -> bool:
        return (self.min_lat <= lat <= self.max_lat) and (self.min_lon <= lon <= self.max_lon)


def _extract_attr(item: Dict, key: str) -> Optional[object]:
    attrs = item.get("attributes")
    if isinstance(attrs, dict):
        return attrs.get(key)
    return None


def _extract_float(item: Dict, keys: Iterable[str]) -> Optional[float]:
    for k in keys:
        v = _extract_attr(item, k)
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            continue
    return None


def _extract_str(item: Dict, keys: Iterable[str]) -> str:
    for k in keys:
        v = _extract_attr(item, k)
        if v is None:
            continue
        return str(v)
    return ""


def fetch_count_points_in_bbox(
    dft: DftRoadTrafficClient,
    *,
    bbox: LondonBBox,
    target_n: int,
    page_size: int = 200,
    max_pages: int = 200,
    seed: int = 42,
) -> pd.DataFrame:
    rows: List[Dict] = []
    page = 1
    while page <= max_pages and len(rows) < (target_n * 5):
        payload = dft.list_count_points(page_size=page_size, page_number=page)
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or len(data) == 0:
            break

        for item in data:
            if not isinstance(item, dict):
                continue
            lat = _extract_float(item, ("latitude", "lat"))
            lon = _extract_float(item, ("longitude", "lon"))
            if lat is None or lon is None:
                continue
            if not bbox.contains(lat, lon):
                continue

            rows.append(
                {
                    "point_id": str(item.get("id", "")),
                    "count_point_id": _extract_str(item, ("count_point_id", "countPointId")),
                    "road_name": _extract_str(item, ("road_name", "roadName")),
                    "road_category": _extract_str(item, ("road_category", "roadCategory")),
                    "latitude": lat,
                    "longitude": lon,
                }
            )

        page += 1

    if len(rows) == 0:
        raise RuntimeError(
            "No DfT count points found in the London bounding box. "
            "Try expanding the bbox in .env or check API connectivity."
        )

    df = pd.DataFrame(rows).drop_duplicates(subset=["point_id"])

    if len(df) > target_n:
        df = df.sample(n=target_n, random_state=seed).reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    return df


def ensure_points_csv(
    dft: DftRoadTrafficClient,
    *,
    bbox: LondonBBox,
    target_n: int,
    out_path: Path,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        return out_path

    df = fetch_count_points_in_bbox(dft, bbox=bbox, target_n=target_n)
    df.to_csv(out_path, index=False)
    return out_path
