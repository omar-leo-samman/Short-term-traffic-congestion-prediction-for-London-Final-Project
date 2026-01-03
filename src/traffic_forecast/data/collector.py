"""Data collection loop (Day 2).

Stores raw snapshots as one file per tick:
- data/raw/tomtom/YYYYMMDD/HHMMSSZ.parquet
- data/raw/tfl/YYYYMMDD/HHMMSSZ.json
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from ..clients.tomtom import TomTomClient
from ..clients.tfl import TflClient
from ..clients.dft import DftRoadTrafficClient
from .points import LondonBBox, ensure_points_csv


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


@dataclass(frozen=True)
class CollectorConfig:
    num_points: int = 50
    interval_seconds: int = 600
    duration_minutes: int = 180
    out_dir: Path = Path("data")
    london_bbox: Optional[LondonBBox] = None


class Collector:
    def __init__(
        self,
        *,
        tomtom: TomTomClient,
        tfl: TflClient,
        dft: DftRoadTrafficClient,
        cfg: CollectorConfig,
    ) -> None:
        self.tomtom = tomtom
        self.tfl = tfl
        self.dft = dft
        self.cfg = cfg

    def _points_path(self) -> Path:
        return self.cfg.out_dir / "metadata" / "points.csv"

    def _ensure_points(self) -> pd.DataFrame:
        bbox = self.cfg.london_bbox or LondonBBox(51.28, 51.70, -0.55, 0.30)
        points_csv = ensure_points_csv(self.dft, bbox=bbox, target_n=self.cfg.num_points, out_path=self._points_path())
        return pd.read_csv(points_csv)

    def _fetch_tfl_features(self) -> Dict:
        roads = self.tfl.list_roads()
        road_ids: List[str] = []
        if isinstance(roads, list):
            for r in roads[:50]:
                if isinstance(r, dict) and "id" in r:
                    road_ids.append(str(r["id"]))

        disruptions_count = 0
        severe_count = 0
        sample_ids = road_ids[:8]

        disruptions_payloads: List[Dict] = []
        for rid in sample_ids:
            try:
                dis = self.tfl.road_disruptions(rid)
                if isinstance(dis, list):
                    disruptions_count += len(dis)
                    for d in dis:
                        if isinstance(d, dict):
                            sev = d.get("severity") or d.get("severityLevel") or d.get("category")
                            if isinstance(sev, str) and sev.lower() in ("serious", "severe", "critical"):
                                severe_count += 1
                disruptions_payloads.append({"road_id": rid, "disruptions": dis})
            except Exception as exc:
                disruptions_payloads.append({"road_id": rid, "error": str(exc)})

        return {
            "roads_seen": len(road_ids),
            "sampled_road_ids": sample_ids,
            "disruptions_count": disruptions_count,
            "severe_disruptions_count": severe_count,
            "disruptions_payloads": disruptions_payloads,
        }

    def _fetch_tomtom_tick(self, points: pd.DataFrame, *, tick_ts: str) -> pd.DataFrame:
        rows: List[Dict] = []
        for _, p in tqdm(points.iterrows(), total=len(points), desc="TomTom per-point", leave=False):
            lat = float(p["latitude"])
            lon = float(p["longitude"])
            point_id = str(p.get("point_id", ""))

            try:
                payload = self.tomtom.flow_segment_data(lat=lat, lon=lon, zoom=10)
                metrics = self.tomtom.parse_metrics(payload)

                rows.append(
                    {
                        "timestamp_utc": tick_ts,
                        "point_id": point_id,
                        "latitude": lat,
                        "longitude": lon,
                        "current_speed": metrics.current_speed,
                        "free_flow_speed": metrics.free_flow_speed,
                        "current_travel_time": metrics.current_travel_time,
                        "free_flow_travel_time": metrics.free_flow_travel_time,
                        "confidence": metrics.confidence,
                        "road_closure": metrics.road_closure,
                        "congestion_index": metrics.congestion_index,
                    }
                )
            except Exception as exc:
                rows.append(
                    {
                        "timestamp_utc": tick_ts,
                        "point_id": point_id,
                        "latitude": lat,
                        "longitude": lon,
                        "error": str(exc),
                    }
                )
        return pd.DataFrame(rows)

    def run(self) -> None:
        points = self._ensure_points()
        print(f"Loaded {len(points)} points -> {self._points_path()}")

        total_ticks = max(1, int((self.cfg.duration_minutes * 60) / self.cfg.interval_seconds))
        print(
            f"Collector will run for {total_ticks} ticks (~{self.cfg.duration_minutes} min). "
            f"Interval={self.cfg.interval_seconds}s Points={len(points)}"
        )

        for tick in range(total_ticks):
            tick_ts = utc_timestamp()
            date = utc_date()

            tfl_features = self._fetch_tfl_features()
            tomtom_df = self._fetch_tomtom_tick(points, tick_ts=tick_ts)

            out_tomtom = self.cfg.out_dir / "raw" / "tomtom" / date / f"{tick_ts}.parquet"
            out_tfl = self.cfg.out_dir / "raw" / "tfl" / date / f"{tick_ts}.json"
            out_tomtom.parent.mkdir(parents=True, exist_ok=True)
            out_tfl.parent.mkdir(parents=True, exist_ok=True)

            tomtom_df.to_parquet(out_tomtom, index=False)
            out_tfl.write_text(json.dumps({"timestamp_utc": tick_ts, **tfl_features}, indent=2), encoding="utf-8")

            print(f"[{tick+1}/{total_ticks}] Wrote: {out_tomtom} and {out_tfl}")

            if tick < total_ticks - 1:
                time.sleep(self.cfg.interval_seconds)
