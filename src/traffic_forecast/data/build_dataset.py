"""Build a modeling dataset from raw tick snapshots (Day 3)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


def _load_tomtom_parquets(root: Path) -> pd.DataFrame:
    files = sorted(root.glob("raw/tomtom/*/*.parquet"))
    if not files:
        raise FileNotFoundError("No TomTom parquet files found under data/raw/tomtom/. Run the collector first.")

    dfs: List[pd.DataFrame] = []
    for fp in files:
        try:
            dfs.append(pd.read_parquet(fp))
        except Exception:
            continue
    if not dfs:
        raise RuntimeError("Found TomTom files but failed to read any parquet.")

    return pd.concat(dfs, ignore_index=True)


def _load_tfl_json(root: Path) -> pd.DataFrame:
    files = sorted(root.glob("raw/tfl/*/*.json"))
    if not files:
        return pd.DataFrame(columns=["timestamp_utc", "tfl_disruptions_count", "tfl_severe_disruptions_count", "tfl_roads_seen"])

    rows: List[Dict] = []
    for fp in files:
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
            rows.append(
                {
                    "timestamp_utc": obj.get("timestamp_utc"),
                    "tfl_disruptions_count": obj.get("disruptions_count"),
                    "tfl_severe_disruptions_count": obj.get("severe_disruptions_count"),
                    "tfl_roads_seen": obj.get("roads_seen"),
                }
            )
        except Exception:
            continue

    return pd.DataFrame(rows)


def build_dataset(*, data_dir: Path = Path("data"), out_path: Path = Path("data/processed/observations.parquet")) -> Path:
    data_dir = Path(data_dir)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tomtom_df = _load_tomtom_parquets(data_dir)
    tfl_df = _load_tfl_json(data_dir)

    tomtom_df["timestamp_utc"] = pd.to_datetime(tomtom_df["timestamp_utc"], utc=True, errors="coerce")
    if len(tfl_df) > 0:
        tfl_df["timestamp_utc"] = pd.to_datetime(tfl_df["timestamp_utc"], utc=True, errors="coerce")
        df = tomtom_df.merge(tfl_df, on="timestamp_utc", how="left")
    else:
        df = tomtom_df.copy()
        df["tfl_disruptions_count"] = None
        df["tfl_severe_disruptions_count"] = None
        df["tfl_roads_seen"] = None

    df = df.sort_values(["point_id", "timestamp_utc"]).reset_index(drop=True)
    df.to_parquet(out_path, index=False)
    return out_path
