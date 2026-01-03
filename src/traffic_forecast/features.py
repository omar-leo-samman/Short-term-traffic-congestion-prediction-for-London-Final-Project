"""Feature engineering utilities (Day 4).

We keep dependencies light by using pandas only.

Core idea:
- Our target is `congestion_index = 1 - currentSpeed/freeFlowSpeed` (computed upstream).
- For short-horizon forecasting (15–30 min), the most predictive signals are recent lags.

Features implemented:
- Time features: hour, day_of_week, is_weekend
- Lag features of congestion_index (by point_id)
- Rolling mean/std of congestion_index (by point_id)

Targets:
- y_h = congestion_index shifted by -h minutes (by point_id)

Important:
- Lags and rolling windows are defined in *rows*, not minutes.
  This keeps code simple given variable sampling interval.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd


@dataclass(frozen=True)
class FeatureConfig:
    horizons_min: Sequence[int] = (15, 30)
    lags: Sequence[int] = (1, 2, 3)  # in number of rows (ticks)
    rolling_windows: Sequence[int] = (3, 6)  # in number of rows
    drop_na: bool = True


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts = pd.to_datetime(out["timestamp_utc"], utc=True, errors="coerce")
    out["hour"] = ts.dt.hour
    out["day_of_week"] = ts.dt.dayofweek  # Monday=0
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
    return out


def add_lag_features(df: pd.DataFrame, *, group_key: str, col: str, lags: Sequence[int]) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values([group_key, "timestamp_utc"]).reset_index(drop=True)
    for k in lags:
        out[f"{col}_lag_{k}"] = out.groupby(group_key)[col].shift(k)
    return out


def add_rolling_features(df: pd.DataFrame, *, group_key: str, col: str, windows: Sequence[int]) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values([group_key, "timestamp_utc"]).reset_index(drop=True)
    g = out.groupby(group_key)[col]
    for w in windows:
        shifted = g.shift(1)
        out[f"{col}_roll_mean_{w}"] = shifted.rolling(window=w, min_periods=1).mean().reset_index(level=0, drop=True)
        out[f"{col}_roll_std_{w}"] = shifted.rolling(window=w, min_periods=1).std().reset_index(level=0, drop=True)
    return out


def add_targets(
    df: pd.DataFrame,
    *,
    group_key: str,
    target_col: str,
    horizons_min: Sequence[int],
    interval_minutes: int,
) -> pd.DataFrame:
    """Create future targets by shifting negative rows.

    shift_rows = horizon / interval_minutes
    Example: interval 10 min, horizon 30 min -> shift_rows = 3
    """
    out = df.copy().sort_values([group_key, "timestamp_utc"]).reset_index(drop=True)
    for h in horizons_min:
        shift_rows = int(round(h / interval_minutes))
        out[f"y_{h}"] = out.groupby(group_key)[target_col].shift(-shift_rows)
    return out


def make_feature_frame(
    df: pd.DataFrame,
    *,
    cfg: FeatureConfig,
    interval_minutes: int,
    group_key: str = "point_id",
    base_target_col: str = "congestion_index",
) -> pd.DataFrame:
    out = df.copy()
    out = add_time_features(out)
    out = add_lag_features(out, group_key=group_key, col=base_target_col, lags=cfg.lags)
    out = add_rolling_features(out, group_key=group_key, col=base_target_col, windows=cfg.rolling_windows)
    out = add_targets(out, group_key=group_key, target_col=base_target_col, horizons_min=cfg.horizons_min, interval_minutes=interval_minutes)

    if cfg.drop_na:
        target_cols = [f"y_{h}" for h in cfg.horizons_min]
        out = out.dropna(subset=target_cols)

    return out
