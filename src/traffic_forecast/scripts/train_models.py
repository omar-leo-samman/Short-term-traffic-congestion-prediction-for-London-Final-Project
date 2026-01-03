"""Train models from processed dataset (Day 4)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ..features import FeatureConfig, make_feature_frame
from ..model import TrainConfig, train_models


def main() -> None:
    obs_path = Path("data/processed/observations.parquet")
    if not obs_path.exists():
        raise FileNotFoundError("Missing data/processed/observations.parquet. Run build_dataset first.")

    df = pd.read_parquet(obs_path)

    ts = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce").sort_values()
    diffs = ts.diff().dt.total_seconds().dropna()
    interval_minutes = int(round(diffs.median() / 60.0)) if len(diffs) else 10
    interval_minutes = max(1, interval_minutes)

    feat_cfg = FeatureConfig(horizons_min=(15, 30), lags=(1, 2, 3), rolling_windows=(3, 6), drop_na=True)
    fdf = make_feature_frame(df, cfg=feat_cfg, interval_minutes=interval_minutes)

    target_cols = ["y_15", "y_30"]
    drop_cols = {"timestamp_utc", "point_id", "latitude", "longitude"} | set(target_cols)
    feature_cols = [c for c in fdf.columns if c not in drop_cols]

    results = train_models(fdf, feature_cols=feature_cols, cfg=TrainConfig(horizons_min=(15, 30), test_fraction=0.2))

    Path("reports").mkdir(parents=True, exist_ok=True)
    Path("reports/metrics.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    print("Training complete. Metrics -> reports/metrics.json")


if __name__ == "__main__":
    main()
