"""Inference helpers.

For an end-to-end demo we need a predictable inference path.
This module supports **offline inference** based on the latest observation stored in
`data/processed/observations.parquet`.

For a true live system (Hugging Face Space), you'll want:
- live TomTom fetch at time t
- a rolling cache for lags and rolling windows
- feature builder that operates on that cache

We keep this as a pragmatic starting point.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import joblib
import pandas as pd

from .features import add_time_features


@dataclass(frozen=True)
class LoadedModel:
    model: object
    feature_cols: list
    horizon_min: int


def load_model(path: Path) -> LoadedModel:
    obj = joblib.load(path)
    return LoadedModel(model=obj["model"], feature_cols=obj["feature_cols"], horizon_min=int(obj["horizon_min"]))


def latest_features_for_point(
    observations: pd.DataFrame,
    *,
    point_id: str,
    feature_cols: list,
) -> pd.DataFrame:
    dfp = observations[observations["point_id"].astype(str) == str(point_id)].copy()
    if dfp.empty:
        raise ValueError(f"No observations found for point_id={point_id}")

    dfp = dfp.sort_values("timestamp_utc").tail(1).copy()
    dfp = add_time_features(dfp)

    # Fill any missing engineered columns using a safe fallback.
    fallback = float(dfp["congestion_index"].iloc[0]) if "congestion_index" in dfp.columns else 0.0
    for c in feature_cols:
        if c not in dfp.columns:
            dfp[c] = fallback

    X = dfp[feature_cols].fillna(fallback)
    return X


def predict_for_point(
    model_path: Path,
    *,
    observations_path: Path,
    point_id: str,
) -> Dict[str, float]:
    lm = load_model(model_path)
    obs = pd.read_parquet(observations_path)
    X = latest_features_for_point(obs, point_id=point_id, feature_cols=lm.feature_cols)
    yhat = float(lm.model.predict(X)[0])  # type: ignore[attr-defined]
    return {"point_id": str(point_id), "horizon_min": lm.horizon_min, "prediction": yhat}
