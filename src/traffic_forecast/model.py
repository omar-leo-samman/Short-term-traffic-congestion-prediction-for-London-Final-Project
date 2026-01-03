"""Model training and evaluation helpers (Day 4).

We keep it simple and reproducible:
- Baseline: persistence (predict future congestion = current congestion)
- ML model: sklearn HistGradientBoostingRegressor

We train one model per horizon (15 and 30 minutes).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error


@dataclass(frozen=True)
class TrainConfig:
    horizons_min: Tuple[int, ...] = (15, 30)
    test_fraction: float = 0.2
    random_state: int = 42


def time_split(df: pd.DataFrame, *, test_fraction: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split by time (not random) to reduce leakage."""
    df = df.sort_values("timestamp_utc").reset_index(drop=True)
    n = len(df)
    cut = int(round(n * (1 - test_fraction)))
    cut = max(1, min(n - 1, cut))
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()


def persistence_predict(df: pd.DataFrame) -> np.ndarray:
    """Baseline forecast: yhat(t+h) = congestion_index(t)."""
    return df["congestion_index"].to_numpy()


def train_hgb(X_train: pd.DataFrame, y_train: np.ndarray, *, random_state: int) -> HistGradientBoostingRegressor:
    model = HistGradientBoostingRegressor(
        learning_rate=0.05,
        max_depth=6,
        max_iter=300,
        random_state=random_state,
    )
    model.fit(X_train, y_train)
    return model


def evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(mean_squared_error(y_true, y_pred, squared=False))
    return {"mae": mae, "rmse": rmse}


def train_models(
    df: pd.DataFrame,
    *,
    feature_cols: List[str],
    cfg: TrainConfig,
    out_dir: Path = Path("models"),
) -> Dict[str, Dict]:
    """Train one model per horizon and save joblib artifacts."""

    out_dir.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Dict] = {}

    train_df, test_df = time_split(df, test_fraction=cfg.test_fraction)

    X_train = train_df[feature_cols]
    X_test = test_df[feature_cols]

    for h in cfg.horizons_min:
        ycol = f"y_{h}"
        y_train = train_df[ycol].to_numpy()
        y_test = test_df[ycol].to_numpy()

        # Baseline
        y_pred_base = persistence_predict(test_df)
        base_metrics = evaluate(y_test, y_pred_base)

        # ML model
        model = train_hgb(X_train, y_train, random_state=cfg.random_state)
        y_pred = model.predict(X_test)
        ml_metrics = evaluate(y_test, y_pred)

        model_path = out_dir / f"hgb_h{h}.joblib"
        joblib.dump({"model": model, "feature_cols": feature_cols, "horizon_min": h}, model_path)

        results[str(h)] = {
            "baseline": base_metrics,
            "hgb": ml_metrics,
            "model_path": str(model_path),
            "n_train": int(len(train_df)),
            "n_test": int(len(test_df)),
        }

    return results
