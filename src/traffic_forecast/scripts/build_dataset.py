"""Build processed dataset from raw snapshots (Day 3)."""

from __future__ import annotations

from pathlib import Path

from ..data.build_dataset import build_dataset


def main() -> None:
    out = build_dataset(data_dir=Path("data"), out_path=Path("data/processed/observations.parquet"))
    print(f"Wrote dataset to: {out}")


if __name__ == "__main__":
    main()
