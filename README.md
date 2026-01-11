# London Short-Term Traffic Congestion Prediction (London, UK)

This repository contains an end-to-end machine learning system to predict short-term traffic congestion in London (15–30 minute horizon).

The project is designed as a production-style ML pipeline and uses **Modal** for automated execution and scheduling instead of GitHub Actions.

Live demo (Hugging Face Space):  
https://huggingface.co/spaces/DavidBazaldua/London_traffic

---

## Project Overview

The goal of this project is to predict near-term traffic conditions for selected road monitoring points in London using real-time and contextual data.

The system follows modern ML-Ops principles:
- Clear separation between offline (historical) and online (real-time) pipelines
- Centralized feature management via a feature store
- Reproducible notebooks for experimentation and backfills
- Automated real-time inference every 30 minutes using Modal Cron
- Lightweight deployment for visualization and consumption

---

## Data Sources

The pipeline integrates multiple public data sources:

- TomTom Traffic API  
  Real-time traffic flow data (speed, free-flow speed, travel time, confidence).

- Transport for London (TfL) Unified API  
  Road disruptions, incidents, and closures.

- UK Department for Transport (DfT) Road Traffic Data  
  Static metadata and monitoring point information.

---

## Repository Structure

.
├── notebooks/
│   ├── 00_api_smoke_tests.ipynb
│   ├── 01_data_collection_and_eda.ipynb
│   ├── 01b_etl_to_feature_store.ipynb
│   ├── 01c_metadata_feature_group.ipynb
│   ├── 02_feature_engineering_backfill.ipynb
│   ├── 03_weather_feature_backfill.ipynb
│   ├── 04_tfl_disruptions_backfill_fast.ipynb
│   ├── 05_label_generation_backfill.ipynb
│   ├── 05b_feature_view.ipynb
│   ├── 06_model_training.ipynb
│   ├── 07_realtime_feature_update_modal_30min.ipynb
│   ├── 08_inference_pipeline.ipynb
│   └── appendix_*.ipynb
├── src/
├── modal_app.py
├── predictions_latest.json
├── requirements.txt
├── pyproject.toml
└── README.md

---

## Pipeline Architecture

The system is composed of three main pipelines.

### 1. Feature Pipeline (Offline & Online)

- Historical backfills populate feature groups with enriched traffic signals.
- Feature engineering includes:
  - Speed ratios and delay estimation
  - Time-based features (lags and rolling behavior)
  - Disruption indicators from TfL
- All features are stored in a centralized feature store to ensure training–serving consistency.

---

### 2. Training Pipeline

- A feature view is defined to join all required feature groups.
- Labels are generated with future horizons (e.g. +30 and +60 minutes).
- The model is trained using historical data with time-aware validation.
- The trained model artifact is reused by the inference pipeline.

---

### 3. Real-Time Inference Pipeline (Modal)

- Real-time feature updates and inference are executed using Modal.
- A scheduled Modal job runs every 30 minutes.
- The pipeline fetches the latest traffic data, applies feature transformations, and generates predictions.
- The output is exported to predictions_latest.json.

---

## Automation with Modal

This project uses Modal as the orchestration and scheduling layer for real-time pipelines.

- Execution logic is defined in modal_app.py
- Scheduling is handled via Modal Cron
- Secrets and API keys are managed through Modal Secrets
- The full repository is mounted or cloned inside the Modal container

---

## Local Development

### 1. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure API keys

Set the following environment variables locally (do not commit secrets):

- HOPSWORKS_API_KEY
- TOMTOM_API_KEY
- TFL_APP_KEY
- HUGGINGFACE_API_KEY

---

## Running with Modal

### Manual execution

```bash
modal run modal_app.py
```

### Deploy scheduled pipeline

```bash
modal deploy modal_app.py
```

---

## Deployment

The latest predictions are visualized via a Hugging Face Space.

The Space reads predictions_latest.json and displays the results in a lightweight interactive UI.

---

## Technologies Used

- Python
- Pandas
- NumPy
- Scikit-learn
- Hopsworks Feature Store
- Modal
- Hugging Face Spaces
- Jupyter Notebooks

---

## Status

This project is fully functional and designed as a production-grade ML system prototype.
