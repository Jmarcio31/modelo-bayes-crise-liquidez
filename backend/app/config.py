from __future__ import annotations

import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
APP_DIR = BASE_DIR / "backend" / "app"
RAW_DIR = BASE_DIR / "backend" / "data" / "raw"
DATA_DIR = BASE_DIR / "frontend" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = BASE_DIR / "backend" / "data" / "liquidez.db"
LATEST_JSON = DATA_DIR / "latest.json"
HISTORY_JSON = DATA_DIR / "history.json"
DATA_FEED_CSV = RAW_DIR / "data_feed.csv"
MODEL_CONFIG_PATH = APP_DIR / "model_config.json"

with MODEL_CONFIG_PATH.open("r", encoding="utf-8") as fh:
    MODEL_CONFIG = json.load(fh)

PRIOR = float(MODEL_CONFIG.get("prior", 0.10))
SIGNALS = MODEL_CONFIG.get("signals", [])
MANUAL_DEFAULTS = MODEL_CONFIG.get("manual_defaults", {
    "fra_ois_bp": 29.0,
    "repo_stress_score": 0.66,
    "tic_3m_usd_bn": -96.0,
    "rrp_usd_bn": 145.0,
    "reserve_floor": 3000000.0,
})
