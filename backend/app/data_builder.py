from __future__ import annotations

import csv
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "backend" / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

DATA_FEED_PATH = RAW_DIR / "data_feed.csv"
TIMEOUT = 30

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
NYFED_SECURED_LATEST = "https://markets.newyorkfed.org/api/rates/secured/all/latest.json"
NYFED_RRP_JSON = "https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json?startDate={date}&endDate={date}"

SERIES = {
    "dxy_broad": "DTWEXBGS",
    "yield10": "DGS10",
    "yield2": "DGS2",
    "sofr": "SOFR",
    "cp3m_nonfinancial": "CPN3M",
    "cp3m_financial": "CPF3M",
    "curve10y3m": "T10Y3M",
}

DEFAULT_FALLBACKS = {
    "tic_3m_usd_bn": -96.0,
    "repo_stress_score": 0.66,
    "fra_ois_bp": 29.0,
    "rrp_usd_bn": 145.0,
    "usd_stress_score": 0.61,
}


@dataclass
class FeedRow:
    key: str
    value: float
    as_of_date: str
    source: str
    method: str
    quality_flag: str


def today_utc_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def get_fred_api_key() -> str:
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError("FRED_API_KEY nao encontrada no ambiente")
    return key


def _safe_float(value) -> Optional[float]:
    try:
        if value in ("", None, "."):
            return None
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _std(values: Iterable[float]) -> float:
    values = list(values)
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


def _pct_change(current: float, past: float) -> float:
    if not past:
        return 0.0
    return ((current / past) - 1.0) * 100.0


def fetch_fred_series(series_id: str) -> List[tuple[str, float]]:
    params = {
        "series_id": series_id,
        "api_key": get_fred_api_key(),
        "file_type": "json",
        "sort_order": "asc",
    }
    resp = requests.get(FRED_URL, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    obs = payload.get("observations", [])
    out: List[tuple[str, float]] = []
    for row in obs:
        val = _safe_float(row.get("value"))
        if val is None:
            continue
        out.append((row.get("date", ""), val))
    if not out:
        raise RuntimeError(f"serie FRED vazia: {series_id}")
    return out


def latest_value(series: List[tuple[str, float]]) -> float:
    return series[-1][1]


def latest_date(series: List[tuple[str, float]]) -> str:
    return series[-1][0]


def _load_last_feed() -> Dict[str, FeedRow]:
    if not DATA_FEED_PATH.exists():
        return {}
    out: Dict[str, FeedRow] = {}
    with DATA_FEED_PATH.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = row.get("key")
            if not key:
                continue
            value = _safe_float(row.get("value"))
            if value is None:
                continue
            out[key] = FeedRow(
                key=key,
                value=value,
                as_of_date=row.get("as_of_date", ""),
                source=row.get("source", ""),
                method=row.get("method", ""),
                quality_flag=row.get("quality_flag", ""),
            )
    return out


LAST_FEED = _load_last_feed()


def _fallback_row(key: str, source: str, reason: str) -> FeedRow:
    if key in LAST_FEED:
        prev = LAST_FEED[key]
        return FeedRow(
            key=key,
            value=prev.value,
            as_of_date=prev.as_of_date or today_utc_str(),
            source=prev.source or source,
            method=f"fallback_last_valid:{reason}",
            quality_flag="stale_fallback",
        )
    return FeedRow(
        key=key,
        value=DEFAULT_FALLBACKS[key],
        as_of_date=today_utc_str(),
        source=source,
        method=f"default_fallback:{reason}",
        quality_flag="default_fallback",
    )


def build_tic() -> FeedRow:
    key = "tic_3m_usd_bn"
    try:
        dxy = fetch_fred_series(SERIES["dxy_broad"])
        curve = fetch_fred_series(SERIES["curve10y3m"])
        dxy_values = [v for _, v in dxy]
        curve_values = [v for _, v in curve]

        dxy_chg_20d = _pct_change(dxy_values[-1], dxy_values[-21]) if len(dxy_values) >= 21 else 0.0
        curve_level = curve_values[-1] if curve_values else 0.0

        value = -(60.0 + 10.0 * max(dxy_chg_20d, 0.0) + 20.0 * max(curve_level, 0.0))
        value = max(-180.0, min(-20.0, value))

        return FeedRow(
            key=key,
            value=round(value, 4),
            as_of_date=latest_date(dxy),
            source="public_proxy",
            method="calculated_proxy_dxy_plus_curve",
            quality_flag="ok",
        )
    except Exception as exc:
        return _fallback_row(key, "public_proxy", f"tic_error:{type(exc).__name__}")


def build_fra_ois_proxy() -> FeedRow:
    key = "fra_ois_bp"
    try:
        sofr = fetch_fred_series(SERIES["sofr"])
        cp_fin = fetch_fred_series(SERIES["cp3m_financial"])
        cp_nf = fetch_fred_series(SERIES["cp3m_nonfinancial"])

        sofr_last = latest_value(sofr)
        cp_fin_last = latest_value(cp_fin)
        cp_nf_last = latest_value(cp_nf)

        proxy_bp = ((0.6 * cp_fin_last + 0.4 * cp_nf_last) - sofr_last) * 100.0
        proxy_bp = max(0.0, min(120.0, proxy_bp))
        return FeedRow(
            key=key,
            value=round(proxy_bp, 4),
            as_of_date=latest_date(sofr),
            source="fred",
            method="calculated_proxy_cp_minus_sofr",
            quality_flag="ok",
        )
    except Exception as exc:
        return _fallback_row(key, "fred", f"fra_ois_error:{type(exc).__name__}")


def _fetch_json(url: str) -> dict:
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def build_repo_stress() -> FeedRow:
    key = "repo_stress_score"
    try:
        payload = _fetch_json(NYFED_SECURED_LATEST)
        rates = payload.get("refRates", [])
        sofr = next((x for x in rates if x.get("type") == "SOFR"), None)
        tgcr = next((x for x in rates if x.get("type") == "TGCR"), None)
        bgcr = next((x for x in rates if x.get("type") == "BGCR"), None)

        if not sofr:
            raise RuntimeError("SOFR ausente no NY Fed payload")

        sofr_rate = _safe_float(sofr.get("percentRate")) or 0.0
        sofr_p1 = _safe_float(sofr.get("percentPercentile1")) or sofr_rate
        sofr_p99 = _safe_float(sofr.get("percentPercentile99")) or sofr_rate
        sofr_vol = _safe_float(sofr.get("volumeInBillions")) or 0.0

        tgcr_rate = _safe_float(tgcr.get("percentRate")) if tgcr else sofr_rate
        bgcr_rate = _safe_float(bgcr.get("percentRate")) if bgcr else sofr_rate

        spread_component = min(1.0, max(0.0, abs(sofr_rate - (tgcr_rate + bgcr_rate) / 2.0) / 0.08))
        dispersion_component = min(1.0, max(0.0, (sofr_p99 - sofr_p1) / 0.25))
        volume_component = min(1.0, max(0.0, (1800.0 - sofr_vol) / 1000.0))

        score = 0.40 * spread_component + 0.35 * dispersion_component + 0.25 * volume_component
        score = max(0.0, min(1.0, score))
        return FeedRow(
            key=key,
            value=round(score, 4),
            as_of_date=today_utc_str(),
            source="nyfed_markets",
            method="calculated_composite",
            quality_flag="ok",
        )
    except Exception as exc:
        return _fallback_row(key, "nyfed_markets", f"repo_error:{type(exc).__name__}")


def build_rrp() -> FeedRow:
    key = "rrp_usd_bn"
    try:
        payload = _fetch_json("https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json")

        repo_block = payload.get("repo") if isinstance(payload, dict) else None
        operations = repo_block.get("operations", []) if isinstance(repo_block, dict) else []

        values = []
        if isinstance(operations, list):
            for item in operations[:5]:
                if not isinstance(item, dict):
                    continue
                value = _safe_float(item.get("totalAmtAccepted"))
                if value is not None:
                    values.append(value)

        if not values:
            raise RuntimeError("RRP nao retornou valores")

        total = sum(values) / len(values)

        # normalização para USD bn
        if total > 1e9:
            total = total / 1e9
        elif total > 1e6:
            total = total / 1e3

        return FeedRow(
            key=key,
            value=round(total, 4),
            as_of_date=today_utc_str(),
            source="nyfed_markets",
            method="api_direct_5d_avg",
            quality_flag="ok",
        )

    except Exception as exc:
        return _fallback_row(key, "nyfed_markets", f"rrp_error:{type(exc).__name__}")


def build_usd_stress() -> FeedRow:
    key = "usd_stress_score"
    try:
        dxy = fetch_fred_series(SERIES["dxy_broad"])
        y10 = fetch_fred_series(SERIES["yield10"])
        y2 = fetch_fred_series(SERIES["yield2"])

        dxy_values = [v for _, v in dxy]
        y10_values = [v for _, v in y10]
        y2_values = [v for _, v in y2]

        dxy_change_20d = _pct_change(dxy_values[-1], dxy_values[-21]) if len(dxy_values) >= 21 else 0.0
        y10_vol = _std(y10_values[-20:]) if len(y10_values) >= 20 else 0.0
        curve_slope = y10_values[-1] - y2_values[-1] if y10_values and y2_values else 0.0

        dxy_component = min(max(dxy_change_20d / 4.0, 0.0), 1.0)
        vol_component = min(max(y10_vol / 0.18, 0.0), 1.0)
        curve_component = min(max((0.5 - curve_slope) / 1.5, 0.0), 1.0)

        score = 0.45 * dxy_component + 0.35 * vol_component + 0.20 * curve_component
        score = max(0.0, min(1.0, score))

        return FeedRow(
            key=key,
            value=round(score, 4),
            as_of_date=latest_date(dxy),
            source="fred",
            method="calculated_composite",
            quality_flag="ok",
        )
    except Exception as exc:
        return _fallback_row(key, "fred", f"usd_stress_error:{type(exc).__name__}")


def build_rows() -> List[FeedRow]:
    return [
        build_tic(),
        build_repo_stress(),
        build_fra_ois_proxy(),
        build_rrp(),
        build_usd_stress(),
    ]


def write_feed(rows: List[FeedRow]) -> None:
    with DATA_FEED_PATH.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["key", "value", "as_of_date", "source", "method", "quality_flag"])
        for row in rows:
            writer.writerow([row.key, row.value, row.as_of_date, row.source, row.method, row.quality_flag])


def main() -> int:
    rows = build_rows()
    write_feed(rows)
    print(f"[OK] data_feed.csv: {DATA_FEED_PATH}")
    print(json.dumps([row.__dict__ for row in rows], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
