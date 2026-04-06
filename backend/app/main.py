from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .bayes import compute_model
from .config import DATA_FEED_CSV, DB_PATH, HISTORY_JSON, LATEST_JSON, MANUAL_DEFAULTS, PRIOR, RAW_DIR, SIGNALS, SERIES
from .exporter import export_history, export_latest
from .feed_loader import feed_entries_to_meta, load_data_feed
from .signals import classify
from .sources import latest_date, latest_value, load_fred_series, load_manual_overrides, load_nfci, try_rrp_usd_bn
from .storage import connect, fetch_history, insert_run
from .transforms import (
    custody_12w_pct,
    discount_window_bn,
    effr_iorb_bp,
    external_block_score,
    reservas_pct_min,
    sahm_gap,
    sofr_iorb_bp,
    stlfsi4_stress,
    usd_stress_score,
    vol_yields_20d_bp,
)


@dataclass
class PipelineOutput:
    latest_payload: dict
    history_payload: list[dict]


def _load_prior_config() -> dict:
    model_config_path = Path(__file__).resolve().parent / "model_config.json"
    try:
        with model_config_path.open("r", encoding="utf-8") as fh:
            config = json.load(fh)
        return config.get("prior_config", {"mode": "fixed", "base": PRIOR, "min": PRIOR, "max": PRIOR})
    except Exception:
        return {"mode": "fixed", "base": PRIOR, "min": PRIOR, "max": PRIOR}


def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def _smooth_add(value: float, threshold: float, kind: str, add_max: float, k: float) -> float:
    if kind == "gte":
        return add_max * _sigmoid(k * (value - threshold))
    elif kind == "lte":
        return add_max * _sigmoid(k * (threshold - value))
    return 0.0


def compute_dynamic_prior(raw: dict[str, float], prior_config: dict) -> tuple[float, list[dict]]:
    mode = prior_config.get("mode", "fixed")
    base = float(prior_config.get("base", PRIOR))
    min_v = float(prior_config.get("min", base))
    max_v = float(prior_config.get("max", base))
    if mode != "dynamic":
        return base, []

    prior = base
    details: list[dict] = []
    for rule in prior_config.get("rules", []):
        raw_key = rule["raw_key"]
        kind = rule["kind"]
        threshold = float(rule["threshold"])
        value = float(raw.get(raw_key, 0.0))

        add_max = rule.get("add_max")
        smooth_k = rule.get("smooth_k")

        if add_max is not None and smooth_k is not None:
            add_max = float(add_max)
            smooth_k = float(smooth_k)
            effective_add = _smooth_add(value, threshold, kind, add_max, smooth_k)
            prior += effective_add
            details.append({
                "label": rule.get("label", raw_key),
                "raw_key": raw_key,
                "kind": kind,
                "threshold": threshold,
                "value": value,
                "add": round(effective_add, 6),
                "add_max": add_max,
                "smooth_k": smooth_k,
                "mode": "smooth",
            })
        else:
            add = float(rule.get("add", 0.0))
            hit = (kind == "gte" and value >= threshold) or (kind == "lte" and value <= threshold)
            if hit:
                prior += add
                details.append({
                    "label": rule.get("label", raw_key),
                    "raw_key": raw_key,
                    "kind": kind,
                    "threshold": threshold,
                    "value": value,
                    "add": add,
                    "mode": "binary",
                })

    prior = max(min_v, min(max_v, prior))
    return prior, details


def _simplify_feed_status(method: str, quality_flag: str) -> str:
    text = f"{method} {quality_flag}".lower()
    if "fallback" in text:
        return "fallback"
    if "proxy" in text or "calculated" in text or "composite" in text:
        return "proxy"
    if "api" in text or "direct" in text or "scrape" in text:
        return "direct"
    if "missing" in text:
        return "missing"
    return quality_flag or method or "unknown"


def _source_status_from_feed(feed) -> dict[str, str]:
    mapping = {
        "tic_3m_usd_bn": "tic",
        "repo_stress_score": "repo_stress",
        "fra_ois_bp": "fra_ois",
        "rrp_usd_bn": "rrp",
        "usd_stress_score": "usd_stress",
        "private_credit_stress_score": "private_credit",
    }
    out: dict[str, str] = {}
    for key, alias in mapping.items():
        entry = feed.get(key)
        if entry is None:
            continue
        out[alias] = _simplify_feed_status(entry.method, entry.quality_flag)
    return out


def collect_raw_data() -> tuple[dict[str, float], dict[str, str], dict[str, dict]]:
    overrides = MANUAL_DEFAULTS.copy()
    overrides.update(load_manual_overrides(RAW_DIR / "manual_inputs.csv"))

    # Carregar séries FRED existentes
    curve = load_fred_series(SERIES["curve10y3m"])
    unrate = load_fred_series(SERIES["unrate"])
    sofr = load_fred_series(SERIES["sofr"])
    iorb = load_fred_series(SERIES["iorb"])
    custody = load_fred_series(SERIES["custody"])
    dxy = load_fred_series(SERIES["dxy_broad"])
    y10 = load_fred_series(SERIES["yield10"])
    y2 = load_fred_series(SERIES["yield2"])
    reserves = load_fred_series(SERIES["reserve_balances"])

    # Novas séries
    effr     = load_fred_series(SERIES["effr"])
    stlfsi4  = load_fred_series(SERIES["stlfsi4"])
    dpcredit = load_fred_series(SERIES["discount_window"])
    sp500    = load_fred_series(SERIES["sp500"])

    rrp = try_rrp_usd_bn(latest_date(curve))
    if rrp is None:
        rrp = float(overrides["rrp_usd_bn"])

    # Dados brutos
    raw = {
        "curva_spread": latest_value(curve),
        "sahm_gap": sahm_gap([v for _, v in unrate]),
        "reservas_pct_min": reservas_pct_min(latest_value(reserves), float(overrides["reserve_floor"])),
        "rrp_usd_bn": rrp,
        "sofr_iorb_bp": sofr_iorb_bp(latest_value(sofr), latest_value(iorb)),
        "fra_ois_bp": float(overrides["fra_ois_bp"]),
        "repo_stress_score": float(overrides["repo_stress_score"]),
        "vol_yields_20d_bp": vol_yields_20d_bp([v for _, v in y10]),
        "custody_12w_pct": custody_12w_pct([v for _, v in custody]),
        "tic_3m_usd_bn": float(overrides["tic_3m_usd_bn"]),
        "usd_stress_score": float(overrides.get("usd_stress_score", usd_stress_score(
            [v for _, v in dxy], [v for _, v in y10], [v for _, v in y2]
        ))),
        "nfci": load_nfci(),
        "private_credit_stress_score": float(overrides.get("private_credit_stress_score", 0.50)),
        # Novos sinais
        "effr_iorb_bp": effr_iorb_bp([v for _, v in effr], latest_value(iorb)),
        "stlfsi4_stress": stlfsi4_stress(latest_value(stlfsi4)),
        "discount_window_bn": discount_window_bn(latest_value(dpcredit)),
        "sp500": latest_value(sp500),
    }

    source_status = {"custody": "direct", "nfci": "direct"}

    # Atualizar com dados do feed
    # Valores com quality_flag="default_fallback" NÃO substituem os calculados via FRED —
    # são usados apenas quando não há nenhuma outra fonte disponível (já estão em raw
    # via MANUAL_DEFAULTS). Isso evita que falhas temporárias da API ativem sinais
    # artificialmente com valores históricos desatualizados.
    feed = load_data_feed(DATA_FEED_CSV)
    for key, entry in feed.items():
        if key in raw and entry.quality_flag != "default_fallback":
            raw[key] = entry.value

    source_status.update(_source_status_from_feed(feed))
    source_status.setdefault("tic", "manual_or_default")
    source_status.setdefault("repo_stress", "manual_or_default")
    source_status.setdefault("fra_ois", "manual_or_default")
    source_status.setdefault("rrp", "direct" if rrp != float(overrides["rrp_usd_bn"]) else "fallback")
    source_status.setdefault("usd_stress", "proxy")
    source_status.setdefault("private_credit", "manual_or_default")
    # Novos sinais — todos diretos via FRED
    source_status["effr_iorb"] = "direct"
    source_status["stlfsi4_stress"] = "direct"
    source_status["discount_window"] = "direct"

    # Bloco externo
    raw["bloco_externo_score"] = external_block_score(
        raw["custody_12w_pct"],
        raw["tic_3m_usd_bn"],
        raw["usd_stress_score"]
    )
    
    return raw, source_status, feed_entries_to_meta(feed)


def run_pipeline() -> PipelineOutput:
    raw, source_status, data_feed_meta = collect_raw_data()
    statuses, ext_score = classify(raw)

    prior_config = _load_prior_config()
    effective_prior, prior_details = compute_dynamic_prior(raw, prior_config)

    run_date = str(date.today())
    model = compute_model(effective_prior, SIGNALS, raw, statuses,
                          data_feed_meta=data_feed_meta, run_date=run_date)

    external_block = {
        "custody_12w_pct": raw["custody_12w_pct"],
        "tic_3m_usd_bn": raw["tic_3m_usd_bn"],
        "usd_stress_score": raw["usd_stress_score"],
        "composite_score": ext_score,
        "status": statuses.get("bloco_externo", "NEUTRO"),
    }

    # SP500 — carregado para o payload mas não como sinal do modelo
    sp500_value = raw.get("sp500", None)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(DB_PATH)
    run_id = insert_run(conn, run_date, model, external_block, sp500=sp500_value)
    history = fetch_history(conn, limit=520)

    latest_payload = {
        "run_id": run_id,
        "run_date": run_date,
        "prior": model["prior"],
        "prior_base": float(prior_config.get("base", model["prior"])),
        "prior_min": float(prior_config.get("min", model["prior"])),
        "prior_max": float(prior_config.get("max", model["prior"])),
        "prior_mode": prior_config.get("mode", "fixed"),
        "prior_details": prior_details,
        "posterior": model["posterior"],
        "risk_label": model["risk_label"],
        "signals": model["signals"],
        "external_block": external_block,
        "raw_data": raw,
        "source_status": source_status,
        "data_feed_meta": data_feed_meta,
        "sp500": sp500_value,
    }
    export_latest(LATEST_JSON, latest_payload)
    export_history(HISTORY_JSON, history)
    return PipelineOutput(latest_payload=latest_payload, history_payload=history)


if __name__ == "__main__":
    out = run_pipeline()
    print(f"[OK] latest.json: {LATEST_JSON}")
    print(f"[OK] history.json: {HISTORY_JSON}")
    print(f"[OK] prior: {out.latest_payload['prior']:.4f}")
    print(f"[OK] posterior: {out.latest_payload['posterior']:.4f}")