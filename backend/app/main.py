from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .bayes import compute_model
from .config import DB_PATH, FRONTEND_DIR, HISTORY_JSON, LATEST_JSON, MANUAL_DEFAULTS, PRIOR, RAW_DIR, SIGNALS
from .exporter import export_history, export_latest
from .signals import classify
from .sources import SERIES, latest_date, latest_value, load_fred_series, load_manual_overrides, load_nfci, try_rrp_usd_bn
from .storage import connect, fetch_history, insert_run
from .transforms import (
    custody_12w_pct,
    reservas_pct_min,
    sahm_gap,
    sofr_iorb_bp,
    usd_stress_score,
    vol_yields_20d_bp,
    external_block_score,
)


@dataclass
class PipelineOutput:
    latest_payload: dict
    history_payload: list[dict]


def collect_raw_data() -> tuple[dict[str, float], dict[str, str]]:
    overrides = MANUAL_DEFAULTS.copy()
    overrides.update(load_manual_overrides(RAW_DIR / 'manual_inputs.csv'))

    curve = load_fred_series(SERIES['curve10y3m'])
    unrate = load_fred_series(SERIES['unrate'])
    sofr = load_fred_series(SERIES['sofr'])
    iorb = load_fred_series(SERIES['iorb'])
    custody = load_fred_series(SERIES['custody'])
    dxy = load_fred_series(SERIES['dxy_broad'])
    y10 = load_fred_series(SERIES['yield10'])
    y2 = load_fred_series(SERIES['yield2'])
    reserves = load_fred_series(SERIES['reserve_balances'])

    rrp = try_rrp_usd_bn(latest_date(curve))
    if rrp is None:
        rrp = float(overrides['rrp_usd_bn'])

    raw = {
        'curva_spread': latest_value(curve),
        'sahm_gap': sahm_gap([v for _, v in unrate]),
        'reservas_pct_min': reservas_pct_min(latest_value(reserves), float(overrides['reserve_floor'])),
        'rrp_usd_bn': rrp,
        'sofr_iorb_bp': sofr_iorb_bp(latest_value(sofr), latest_value(iorb)),
        'fra_ois_bp': float(overrides['fra_ois_bp']),
        'repo_stress_score': float(overrides['repo_stress_score']),
        'vol_yields_20d_bp': vol_yields_20d_bp([v for _, v in y10]),
        'custody_12w_pct': custody_12w_pct([v for _, v in custody]),
        'tic_3m_usd_bn': float(overrides['tic_3m_usd_bn']),
        'usd_stress_score': usd_stress_score([v for _, v in dxy], [v for _, v in y10], [v for _, v in y2]),
        'nfci': load_nfci(),
    }
    raw['bloco_externo_score'] = external_block_score(raw['custody_12w_pct'], raw['tic_3m_usd_bn'], raw['usd_stress_score'])

    source_status = {
        'custody': 'ok',
        'tic': 'manual_proxy',
        'repo_stress': 'manual_score',
        'fra_ois': 'manual_proxy',
        'rrp': 'ok' if rrp != float(overrides['rrp_usd_bn']) else 'manual_fallback',
        'nfci': 'ok',
    }
    return raw, source_status


def run_pipeline() -> PipelineOutput:
    raw, source_status = collect_raw_data()
    statuses, ext_score = classify(raw)
    model = compute_model(PRIOR, SIGNALS, raw, statuses)
    run_date = str(date.today())

    external_block = {
        'custody_12w_pct': raw['custody_12w_pct'],
        'tic_3m_usd_bn': raw['tic_3m_usd_bn'],
        'usd_stress_score': raw['usd_stress_score'],
        'composite_score': ext_score,
        'status': statuses['bloco_externo'],
    }

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(DB_PATH)
    run_id = insert_run(conn, run_date, model, external_block)
    history = fetch_history(conn, limit=104)

    latest_payload = {
        'run_id': run_id,
        'run_date': run_date,
        'prior': model['prior'],
        'posterior': model['posterior'],
        'risk_label': model['risk_label'],
        'signals': model['signals'],
        'external_block': external_block,
        'raw_data': raw,
        'source_status': source_status,
    }
    export_latest(LATEST_JSON, latest_payload)
    export_history(HISTORY_JSON, history)
    return PipelineOutput(latest_payload=latest_payload, history_payload=history)


if __name__ == '__main__':
    out = run_pipeline()
    print(f"[OK] latest.json: {LATEST_JSON}")
    print(f"[OK] history.json: {HISTORY_JSON}")
    print(f"[OK] posterior: {out.latest_payload['posterior']:.4f}")
