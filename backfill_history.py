"""
backfill_history.py
===================
Reconstrói o histórico semanal do modelo bayesiano de risco de liquidez
desde 2019-01-04 (primeira sexta do ano) até hoje.

Uso:
    python backfill_history.py
    python backfill_history.py --start 2020-01-01
    python backfill_history.py --dry-run          # simula sem gravar no banco
    python backfill_history.py --skip-existing    # pula datas já no banco

Variáveis de ambiente:
    FRED_API_KEY  chave da API FRED (obrigatória)

Decisões de design:
    - IORB só existe desde jun/2021. Antes usa IOER (mesmo significado econômico).
    - RRP histórico via RRPONTSYD (H.4.1 FRED) em vez da API do NY Fed.
    - Granularidade semanal (sextas-feiras).
    - Séries carregadas uma vez e fatiadas por data — minimiza chamadas à API.
    - Inserção compatível com o schema do storage.py existente.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

# ── Configuração ──────────────────────────────────────────────────────────────
FRED_URL        = "https://api.stlouisfed.org/fred/series/observations"
REQUEST_TIMEOUT = 60
BACKOFF         = 2

SERIES = {
    "curve10y3m": "T10Y3M",
    "unrate":     "UNRATE",
    "sofr":       "SOFR",
    "iorb":       "IORB",       # desde jun/2021
    "ioer":       "IOER",       # até mar/2021 — fallback para IORB
    "effr":       "EFFR",
    "yield10":    "DGS10",
    "yield2":     "DGS2",
    "dxy_broad":  "DTWEXBGS",
    "custody":    "WTREGEN",
    "nfci":       "NFCI",
    "stlfsi4":    "STLFSI4",
    "dpcredit":   "DPCREDIT",
    "cp3m_fin":   "CPF3M",
    "cp3m_nonfin":"CPN3M",
    "hy_oas":     "BAMLH0A0HYM2",
    "reserve_bal":"WRESBAL",
    "rrp_h41":    "RRPONTSYD",  # RRP histórico via H.4.1 (bilhões USD)
    "sp500":      "SP500",      # S&P500 — sobreposição visual
}

def _repo_root() -> Path:
    """
    Localiza a raiz do repositório de forma robusta no Windows e Linux.
    Tenta primeiro o diretório do script, depois o diretório de trabalho atual.
    """
    # Opção 1: diretório onde o script está fisicamente
    try:
        script_dir = Path(__file__).resolve().parent
        if (script_dir / "backend" / "app" / "model_config.json").exists():
            return script_dir
    except Exception:
        pass
    # Opção 2: diretório de trabalho atual (funciona com `cd` + execução relativa)
    cwd = Path.cwd()
    if (cwd / "backend" / "app" / "model_config.json").exists():
        return cwd
    # Opção 3: retorna cwd mesmo assim e deixa o erro aparecer com caminho claro
    return cwd

_ROOT   = _repo_root()
DEFAULT_DB  = _ROOT / "backend" / "data" / "liquidez.db"
DEFAULT_CFG = _ROOT / "backend" / "app" / "model_config.json"


# ── FRED loader ───────────────────────────────────────────────────────────────
def get_fred_key() -> str:
    key = os.getenv("FRED_API_KEY", "")
    if not key:
        sys.exit("ERRO: variável de ambiente FRED_API_KEY não definida.")
    return key


def fetch_fred(series_id: str, start: str = "2018-01-01") -> list[tuple[str, float]]:
    params = {
        "series_id": series_id,
        "api_key": get_fred_key(),
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": start,
    }
    for attempt in range(3):
        try:
            r = requests.get(FRED_URL, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            out = []
            for row in r.json().get("observations", []):
                try:
                    out.append((row["date"], float(row["value"])))
                except (ValueError, KeyError):
                    pass
            return out
        except Exception as exc:
            if attempt < 2:
                print(f"    tentativa {attempt+1} falhou ({exc}), aguardando...")
                time.sleep(BACKOFF * (attempt + 1))
            else:
                print(f"    AVISO: {series_id} não carregou. Usando série vazia.")
                return []


def as_of(series: list[tuple[str, float]], ref: str) -> Optional[float]:
    """Último valor disponível até ref (inclusive)."""
    result = None
    for d, v in series:
        if d <= ref:
            result = v
        else:
            break
    return result


def window_up_to(series: list[tuple[str, float]], ref: str, n: int) -> list[float]:
    vals = [v for d, v in series if d <= ref]
    return vals[-n:] if vals else []


# ── Transforms (standalone) ───────────────────────────────────────────────────
def clamp(v, lo, hi): return min(hi, max(lo, v))

def mean(vals):
    vals = list(vals)
    return sum(vals) / len(vals) if vals else 0.0

def stddev(vals):
    vals = list(vals)
    if len(vals) < 2: return 0.0
    m = mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / len(vals))

def sahm_gap(unrate_vals):
    if len(unrate_vals) < 12: return 0.0
    return mean(unrate_vals[-3:]) - min(unrate_vals[-12:])

def vol_yields_20d(y10_vals):
    w = y10_vals[-20:] if len(y10_vals) >= 20 else y10_vals
    return stddev(w) * 100.0

def custody_12w_pct(vals):
    if len(vals) < 13: return 0.0
    past = vals[-13]
    return ((vals[-1] / past) - 1.0) * 100.0 if past else 0.0

def reservas_pct_min(reserves, floor):
    return reserves / floor if floor else 0.0

def usd_stress_score(dxy_vals, y10_vals, y2_vals):
    dxy_chg = (((dxy_vals[-1] / dxy_vals[-21]) - 1.0) * 100.0
               if len(dxy_vals) >= 21 else 0.0)
    y10_vol = stddev(y10_vals[-20:]) if len(y10_vals) >= 20 else 0.0
    slope   = (y10_vals[-1] - y2_vals[-1]) if y10_vals and y2_vals else 0.0
    return (0.45 * clamp(dxy_chg / 4.0, 0, 1) +
            0.35 * clamp(y10_vol / 0.18, 0, 1) +
            0.20 * clamp((0.5 - slope) / 1.5, 0, 1))

def external_block_score(custody_12w, tic_3m, usd_score):
    return (0.4 * clamp((-custody_12w) / 8.0, 0, 1) +
            0.3 * clamp((-tic_3m) / 150.0, 0, 1) +
            0.3 * clamp(usd_score, 0, 1))

def fra_ois_proxy(cp_fin, cp_nonfin, sofr_v):
    return clamp((0.6 * cp_fin + 0.4 * cp_nonfin - sofr_v) * 100.0, 0.0, 120.0)

def repo_stress_proxy(sofr_window):
    """Proxy histórica: usa volatilidade do SOFR como indicador de stress."""
    vol = stddev(sofr_window) if len(sofr_window) >= 5 else 0.0
    return clamp(0.3 * clamp(vol / 0.10, 0, 1), 0.0, 1.0)

def stlfsi4_stress(v):
    return clamp(1.0 / (1.0 + math.exp(-0.6 * v)), 0.0, 1.0)

def discount_window_bn(dpcredit_millions):
    return clamp(dpcredit_millions / 1000.0, 0.0, 1e6)

def tic_proxy(dxy_vals, curve_vals):
    dxy_chg = (((dxy_vals[-1] / dxy_vals[-21]) - 1.0) * 100.0
               if len(dxy_vals) >= 21 else 0.0)
    curve_level = curve_vals[-1] if curve_vals else 0.0
    return clamp(-(60.0 + 10.0 * max(dxy_chg, 0.0) + 20.0 * max(curve_level, 0.0)),
                 -180.0, -20.0)

def private_credit_proxy(hy_oas_val, repo_score, usd_score):
    hy_stress = clamp((hy_oas_val - 4.0) / 3.0, 0.0, 1.0)
    funding   = 0.5 * repo_score + 0.5 * usd_score
    return clamp(0.35 * hy_stress + 0.35 * funding + 0.30 * 0.5, 0.0, 1.0)

def effr_iorb_ma5(effr_vals, iorb):
    w = effr_vals[-5:] if len(effr_vals) >= 5 else effr_vals
    if not w: return 0.0
    return (mean(w) - iorb) * 100.0


# ── Classificação ─────────────────────────────────────────────────────────────
def classify_signal(value, thresholds):
    if thresholds.get("active_gte") is not None and value >= thresholds["active_gte"]:
        return "ATIVO"
    if thresholds.get("active_lte") is not None and value <= thresholds["active_lte"]:
        return "ATIVO"
    if thresholds.get("contrary_gte") is not None and value >= thresholds["contrary_gte"]:
        return "CONTRARIO"
    if thresholds.get("contrary_lte") is not None and value <= thresholds["contrary_lte"]:
        return "CONTRARIO"
    return "NEUTRO"


# ── Prior dinâmico ────────────────────────────────────────────────────────────
def _sigmoid(x):
    try: return 1.0 / (1.0 + math.exp(-x))
    except OverflowError: return 0.0 if x < 0 else 1.0

def compute_prior(raw, prior_config):
    base  = float(prior_config.get("base", 0.12))
    min_v = float(prior_config.get("min",  base))
    max_v = float(prior_config.get("max",  base))
    prior = base
    for rule in prior_config.get("rules", []):
        value    = float(raw.get(rule["raw_key"], 0.0))
        threshold = float(rule["threshold"])
        add_max  = rule.get("add_max")
        smooth_k = rule.get("smooth_k")
        if add_max is not None and smooth_k is not None:
            add_max, smooth_k = float(add_max), float(smooth_k)
            if rule["kind"] == "gte":
                prior += add_max * _sigmoid(smooth_k * (value - threshold))
            else:
                prior += add_max * _sigmoid(smooth_k * (threshold - value))
        else:
            add = float(rule.get("add", 0.0))
            hit = ((rule["kind"] == "gte" and value >= threshold) or
                   (rule["kind"] == "lte" and value <= threshold))
            if hit:
                prior += add
    return clamp(prior, min_v, max_v)


# ── Motor bayesiano ───────────────────────────────────────────────────────────
def compute_model(prior, signals_cfg, raw):
    log_odds = math.log(prior / (1.0 - prior))
    rows = []
    for s in signals_cfg:
        value  = float(raw.get(s["raw_key"], 0.0))
        status = classify_signal(value, s.get("thresholds", {}))
        w, peh, penh = float(s["weight"]), float(s["p_e_h"]), float(s["p_e_not_h"])
        lr_r   = peh / penh
        lr_rev = (1 - peh) / (1 - penh)
        if status == "ATIVO":
            lr_used, lc = lr_r, w * math.log(lr_r)
        elif status == "CONTRARIO":
            lr_used, lc = lr_rev, w * math.log(lr_rev)
        else:
            lr_used, lc = 1.0, 0.0
        log_odds += lc
        rows.append({
            "signal_id":   s["id"],
            "signal_name": s["signal_name"],
            "block":       s["block"],
            "raw_value":   value,
            "status":      status,
            "weight":      w,
            "p_e_h":       peh,
            "p_e_not_h":   penh,
            "lr_used":     lr_used,
            "log_contrib": lc,
        })
    posterior = math.exp(log_odds) / (1.0 + math.exp(log_odds))
    label = ("Estresse elevado"      if posterior >= 0.70 else
             "Estresse intermediario" if posterior >= 0.40 else
             "Estresse contido")
    return {"prior": prior, "posterior": posterior, "risk_label": label, "signals": rows}


# ── Storage (compatível com storage.py existente) ─────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    prior REAL NOT NULL,
    posterior REAL NOT NULL,
    risk_label TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS signal_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    signal_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    bloco TEXT NOT NULL,
    raw_value REAL,
    status TEXT NOT NULL,
    weight REAL NOT NULL,
    p_e_h REAL NOT NULL,
    p_e_not_h REAL NOT NULL,
    lr_used REAL NOT NULL,
    log_contrib REAL NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS external_block_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    custody_12w_pct REAL,
    tic_3m_usd_bn REAL,
    usd_stress_score REAL,
    composite_score REAL,
    status TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);
"""

def db_connect(path):
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn

def db_existing_dates(conn):
    return {r[0] for r in conn.execute("SELECT DISTINCT run_date FROM runs").fetchall()}

def db_insert(conn, run_date, model, ext_block, sp500=None):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO runs (run_date, prior, posterior, risk_label, created_at, sp500) VALUES (?,?,?,?,?,?)",
        (run_date, model["prior"], model["posterior"], model["risk_label"],
         datetime.now(timezone.utc).isoformat(), sp500)
    )
    run_id = cur.lastrowid
    for s in model["signals"]:
        cur.execute(
            """INSERT INTO signal_results
               (run_id,signal_id,signal_name,bloco,raw_value,status,
                weight,p_e_h,p_e_not_h,lr_used,log_contrib)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (run_id, s["signal_id"], s["signal_name"], s["block"],
             s["raw_value"], s["status"], s["weight"],
             s["p_e_h"], s["p_e_not_h"], s["lr_used"], s["log_contrib"])
        )
    cur.execute(
        """INSERT INTO external_block_details
           (run_id,custody_12w_pct,tic_3m_usd_bn,usd_stress_score,composite_score,status)
           VALUES (?,?,?,?,?,?)""",
        (run_id, ext_block["custody_12w_pct"], ext_block["tic_3m_usd_bn"],
         ext_block["usd_stress_score"], ext_block["composite_score"], ext_block["status"])
    )
    conn.commit()
    return int(run_id)


# ── Geração de sextas-feiras ──────────────────────────────────────────────────
def fridays_between(start: date, end: date) -> list[date]:
    d = start
    while d.weekday() != 4:   # avança até a primeira sexta
        d += timedelta(days=1)
    days = []
    while d <= end:
        days.append(d)
        d += timedelta(weeks=1)
    return days


# ── Pipeline por data ─────────────────────────────────────────────────────────
def compute_for_date(ref_date: date, cache: dict,
                     signals_cfg: list, prior_config: dict,
                     reserve_floor: float) -> tuple[dict, dict]:
    d = ref_date.isoformat()

    def pt(key):
        v = as_of(cache[key], d)
        return v if v is not None else 0.0

    def wn(key, n):
        return window_up_to(cache[key], d, n)

    # IORB com fallback para IOER antes de jun/2021
    iorb_val = as_of(cache["iorb"], d)
    if iorb_val is None:
        iorb_val = as_of(cache["ioer"], d) or 0.0

    # Séries em janela
    dxy_vals   = wn("dxy_broad",  22)
    y10_vals   = wn("yield10",    22)
    y2_vals    = wn("yield2",      2)
    curve_vals = wn("curve10y3m",  2)
    sofr_w20   = wn("sofr",       20)
    effr_w5    = wn("effr",        5)
    unrate_w15 = wn("unrate",     15)
    custody_w14= wn("custody",    14)

    sofr_v     = pt("sofr")
    repo_score = repo_stress_proxy(sofr_w20)
    usd_score  = usd_stress_score(dxy_vals, y10_vals, y2_vals)
    tic_v      = tic_proxy(dxy_vals, curve_vals)
    custody_v  = custody_12w_pct(custody_w14)
    ext_score  = external_block_score(custody_v, tic_v, usd_score)

    raw = {
        "curva_spread":                pt("curve10y3m"),
        "sahm_gap":                    sahm_gap(unrate_w15),
        "reservas_pct_min":            reservas_pct_min(pt("reserve_bal"), reserve_floor),
        "rrp_usd_bn":                  pt("rrp_h41"),
        "sofr_iorb_bp":                (sofr_v - iorb_val) * 100.0,
        "fra_ois_bp":                  fra_ois_proxy(pt("cp3m_fin"), pt("cp3m_nonfin"), sofr_v),
        "repo_stress_score":           repo_score,
        "vol_yields_20d_bp":           vol_yields_20d(y10_vals),
        "custody_12w_pct":             custody_v,
        "tic_3m_usd_bn":               tic_v,
        "usd_stress_score":            usd_score,
        "nfci":                        pt("nfci"),
        "private_credit_stress_score": private_credit_proxy(pt("hy_oas"), repo_score, usd_score),
        "effr_iorb_bp":                effr_iorb_ma5(effr_w5, iorb_val),
        "stlfsi4_stress":              stlfsi4_stress(pt("stlfsi4")),
        "discount_window_bn":          discount_window_bn(pt("dpcredit")),
        "bloco_externo_score":         ext_score,
    }

    sp500_value = pt("sp500") or None

    prior = compute_prior(raw, prior_config)
    model = compute_model(prior, signals_cfg, raw)

    ext_status = classify_signal(ext_score, {"active_gte": 0.4, "contrary_lte": 0.2})
    ext_block  = {
        "custody_12w_pct":  custody_v,
        "tic_3m_usd_bn":    tic_v,
        "usd_stress_score": usd_score,
        "composite_score":  ext_score,
        "status":           ext_status,
    }
    return model, ext_block, sp500_value


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Backfill histórico semanal do modelo de liquidez")
    parser.add_argument("--start",         default="2019-01-01")
    parser.add_argument("--end",           default=date.today().isoformat())
    parser.add_argument("--db",            default=str(DEFAULT_DB))
    parser.add_argument("--config",        default=str(DEFAULT_CFG))
    parser.add_argument("--dry-run",       action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()

    # Carrega model_config
    cfg_path = Path(args.config)
    if not cfg_path.exists():
        sys.exit(f"ERRO: {cfg_path} não encontrado.")
    with cfg_path.open(encoding="utf-8") as f:
        cfg = json.load(f)
    signals_cfg   = cfg["signals"]
    prior_config  = cfg["prior_config"]
    reserve_floor = float(cfg.get("manual_defaults", {}).get("reserve_floor", 3_000_000.0))

    # Datas semanais
    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    dates = fridays_between(start, end)
    print(f"\nBackfill: {len(dates)} sextas de {dates[0]} a {dates[-1]}")
    if args.dry_run:
        print("  [DRY RUN — nenhuma gravação no banco]")

    # Banco
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn     = db_connect(db_path)
    existing = db_existing_dates(conn) if args.skip_existing else set()
    if existing:
        print(f"  {len(existing)} datas já no banco serão puladas")

    # Carrega todas as séries uma vez
    print("\nCarregando séries FRED...")
    cache = {}
    for key, sid in SERIES.items():
        print(f"  {sid:<20} ", end="", flush=True)
        data = fetch_fred(sid, start="2018-01-01")
        cache[key] = data
        print(f"{len(data)} obs")
        time.sleep(0.25)

    iorb_n = len(cache.get("iorb", []))
    ioer_n = len(cache.get("ioer", []))
    print(f"\n  IORB: {iorb_n} obs | IOER (fallback pré-2021): {ioer_n} obs\n")

    # Processa cada sexta
    inserted = skipped = errors = 0
    for ref_date in dates:
        d_str = ref_date.isoformat()
        if d_str in existing:
            skipped += 1
            continue
        try:
            model, ext_block, sp500_val = compute_for_date(
                ref_date, cache, signals_cfg, prior_config, reserve_floor
            )
            if not args.dry_run:
                db_insert(conn, d_str, model, ext_block, sp500=sp500_val)
            sp_str = f"  SP500={sp500_val:.0f}" if sp500_val else ""
            print(f"  {d_str}  {model['posterior']*100:5.1f}%  [{model['risk_label']}]"
                  f"  prior={model['prior']*100:.1f}%{sp_str}"
                  + ("  [DRY]" if args.dry_run else ""))
            inserted += 1
        except Exception as exc:
            print(f"  {d_str}  ERRO: {exc}")
            errors += 1

    print(f"\n{'='*55}")
    print(f"Concluído: {inserted} inseridos | {skipped} pulados | {errors} erros")
    if not args.dry_run and inserted > 0:
        total = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        print(f"Total de runs no banco: {total}")
    conn.close()


if __name__ == "__main__":
    main()
