from __future__ import annotations

import csv
import io
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_GRAPH_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
NYFED_RRP_JSON = "https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json?startDate={date}&endDate={date}"

REQUEST_TIMEOUT = 60
REQUEST_ATTEMPTS = 3
BACKOFF_SECONDS = 2

# Nota: o dict SERIES canônico está em config.py e é importado por main.py.
# Este dict local é mantido apenas para load_nfci(), que precisa de NFCI
# sem importar config.py (evita dependência circular).
_NFCI_SERIES_ID = "NFCI"


class SourceError(Exception):
    pass


def get_fred_api_key() -> str:
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise SourceError("FRED_API_KEY nao encontrada no ambiente")
    return key


def _safe_float(value):
    try:
        if value in (None, "", "."):
            return None
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _get_with_retry(url: str, *, params=None, attempts: int = REQUEST_ATTEMPTS, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    last_exc = None
    headers = {"User-Agent": "Mozilla/5.0"}
    for i in range(attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            last_exc = e
            if i < attempts - 1:
                time.sleep(BACKOFF_SECONDS * (i + 1))
    raise last_exc


def load_fred_series(series_id: str) -> List[Tuple[str, float]]:
    params = {
        "series_id": series_id,
        "api_key": get_fred_api_key(),
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": "2000-01-01",
    }

    try:
        r = _get_with_retry(FRED_URL, params=params)
        data = r.json()
        obs = data.get("observations", [])

        out: List[Tuple[str, float]] = []
        for row in obs:
            val = _safe_float(row.get("value"))
            if val is None:
                continue
            out.append((row.get("date", ""), val))

        if out:
            return out

    except Exception:
        pass

    try:
        url = FRED_GRAPH_CSV.format(series_id=series_id)
        r = _get_with_retry(url)

        reader = csv.DictReader(io.StringIO(r.text))
        fields = reader.fieldnames or []

        if len(fields) < 2:
            raise SourceError(f"CSV invalido para {series_id}")

        date_col = fields[0]
        value_col = fields[1]

        out: List[Tuple[str, float]] = []
        for row in reader:
            val = _safe_float(row.get(value_col))
            if val is None:
                continue
            out.append((row.get(date_col, ""), val))

        if not out:
            raise SourceError(f"Serie vazia para {series_id}")

        return out

    except Exception as e:
        raise SourceError(f"Erro ao carregar serie {series_id}: {e}")


def latest_value(series: List[Tuple[str, float]]) -> float:
    if not series:
        raise SourceError("Serie vazia")
    return series[-1][1]


def latest_date(series: List[Tuple[str, float]]) -> str:
    if not series:
        raise SourceError("Serie vazia")
    return series[-1][0]


def load_nfci() -> float:
    series = load_fred_series(_NFCI_SERIES_ID)
    return latest_value(series)


def load_manual_overrides(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}

    out: Dict[str, float] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    if not rows:
        return {}

    for row in rows:
        key = row.get("chave") or row.get("key") or row.get("serie") or row.get("variavel")
        raw = row.get("valor") or row.get("value")
        if not key:
            continue
        val = _safe_float(raw)
        if val is None:
            continue
        out[key] = val

    return out


def try_rrp_usd_bn(date_hint: str | None = None):
    if not date_hint:
        return None
    url = NYFED_RRP_JSON.format(date=date_hint)
    try:
        response = _get_with_retry(url)
        payload = response.json()
    except Exception:
        return None

    total = None
    if isinstance(payload, dict):
        candidates = [payload]
        for key in ("repoOperations", "operations", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                candidates.extend([v for v in value if isinstance(v, dict)])

        for item in candidates:
            for field in ("totalAmtAccepted", "totalSubmitted", "accepted", "amountAccepted"):
                raw = item.get(field)
                num = _safe_float(raw)
                if num is not None:
                    total = num
                    break
            if total is not None:
                break

    if total is None:
        return None

    if total > 1e9:
        return total / 1e9
    if total > 1e6:
        return total / 1e3
    return total
