from __future__ import annotations

import csv
import io
import os
from pathlib import Path
from typing import Optional

import requests

TIMEOUT = 30
FRED_API_URL = 'https://api.stlouisfed.org/fred/series/observations'
RRP_JSON = 'https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json?startDate={date}&endDate={date}'

SERIES = {
    'curve10y3m': 'T10Y3M',
    'unrate': 'UNRATE',
    'sofr': 'SOFR',
    'iorb': 'IORB',
    'custody': 'WSEFINT1',
    'dxy_broad': 'DTWEXBGS',
    'yield10': 'DGS10',
    'yield2': 'DGS2',
    'reserve_balances': 'WRESBAL',
}


class SourceError(RuntimeError):
    pass


def _get_text(url: str) -> str:
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def get_fred_api_key() -> str:
    api_key = os.getenv('FRED_API_KEY', '').strip()
    if not api_key:
        raise SourceError('FRED_API_KEY nao encontrada no ambiente')
    return api_key


def load_fred_series(series_id: str) -> list[tuple[str, float]]:
    params = {
        'series_id': series_id,
        'api_key': get_fred_api_key(),
        'file_type': 'json',
        'sort_order': 'asc',
        'observation_start': '2000-01-01',
    }
    response = requests.get(FRED_API_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    observations = payload.get('observations')
    if not isinstance(observations, list):
        raise SourceError(f'Resposta invalida da API FRED para {series_id}')

    rows: list[tuple[str, float]] = []
    for obs in observations:
        if not isinstance(obs, dict):
            continue
        raw = obs.get('value')
        if raw in (None, '', '.'):
            continue
        try:
            rows.append((str(obs.get('date', '')), float(raw)))
        except (TypeError, ValueError):
            continue

    if not rows:
        raise SourceError(f'Sem dados numericos na API FRED para {series_id}')
    return rows


def latest_value(series: list[tuple[str, float]]) -> float:
    return series[-1][1]


def latest_date(series: list[tuple[str, float]]) -> str:
    return series[-1][0]


def load_nfci() -> float:
    return latest_value(load_fred_series('NFCI'))


def try_rrp_usd_bn(date_hint: str) -> Optional[float]:
    try:
        r = requests.get(RRP_JSON.format(date=date_hint), timeout=TIMEOUT)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return None
    candidates = []
    if isinstance(payload, dict):
        candidates.append(payload)
        for key in ('repoOperations', 'operations', 'data'):
            value = payload.get(key)
            if isinstance(value, list):
                candidates.extend([v for v in value if isinstance(v, dict)])
    for item in candidates:
        for field in ('totalAmtAccepted', 'totalSubmitted', 'accepted', 'amountAccepted'):
            raw = item.get(field)
            if raw is None:
                continue
            try:
                total = float(raw)
                if total > 1e9:
                    return total / 1e9
                if total > 1e6:
                    return total / 1e3
                return total
            except Exception:
                continue
    return None


def load_manual_overrides(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    reader = csv.DictReader(path.read_text(encoding='utf-8-sig').splitlines())
    out: dict[str, float] = {}
    for row in reader:
        key = row.get('chave') or row.get('key') or row.get('variavel')
        val = row.get('valor') or row.get('value')
        if not key or val in (None, ''):
            continue
        try:
            out[key] = float(str(val).replace(',', ''))
        except ValueError:
            continue
    return out
