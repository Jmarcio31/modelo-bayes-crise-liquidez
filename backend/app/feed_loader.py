from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FeedEntry:
    key: str
    value: float
    as_of_date: str
    source: str
    method: str
    quality_flag: str


def load_data_feed(path: Path) -> dict[str, FeedEntry]:
    if not path.exists():
        return {}

    rows: dict[str, FeedEntry] = {}
    with path.open('r', encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row.get('key') or row.get('chave') or '').strip()
            raw = row.get('value') or row.get('valor')
            if not key or raw in (None, ''):
                continue
            try:
                value = float(str(raw).replace(',', ''))
            except ValueError:
                continue
            rows[key] = FeedEntry(
                key=key,
                value=value,
                as_of_date=(row.get('as_of_date') or '').strip(),
                source=(row.get('source') or '').strip(),
                method=(row.get('method') or '').strip(),
                quality_flag=(row.get('quality_flag') or '').strip(),
            )
    return rows


def feed_entries_to_status(entries: dict[str, FeedEntry]) -> dict[str, str]:
    mapping = {
        'tic_3m_usd_bn': 'tic',
        'repo_stress_score': 'repo_stress',
        'fra_ois_bp': 'fra_ois',
        'rrp_usd_bn': 'rrp',
        'usd_stress_score': 'usd_stress',
    }
    out: dict[str, str] = {}
    for key, alias in mapping.items():
        entry = entries.get(key)
        if entry is None:
            continue
        parts = [p for p in [entry.method, entry.quality_flag] if p]
        out[alias] = '|'.join(parts) if parts else 'feed'
    return out


def feed_entries_to_meta(entries: dict[str, FeedEntry]) -> dict[str, dict[str, str | float]]:
    out: dict[str, dict[str, str | float]] = {}
    for key, entry in entries.items():
        out[key] = {
            'value': entry.value,
            'as_of_date': entry.as_of_date,
            'source': entry.source,
            'method': entry.method,
            'quality_flag': entry.quality_flag,
        }
    return out
