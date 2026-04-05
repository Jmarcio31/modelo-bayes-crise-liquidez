from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = '''
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    prior REAL NOT NULL,
    posterior REAL NOT NULL,
    risk_label TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sp500 REAL
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
'''

# Migration: adiciona coluna sp500 se não existir (banco pré-existente)
_MIGRATION = "ALTER TABLE runs ADD COLUMN sp500 REAL"


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    # Migração: adiciona coluna sp500 em bancos pré-existentes
    try:
        conn.execute(_MIGRATION)
        conn.commit()
    except Exception:
        pass  # coluna já existe
    return conn


def insert_run(conn: sqlite3.Connection, run_date: str, model_result: dict[str, Any], external_block: dict[str, Any], sp500: float | None = None) -> int:
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO runs (run_date, prior, posterior, risk_label, created_at, sp500) VALUES (?, ?, ?, ?, ?, ?)',
        (run_date, model_result['prior'], model_result['posterior'], model_result['risk_label'],
         datetime.now(timezone.utc).isoformat(), sp500),
    )
    run_id = cur.lastrowid
    for s in model_result['signals']:
        cur.execute(
            '''
            INSERT INTO signal_results
            (run_id, signal_id, signal_name, bloco, raw_value, status, weight, p_e_h, p_e_not_h, lr_used, log_contrib)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (run_id, s['signal_id'], s['signal_name'], s['block'], s['raw_value'], s['status'], s['weight'], s['p_e_h'], s['p_e_not_h'], s['lr_used'], s['log_contrib']),
        )
    cur.execute(
        '''
        INSERT INTO external_block_details
        (run_id, custody_12w_pct, tic_3m_usd_bn, usd_stress_score, composite_score, status)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (
            run_id,
            external_block['custody_12w_pct'],
            external_block['tic_3m_usd_bn'],
            external_block['usd_stress_score'],
            external_block['composite_score'],
            external_block['status'],
        ),
    )
    conn.commit()
    return int(run_id)


def fetch_history(conn: sqlite3.Connection, limit: int = 520) -> list[dict[str, Any]]:
    """
    Retorna o último registro de cada data, ordenado cronologicamente.
    Inclui sp500 para sobreposição visual no gráfico histórico.
    """
    cur = conn.cursor()
    cur.execute('''
        SELECT run_date, posterior, risk_label, sp500
        FROM runs
        WHERE id IN (
            SELECT MAX(id) FROM runs GROUP BY run_date
        )
        ORDER BY run_date ASC
        LIMIT ?
    ''', (limit,))
    return [dict(r) for r in cur.fetchall()]
