"""
update_sp500.py
===============
Preenche a coluna sp500 nas runs existentes no banco que foram inseridas
antes da coluna existir. Carrega a série SP500 do FRED uma vez e faz
UPDATE em cada run_date com o valor disponível até aquela data.

Uso:
    python update_sp500.py
    python update_sp500.py --dry-run

Variáveis de ambiente:
    FRED_API_KEY  — obrigatória
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_DB = Path(__file__).resolve().parent / "backend" / "data" / "liquidez.db"


def get_fred_key() -> str:
    key = os.getenv("FRED_API_KEY", "")
    if not key:
        sys.exit("ERRO: FRED_API_KEY não definida.")
    return key


def fetch_sp500(start: str = "2019-01-01") -> list[tuple[str, float]]:
    params = {
        "series_id": "SP500",
        "api_key": get_fred_key(),
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": start,
    }
    for attempt in range(3):
        try:
            r = requests.get(FRED_URL, params=params, timeout=60)
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
                print(f"  tentativa {attempt+1} falhou: {exc}")
                time.sleep(3)
            else:
                sys.exit(f"ERRO: não foi possível carregar SP500: {exc}")


def as_of(series: list[tuple[str, float]], ref: str) -> float | None:
    result = None
    for d, v in series:
        if d <= ref:
            result = v
        else:
            break
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",      default=str(DEFAULT_DB))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        sys.exit(f"ERRO: banco não encontrado em {db_path}")

    conn = sqlite3.connect(db_path)

    # Verifica se coluna existe — se não, cria
    cols = [r[1] for r in conn.execute("PRAGMA table_info(runs)").fetchall()]
    if "sp500" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN sp500 REAL")
        conn.commit()
        print("Coluna sp500 adicionada ao banco.")

    # Datas que precisam de SP500
    rows = conn.execute("""
        SELECT DISTINCT run_date FROM runs
        WHERE sp500 IS NULL
        ORDER BY run_date
    """).fetchall()
    dates = [r[0] for r in rows]
    print(f"\nDatas sem SP500: {len(dates)}")
    if not dates:
        print("Nada a fazer.")
        return

    # Carrega SP500 do FRED
    print("Carregando SP500 do FRED...")
    sp500_series = fetch_sp500(start="2018-01-01")
    print(f"  {len(sp500_series)} observações carregadas")

    # UPDATE por data
    updated = skipped = 0
    for run_date in dates:
        val = as_of(sp500_series, run_date)
        if val is None:
            skipped += 1
            continue
        if not args.dry_run:
            conn.execute(
                "UPDATE runs SET sp500=? WHERE run_date=? AND sp500 IS NULL",
                (val, run_date)
            )
        print(f"  {run_date}  SP500={val:.2f}" + (" [DRY]" if args.dry_run else ""))
        updated += 1

    if not args.dry_run:
        conn.commit()

    print(f"\n{'='*50}")
    print(f"Concluído: {updated} atualizados | {skipped} sem dado disponível")
    n_filled = conn.execute("SELECT COUNT(*) FROM runs WHERE sp500 IS NOT NULL").fetchone()[0]
    print(f"Total de runs com SP500 preenchido: {n_filled}")
    conn.close()


if __name__ == "__main__":
    main()
