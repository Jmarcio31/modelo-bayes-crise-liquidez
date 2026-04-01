from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
BACKEND_DIR = BASE_DIR / 'backend'
FRONTEND_DIR = BASE_DIR / 'frontend'
DATA_DIR = BACKEND_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
DB_PATH = DATA_DIR / 'liquidez.db'
LATEST_JSON = Path("data/latest.json")
HISTORY_JSON = Path("data/history.json")

PRIOR = 0.10

SIGNALS = [
    {
        'id': 'curva',
        'name': 'Desinversao da curva apos inversao profunda',
        'block': 'Macro',
        'weight': 0.55,
        'p_e_h': 0.72,
        'p_e_not_h': 0.42,
        'raw_key': 'curva_spread',
    },
    {
        'id': 'sahm',
        'name': 'Sahm Rule / deterioracao do mercado de trabalho',
        'block': 'Macro',
        'weight': 0.55,
        'p_e_h': 0.68,
        'p_e_not_h': 0.38,
        'raw_key': 'sahm_gap',
    },
    {
        'id': 'reservas',
        'name': 'Reservas bancarias em suporte',
        'block': 'Liquidez domestica',
        'weight': 0.80,
        'p_e_h': 0.83,
        'p_e_not_h': 0.33,
        'raw_key': 'reservas_pct_min',
    },
    {
        'id': 'rrp',
        'name': 'Reverse Repo Facility comprimido',
        'block': 'Liquidez domestica',
        'weight': 0.65,
        'p_e_h': 0.74,
        'p_e_not_h': 0.41,
        'raw_key': 'rrp_usd_bn',
    },
    {
        'id': 'sofr_iorb',
        'name': 'Spread SOFR - IORB',
        'block': 'Funding stress',
        'weight': 0.85,
        'p_e_h': 0.82,
        'p_e_not_h': 0.29,
        'raw_key': 'sofr_iorb_bp',
    },
    {
        'id': 'fra_ois',
        'name': 'FRA-OIS proxy publica',
        'block': 'Funding stress',
        'weight': 0.85,
        'p_e_h': 0.79,
        'p_e_not_h': 0.31,
        'raw_key': 'fra_ois_bp',
    },
    {
        'id': 'repo',
        'name': 'Repo stress / spikes',
        'block': 'Funding stress',
        'weight': 0.95,
        'p_e_h': 0.88,
        'p_e_not_h': 0.24,
        'raw_key': 'repo_stress_score',
    },
    {
        'id': 'vol_yields',
        'name': 'Volatilidade de yields (proxy publica do MOVE)',
        'block': 'Mercado de juros',
        'weight': 0.65,
        'p_e_h': 0.70,
        'p_e_not_h': 0.39,
        'raw_key': 'vol_yields_20d_bp',
    },
    {
        'id': 'bloco_externo',
        'name': 'Bloco externo: Custody + TIC + stress em dolar',
        'block': 'Externo',
        'weight': 0.85,
        'p_e_h': 0.84,
        'p_e_not_h': 0.30,
        'raw_key': 'bloco_externo_score',
    },
    {
        'id': 'nfci',
        'name': 'National Financial Conditions Index',
        'block': 'Condicoes financeiras',
        'weight': 0.60,
        'p_e_h': 0.69,
        'p_e_not_h': 0.40,
        'raw_key': 'nfci',
    },
]

MANUAL_DEFAULTS = {
    'fra_ois_bp': 29.0,
    'repo_stress_score': 0.66,
    'tic_3m_usd_bn': -96.0,
    'rrp_usd_bn': 145.0,
    'reserve_floor': 3000000.0,
}
