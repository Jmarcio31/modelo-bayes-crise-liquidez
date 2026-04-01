from __future__ import annotations


def classify(raw: dict[str, float]) -> tuple[dict[str, str], float]:
    ext_score = raw['bloco_externo_score']
    rules = {
        'curva': 'ATIVO' if raw['curva_spread'] > 0.15 else 'CONTRARIO' if raw['curva_spread'] < -0.25 else 'NEUTRO',
        'sahm': 'ATIVO' if raw['sahm_gap'] >= 0.5 else 'CONTRARIO' if raw['sahm_gap'] <= 0.2 else 'NEUTRO',
        'reservas': 'ATIVO' if raw['reservas_pct_min'] <= 1.03 else 'CONTRARIO' if raw['reservas_pct_min'] >= 1.12 else 'NEUTRO',
        'rrp': 'ATIVO' if raw['rrp_usd_bn'] <= 200 else 'CONTRARIO' if raw['rrp_usd_bn'] >= 450 else 'NEUTRO',
        'sofr_iorb': 'ATIVO' if raw['sofr_iorb_bp'] >= 6 else 'CONTRARIO' if raw['sofr_iorb_bp'] <= 2 else 'NEUTRO',
        'fra_ois': 'ATIVO' if raw['fra_ois_bp'] >= 25 else 'CONTRARIO' if raw['fra_ois_bp'] <= 10 else 'NEUTRO',
        'repo': 'ATIVO' if raw['repo_stress_score'] >= 0.65 else 'CONTRARIO' if raw['repo_stress_score'] <= 0.3 else 'NEUTRO',
        'vol_yields': 'ATIVO' if raw['vol_yields_20d_bp'] >= 12 else 'CONTRARIO' if raw['vol_yields_20d_bp'] <= 6 else 'NEUTRO',
        'bloco_externo': 'ATIVO' if ext_score >= 0.6 else 'CONTRARIO' if ext_score <= 0.3 else 'NEUTRO',
        'nfci': 'ATIVO' if raw['nfci'] >= 0.35 else 'CONTRARIO' if raw['nfci'] <= -0.25 else 'NEUTRO',
    }
    return rules, ext_score
