from __future__ import annotations

import math
from typing import Any


def to_odds(p: float) -> float:
    return p / (1 - p)


def from_odds(odds: float) -> float:
    return odds / (1 + odds)


def label_from_posterior(p: float) -> str:
    if p >= 0.7:
        return 'Estresse elevado'
    if p >= 0.4:
        return 'Estresse intermediario'
    return 'Estresse contido'


def compute_model(prior: float, signals_cfg: list[dict[str, Any]], raw: dict[str, float], statuses: dict[str, str]) -> dict[str, Any]:
    log_odds = math.log(to_odds(prior))
    results = []
    for cfg in signals_cfg:
        status = statuses[cfg['id']]
        p_e_h = cfg['p_e_h']
        p_e_not_h = cfg['p_e_not_h']
        risk_lr = p_e_h / p_e_not_h
        reverse_lr = (1 - p_e_h) / (1 - p_e_not_h)
        if status == 'ATIVO':
            lr_used = risk_lr
            log_contrib = cfg['weight'] * math.log(risk_lr)
        elif status == 'CONTRARIO':
            lr_used = reverse_lr
            log_contrib = cfg['weight'] * math.log(reverse_lr)
        else:
            lr_used = 1.0
            log_contrib = 0.0
        log_odds += log_contrib
        results.append(
            {
                'signal_id': cfg['id'],
                'signal_name': cfg['name'],
                'block': cfg['block'],
                'raw_value': raw[cfg['raw_key']],
                'status': status,
                'weight': cfg['weight'],
                'p_e_h': p_e_h,
                'p_e_not_h': p_e_not_h,
                'lr_used': lr_used,
                'log_contrib': log_contrib,
            }
        )
    posterior = from_odds(math.exp(log_odds))
    return {
        'prior': prior,
        'posterior': posterior,
        'risk_label': label_from_posterior(posterior),
        'signals': results,
    }
