from __future__ import annotations

import math
from typing import Dict, List


def _to_odds(p: float) -> float:
    return p / (1.0 - p)


def _from_odds(odds: float) -> float:
    return odds / (1.0 + odds)


def _risk_label(posterior: float) -> str:
    if posterior >= 0.70:
        return "Estresse elevado"
    if posterior >= 0.40:
        return "Estresse intermediario"
    return "Estresse contido"


def compute_model(prior: float, signals: List[dict], raw: Dict[str, float], statuses: Dict[str, str]) -> dict:
    log_odds = math.log(_to_odds(prior))
    rows = []

    for signal in signals:
        status = statuses.get(signal["id"], "NEUTRO")
        p_e_h = float(signal["p_e_h"])
        p_e_not_h = float(signal["p_e_not_h"])
        weight = float(signal["weight"])

        risk_lr = p_e_h / p_e_not_h
        reverse_lr = (1.0 - p_e_h) / (1.0 - p_e_not_h)

        if status == "ATIVO":
            lr_used = risk_lr
            log_contrib = weight * math.log(risk_lr)
        elif status == "CONTRARIO":
            lr_used = reverse_lr
            log_contrib = weight * math.log(reverse_lr)
        else:
            lr_used = 1.0
            log_contrib = 0.0

        log_odds += log_contrib

        rows.append({
            "signal_id": signal["id"],
            "signal_name": signal["signal_name"],
            "block": signal["block"],
            "raw_value": float(raw.get(signal["raw_key"], 0.0)),
            "status": status,
            "weight": weight,
            "p_e_h": p_e_h,
            "p_e_not_h": p_e_not_h,
            "lr_used": lr_used,
            "log_contrib": log_contrib,
        })

    posterior = _from_odds(math.exp(log_odds))

    return {
        "prior": prior,
        "posterior": posterior,
        "risk_label": _risk_label(posterior),
        "signals": rows,
    }
