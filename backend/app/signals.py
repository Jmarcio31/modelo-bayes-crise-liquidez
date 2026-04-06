from __future__ import annotations

from typing import Dict, Tuple

from .config import SIGNALS


def _classify_value(value: float, thresholds: dict) -> str:
    active_gte = thresholds.get("active_gte")
    active_lte = thresholds.get("active_lte")
    contrary_gte = thresholds.get("contrary_gte")
    contrary_lte = thresholds.get("contrary_lte")

    # Primeiro verifica se é ATIVO (prioridade mais alta)
    if active_gte is not None and value >= active_gte:
        return "ATIVO"
    if active_lte is not None and value <= active_lte:
        return "ATIVO"
    
    # Depois verifica se é CONTRÁRIO
    if contrary_gte is not None and value >= contrary_gte:
        return "CONTRARIO"
    if contrary_lte is not None and value <= contrary_lte:
        return "CONTRARIO"
    
    return "NEUTRO"


def classify(raw: Dict[str, float]) -> Tuple[Dict[str, str], float]:
    statuses: Dict[str, str] = {}
    ext_score = float(raw.get("bloco_externo_score", 0.0))

    for signal in SIGNALS:
        raw_key = signal["raw_key"]
        value = float(raw.get(raw_key, 0.0))
        status = _classify_value(value, signal.get("thresholds", {}))
        statuses[signal["id"]] = status

    return statuses, ext_score




