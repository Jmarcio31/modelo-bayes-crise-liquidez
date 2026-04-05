from __future__ import annotations

import math
from datetime import date, datetime
from typing import Dict, List, Optional


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


# ── Confidence layer ────────────────────────────────────────────────────────
# Fator multiplicativo aplicado ao peso efetivo de cada sinal,
# baseado na qualidade declarada da fonte.
#
# Filosofia: penalizar levemente, não invalidar. Um proxy bem calibrado
# ainda carrega 88% do poder informativo de uma série direta.
CONFIDENCE_FACTORS: dict[str, float] = {
    "direct":            1.00,   # série direta de fonte primária
    "feed":              0.97,   # feed automatizado validado
    "mixed":             0.92,   # combinação de fontes diretas e proxies
    "proxy":             0.88,   # proxy calculado sobre séries públicas
    "fallback":          0.60,   # fallback manual ou default estático
    "manual_or_default": 0.60,
    "stale":             0.55,   # dado atrasado além do ciclo natural da série
}

# Threshold global de fallback (dias) — usado apenas quando o sinal
# não declara expected_frequency_days no model_config.json.
_DEFAULT_STALENESS_THRESHOLD = 3


def _staleness_days(as_of_date: Optional[str], run_date: str) -> Optional[int]:
    """Retorna número de dias de defasagem, ou None se data ausente."""
    if not as_of_date:
        return None
    try:
        d_as_of = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        d_run   = datetime.strptime(run_date,   "%Y-%m-%d").date()
        return (d_run - d_as_of).days
    except ValueError:
        return None


def _effective_weight(
    weight: float,
    source_type: str,
    as_of_date: Optional[str],
    run_date: str,
    expected_frequency_days: int = _DEFAULT_STALENESS_THRESHOLD,
) -> tuple[float, str, bool]:
    """
    Retorna (peso_efetivo, confidence_source, is_stale).

    Staleness é avaliado em relação ao ciclo natural de cada série
    (expected_frequency_days), não a um threshold global. Um dado semanal
    com 6 dias de defasagem não é stale — é o ciclo esperado da série.
    Um dado diário com 4 dias de defasagem é genuinamente stale.

    Tolerância adicional de 3 dias sobre o ciclo natural para absorver
    fins de semana, feriados e atrasos de publicação do FRED.
    """
    days = _staleness_days(as_of_date, run_date)
    # Stale = defasagem excede o ciclo natural + 3 dias de tolerância
    stale_threshold = expected_frequency_days + 3
    is_stale = days is not None and days > stale_threshold

    if is_stale:
        factor = CONFIDENCE_FACTORS["stale"]
        source = "stale"
    else:
        factor = CONFIDENCE_FACTORS.get(source_type, 0.88)
        source = source_type

    return weight * factor, source, is_stale


def compute_model(
    prior: float,
    signals: List[dict],
    raw: Dict[str, float],
    statuses: Dict[str, str],
    data_feed_meta: Optional[Dict[str, dict]] = None,
    run_date: Optional[str] = None,
) -> dict:
    """
    Calcula o modelo bayesiano com confidence layer e staleness automático.

    Novos parâmetros opcionais:
      data_feed_meta: dict com as_of_date por chave de raw_data
      run_date: data da execução (YYYY-MM-DD); usa hoje se ausente
    """
    if run_date is None:
        run_date = date.today().isoformat()
    if data_feed_meta is None:
        data_feed_meta = {}

    log_odds = math.log(_to_odds(prior))
    rows = []

    for signal in signals:
        status = statuses.get(signal["id"], "NEUTRO")
        p_e_h     = float(signal["p_e_h"])
        p_e_not_h = float(signal["p_e_not_h"])
        weight    = float(signal["weight"])
        source_type = signal.get("source_type", "proxy")
        raw_key   = signal.get("raw_key", "")

        # Busca as_of_date no data_feed_meta pela raw_key do sinal
        feed_entry  = data_feed_meta.get(raw_key, {})
        as_of_date  = feed_entry.get("as_of_date") if feed_entry else None

        # Frequência esperada declarada no config (fallback: 3 dias)
        expected_freq = int(signal.get("expected_frequency_days", _DEFAULT_STALENESS_THRESHOLD))

        eff_weight, conf_source, is_stale = _effective_weight(
            weight, source_type, as_of_date, run_date, expected_freq
        )

        # Sinal de cauda com dado stale → força NEUTRO explicitamente
        is_tail = signal.get("tail_signal", False)
        if is_stale and is_tail:
            status = "NEUTRO"

        risk_lr    = p_e_h / p_e_not_h
        reverse_lr = (1.0 - p_e_h) / (1.0 - p_e_not_h)

        if status == "ATIVO":
            lr_used     = risk_lr
            log_contrib = eff_weight * math.log(risk_lr)
        elif status == "CONTRARIO":
            lr_used     = reverse_lr
            log_contrib = eff_weight * math.log(reverse_lr)
        else:
            lr_used     = 1.0
            log_contrib = 0.0

        log_odds += log_contrib

        rows.append({
            "signal_id":               signal["id"],
            "signal_name":             signal["signal_name"],
            "block":                   signal["block"],
            "raw_value":               float(raw.get(raw_key, 0.0)),
            "status":                  status,
            "weight":                  weight,
            "weight_effective":        round(eff_weight, 4),
            "confidence_factor":       round(eff_weight / weight, 3) if weight else 1.0,
            "confidence_source":       conf_source,
            "is_stale":                is_stale,
            "as_of_date":              as_of_date,
            "expected_frequency_days": expected_freq,
            "tail_signal":             is_tail,
            "p_e_h":                   p_e_h,
            "p_e_not_h":               p_e_not_h,
            "lr_used":                 lr_used,
            "log_contrib":             log_contrib,
        })

    posterior = _from_odds(math.exp(log_odds))

    return {
        "prior":     prior,
        "posterior": posterior,
        "risk_label": _risk_label(posterior),
        "signals":   rows,
    }
