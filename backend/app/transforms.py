from __future__ import annotations

import math
from typing import Iterable


def mean(values: Iterable[float]) -> float:
    vals = list(values)
    return sum(vals) / len(vals)


def stddev(values: Iterable[float]) -> float:
    vals = list(values)
    m = mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / len(vals))


def pct_change(current: float, past: float) -> float:
    return ((current / past) - 1.0) * 100.0


def rolling_std_last(values: list[float], window: int) -> float:
    return stddev(values[-window:])


def sahm_gap(unrate_values: list[float]) -> float:
    ma3 = mean(unrate_values[-3:])
    trailing12_min = min(unrate_values[-12:])
    return ma3 - trailing12_min


def sofr_iorb_bp(sofr: float, iorb: float) -> float:
    return (sofr - iorb) * 100.0


def vol_yields_20d_bp(y10_values: list[float]) -> float:
    """Volatilidade de yields em basis points (desvio padrão dos últimos 20 dias * 100)"""
    return rolling_std_last(y10_values, 20) * 100.0


def custody_12w_pct(custody_values: list[float]) -> float:
    return pct_change(custody_values[-1], custody_values[-13])


def reservas_pct_min(current_reserves: float, reserve_floor: float) -> float:
    return current_reserves / reserve_floor


def clamp(v: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, v))


def usd_stress_score(dxy_values: list[float], y10_values: list[float], y2_values: list[float]) -> float:
    dxy_change_20d = pct_change(dxy_values[-1], dxy_values[-21]) if len(dxy_values) >= 21 else 0.0
    y10_vol = rolling_std_last(y10_values, 20)
    curve_slope = y10_values[-1] - y2_values[-1]
    dxy_component = clamp(dxy_change_20d / 4.0, 0.0, 1.0)
    vol_component = clamp(y10_vol / 0.18, 0.0, 1.0)
    curve_component = clamp((0.5 - curve_slope) / 1.5, 0.0, 1.0)
    return 0.45 * dxy_component + 0.35 * vol_component + 0.20 * curve_component


def external_block_score(custody_12w: float, tic_3m: float, usd_score: float) -> float:
    custody_score = clamp((-custody_12w) / 8.0, 0.0, 1.0)
    tic_score = clamp((-tic_3m) / 150.0, 0.0, 1.0)
    usd_score_clamped = clamp(usd_score, 0.0, 1.0)
    return 0.4 * custody_score + 0.3 * tic_score + 0.3 * usd_score_clamped


def effr_iorb_bp(effr_values: list[float], iorb: float) -> float:
    """
    Spread entre Effective Fed Funds Rate e IORB, em basis points,
    calculado como média móvel dos últimos 5 dias úteis disponíveis.

    A persistência (MA5) elimina ruído de dias isolados e captura apenas
    disfunção estrutural na distribuição de reservas — como em set/2019
    e mar/2023, onde o spread permaneceu negativo por vários dias seguidos.

    EFFR - IORB < -10bp por 5 dias consecutivos: distribuição disfuncional.
    EFFR - IORB > +1bp: liquidez bem distribuída (amortecedor).

    Aceita lista de valores EFFR (série histórica) e o valor pontual de IORB.
    """
    if not effr_values:
        return 0.0
    window = effr_values[-5:] if len(effr_values) >= 5 else effr_values
    effr_ma5 = sum(window) / len(window)
    return (effr_ma5 - iorb) * 100.0


def stlfsi4_stress(stlfsi4_value: float) -> float:
    """
    St. Louis Fed Financial Stress Index (STLFSI4), versão 4.
    Série semanal disponível diretamente no FRED.

    O índice é construído pelo Fed com 18 séries de mercado (yields de
    Treasuries, spreads de crédito, volatilidade implícita, LIBOR/SOFR
    spreads), normalizados via PCA. Valores positivos indicam stress
    acima da média histórica; valores negativos indicam condições frouxas.

    Escala: o índice já é um z-score — valores acima de +1.0 são incomuns,
    acima de +1.5 indicam stress moderado-severo, acima de +2.0 são eventos
    de cauda (Covid atingiu ~8.0 no pico de março 2020).

    Esta função mapeia o valor bruto do STLFSI4 para [0, 1] via sigmoide
    calibrada para que:
      -0.5 → ~0.30 (condições frouxas → amortecedor)
       0.0 → ~0.50 (neutro)
      +1.0 → ~0.73 (stress moderado → próximo do threshold ativo)
      +1.5 → ~0.83 (stress elevado → ATIVO)
      +2.0 → ~0.90 (stress severo)
    """
    score = 1.0 / (1.0 + math.exp(-0.6 * stlfsi4_value))
    return clamp(score, 0.0, 1.0)


def discount_window_bn(dpcredit_millions: float) -> float:
    """
    Converte DPCREDIT (milhões de USD, FRED) para bilhões.
    Sinal unidirecional — thresholds do modelo não definem contrary.
    Retorna 0.0 para valores negativos ou ausentes.
    """
    return clamp(dpcredit_millions / 1000.0, 0.0, 1e6)