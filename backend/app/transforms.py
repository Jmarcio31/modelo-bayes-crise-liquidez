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
    return 0.4 * custody_score + 0.3 * tic_score + 0.3 * clamp(usd_score, 0.0, 1.0)
