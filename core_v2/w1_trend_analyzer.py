"""
W1 TREND ANALYZER — Глобальный тренд на недельном графике
"""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger('core_v2.w1_trend_analyzer')


@dataclass
class W1TrendResult:
    trend: str = 'SIDEWAYS'
    strength: float = 0.0
    structure: str = 'NONE'
    adx: float = 0.0
    ema20: float = 0.0
    ema50: float = 0.0
    confidence: float = 0.0
    passed: bool = False
    rejection_reason: str = ""


class W1TrendAnalyzer:
    def __init__(self, config: Optional[Dict] = None):
        self.ema_fast = 20
        self.ema_slow = 50
        self.adx_period = 14
        self.min_candles = 50
        self.strong_adx = 25.0
        self.moderate_adx = 18.0

    def analyze(self, symbol: str, w1_klines: List) -> W1TrendResult:
        if not w1_klines or len(w1_klines) < self.min_candles:
            return W1TrendResult(passed=False, rejection_reason="no_data")

        closes = [float(k[4]) for k in w1_klines]
        highs = [float(k[2]) for k in w1_klines]
        lows = [float(k[3]) for k in w1_klines]

        ema20 = self._ema(closes, self.ema_fast)
        ema50 = self._ema(closes, self.ema_slow)

        if not ema20 or not ema50:
            return W1TrendResult(passed=False, rejection_reason="ema_error")

        current_ema20 = ema20[-1]
        current_ema50 = ema50[-1]

        if current_ema20 > current_ema50:
            trend = 'BULL'
        elif current_ema20 < current_ema50:
            trend = 'BEAR'
        else:
            trend = 'SIDEWAYS'

        adx = self._adx(highs, lows, closes)
        strength = min(adx * 2, 100) if adx else 0

        return W1TrendResult(
            trend=trend,
            strength=strength,
            adx=adx or 0,
            ema20=current_ema20,
            ema50=current_ema50,
            passed=trend in ['BULL', 'BEAR']
        )

    def _ema(self, prices: List[float], period: int) -> List[float]:
        if len(prices) < period:
            return []
        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for price in prices[period:]:
            ema.append((price * multiplier) + (ema[-1] * (1 - multiplier)))
        return ema

    def _adx(self, highs: List[float], lows: List[float], closes: List[float]) -> float:
        if len(highs) < self.adx_period * 2:
            return 0
        try:
            tr_sum = 0
            for i in range(1, self.adx_period + 1):
                tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
                tr_sum += tr
            return (tr_sum / self.adx_period) / (closes[-1] or 1) * 100
        except:
            return 0