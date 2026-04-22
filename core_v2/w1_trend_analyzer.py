"""
📊 W1 TREND ANALYZER — Глобальный тренд на недельном графике
ФАЗА 2.2: Песочница core_v2
"""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger('core_v2.w1_trend_analyzer')


@dataclass
class W1TrendResult:
    """Результат анализа недельного тренда"""
    trend: str = 'SIDEWAYS'          # 'BULL', 'BEAR', 'SIDEWAYS'
    strength: float = 0.0            # 0-100%
    structure: str = 'NONE'          # 'HH/HL', 'LH/LL', 'NONE'
    adx: float = 0.0
    ema20: float = 0.0
    ema50: float = 0.0
    confidence: float = 0.0
    passed: bool = False
    rejection_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trend': self.trend,
            'strength': self.strength,
            'structure': self.structure,
            'adx': self.adx,
            'ema20': self.ema20,
            'ema50': self.ema50,
            'confidence': self.confidence,
            'passed': self.passed,
            'rejection_reason': self.rejection_reason
        }


class W1TrendAnalyzer:
    """
    Анализатор глобального тренда на W1 (недельный график)
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}

        # Параметры
        self.ema_fast = 20
        self.ema_slow = 50
        self.adx_period = 14
        self.min_candles = 50

        # Пороги
        self.strong_adx = 25.0
        self.moderate_adx = 18.0
        self.bullish_threshold = 3
        self.bearish_threshold = 3

        logger.info("✅ W1TrendAnalyzer инициализирован (core_v2)")

    def analyze(self, symbol: str, w1_klines: List) -> W1TrendResult:
        """
        Анализ недельного тренда

        Args:
            symbol: Символ (для логов)
            w1_klines: Свечи W1 в формате [[timestamp, open, high, low, close, volume], ...]

        Returns:
            W1TrendResult
        """
        logger.info(f"📊 [{symbol}] Анализ W1 тренда")

        # Проверка данных
        if not w1_klines or len(w1_klines) < self.min_candles:
            return W1TrendResult(
                trend='SIDEWAYS',
                passed=False,
                rejection_reason=f"Недостаточно данных: {len(w1_klines) if w1_klines else 0} < {self.min_candles}"
            )

        try:
            # Извлекаем цены
            closes = [float(k[4]) for k in w1_klines]
            highs = [float(k[2]) for k in w1_klines]
            lows = [float(k[3]) for k in w1_klines]
            current_close = closes[-1]

            # Рассчитываем EMA
            ema20 = self._calculate_ema(closes, self.ema_fast)
            ema50 = self._calculate_ema(closes, self.ema_slow)

            if not ema20 or not ema50:
                return W1TrendResult(
                    trend='SIDEWAYS',
                    passed=False,
                    rejection_reason="Не удалось рассчитать EMA"
                )

            current_ema20 = ema20[-1]
            current_ema50 = ema50[-1]

            # Определяем структуру тренда
            structure = self._determine_structure(highs, lows)

            # Рассчитываем ADX
            adx_data = self._calculate_adx(highs, lows, closes, self.adx_period)
            adx = adx_data.get('adx', 0)
            plus_di = adx_data.get('plus_di', 0)
            minus_di = adx_data.get('minus_di', 0)

            # Определяем направление
            bull_conditions = [
                current_close > current_ema20,
                current_ema20 > current_ema50,
                plus_di > minus_di,
                structure == 'HH/HL'
            ]

            bear_conditions = [
                current_close < current_ema20,
                current_ema20 < current_ema50,
                minus_di > plus_di,
                structure == 'LH/LL'
            ]

            bull_score = sum(bull_conditions)
            bear_score = sum(bear_conditions)

            # Определяем тренд
            if bull_score >= self.bullish_threshold:
                trend = 'BULL'
                confidence = 0.5 + (bull_score * 0.1)
            elif bear_score >= self.bearish_threshold:
                trend = 'BEAR'
                confidence = 0.5 + (bear_score * 0.1)
            else:
                trend = 'SIDEWAYS'
                confidence = 0.3

            # Сила тренда на основе ADX
            if adx > self.strong_adx:
                strength = min(80 + (adx - self.strong_adx), 100)
            elif adx > self.moderate_adx:
                strength = 50 + (adx - self.moderate_adx) * 2
            else:
                strength = adx * 2

            strength = min(strength, 100)

            # Итог
            passed = trend in ['BULL', 'BEAR'] and confidence >= 0.6 and adx >= self.moderate_adx

            result = W1TrendResult(
                trend=trend,
                strength=strength,
                structure=structure,
                adx=adx,
                ema20=current_ema20,
                ema50=current_ema50,
                confidence=confidence,
                passed=passed
            )

            logger.info(f"✅ [{symbol}] W1: {trend} (ADX={adx:.1f}, сила={strength:.1f}%)")

            return result

        except Exception as e:
            logger.error(f"❌ [{symbol}] Ошибка: {e}")
            return W1TrendResult(
                trend='SIDEWAYS',
                passed=False,
                rejection_reason=f"Ошибка: {str(e)}"
            )

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Расчёт EMA"""
        if len(prices) < period:
            return []

        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]

        for price in prices[period:]:
            ema.append((price * multiplier) + (ema[-1] * (1 - multiplier)))

        return ema

    def _determine_structure(self, highs: List[float], lows: List[float]) -> str:
        """Определение структуры тренда (HH/HL, LH/LL, NONE)"""
        if len(highs) < 10:
            return 'NONE'

        recent_highs = highs[-8:]
        recent_lows = lows[-8:]

        # HH/HL (восходящая структура)
        hh_count = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i] > recent_highs[i-1])
        hl_count = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] > recent_lows[i-1])

        if hh_count >= 3 and hl_count >= 3:
            return 'HH/HL'

        # LH/LL (нисходящая структура)
        lh_count = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i] < recent_highs[i-1])
        ll_count = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] < recent_lows[i-1])

        if lh_count >= 3 and ll_count >= 3:
            return 'LH/LL'

        return 'NONE'

    def _calculate_adx(self, highs: List[float], lows: List[float],
                       closes: List[float], period: int = 14) -> Dict[str, float]:
        """Расчёт ADX"""
        if len(highs) < period * 2:
            return {'adx': 0, 'plus_di': 0, 'minus_di': 0}

        try:
            tr_values = []
            plus_dm_values = []
            minus_dm_values = []

            for i in range(1, len(highs)):
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i-1]),
                    abs(lows[i] - closes[i-1])
                )
                tr_values.append(tr)

                up_move = highs[i] - highs[i-1]
                down_move = lows[i-1] - lows[i]

                plus_dm = up_move if up_move > down_move and up_move > 0 else 0
                minus_dm = down_move if down_move > up_move and down_move > 0 else 0

                plus_dm_values.append(plus_dm)
                minus_dm_values.append(minus_dm)

            # Сглаживание
            atr = self._smooth_wilder(tr_values, period)
            plus_di_smoothed = self._smooth_wilder(plus_dm_values, period)
            minus_di_smoothed = self._smooth_wilder(minus_dm_values, period)

            if not atr or atr[-1] == 0:
                return {'adx': 0, 'plus_di': 0, 'minus_di': 0}

            plus_di = (plus_di_smoothed[-1] / atr[-1]) * 100 if len(plus_di_smoothed) > 0 else 0
            minus_di = (minus_di_smoothed[-1] / atr[-1]) * 100 if len(minus_di_smoothed) > 0 else 0

            # DX и ADX
            if plus_di + minus_di == 0:
                dx = 0
            else:
                dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100

            return {'adx': dx, 'plus_di': plus_di, 'minus_di': minus_di}

        except Exception as e:
            logger.error(f"Ошибка ADX: {e}")
            return {'adx': 0, 'plus_di': 0, 'minus_di': 0}

    def _smooth_wilder(self, values: List[float], period: int) -> List[float]:
        """Сглаживание по Wilder"""
        if len(values) < period:
            return []

        smoothed = [sum(values[:period]) / period]

        for i in range(period, len(values)):
            smoothed.append(smoothed[-1] + (values[i] - smoothed[-1]) / period)

        return smoothed