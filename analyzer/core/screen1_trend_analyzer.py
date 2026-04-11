# core/screen1_trend_analyzer.py (ОБНОВЛЁННАЯ ВЕРСИЯ - ФАЗА 1.3.10)
"""
🎯 ЭКРАН 1 - ПОЛНЫЙ АНАЛИЗ ТРЕНДА (Дневной таймфрейм)

ФАЗА 1.3.10:
- Добавлен расчёт EMA20
- Добавлено определение структуры тренда (HH/HL, LH/LL, NONE)
- Сохранение структуры в indicators
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any
from functools import lru_cache

logger = logging.getLogger('screen1_analyzer')


@dataclass
class Screen1Result:
    """Результат анализа 1-го экрана (тренд)"""
    trend_direction: str = "RANGE"
    trend_strength: str = "SIDEWAYS"
    confidence_score: float = 0.0
    key_levels: Dict[str, float] = field(default_factory=dict)
    indicators: Dict[str, Any] = field(default_factory=dict)
    passed: bool = False
    rejection_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trend_direction": self.trend_direction,
            "trend_strength": self.trend_strength,
            "confidence_score": self.confidence_score,
            "key_levels": self.key_levels,
            "indicators": self.indicators,
            "passed": self.passed,
            "rejection_reason": self.rejection_reason
        }


class Screen1TrendAnalyzer:
    """
    Анализатор для первого экрана - определение тренда
    """

    def __init__(self, config=None):
        self.config = config or {}

        # Получаем параметры анализа из конфига
        analysis_config = self.config.get('analysis', {})
        indicators_config = analysis_config.get('indicators', {})
        structure_config = analysis_config.get('structure', {})
        signal_config = analysis_config.get('signal_generation', {})

        # Параметры индикаторов
        self.ema_periods = indicators_config.get('ema_periods', [20, 50, 100])
        self.adx_period = indicators_config.get('adx_period', 14)
        self.rsi_period = indicators_config.get('rsi_period', 14)
        self.atr_period = indicators_config.get('atr_period', 14)

        # Параметры структуры тренда
        self.lookback_period = structure_config.get('lookback_period', 6)
        self.min_confirmed_swings = structure_config.get('min_confirmed_swings', 2)
        self.ma_crossover_min_candles = structure_config.get('ma_crossover_min_candles', 50)

        # Параметры определения тренда
        self.min_candles_for_analysis = analysis_config.get('thresholds', {}).get('min_candles_for_analysis', 100)
        self.trend_detection_threshold = analysis_config.get('signal_generation', {}).get(
            'moderate_confidence_threshold', 0.65)

        # Параметры силы тренда по ADX
        adx_thresholds = analysis_config.get('adx_thresholds', {})
        self.strong_trend_adx = adx_thresholds.get('strong', 25.0)
        self.moderate_trend_adx = adx_thresholds.get('moderate', 18.0)
        self.weak_trend_adx = adx_thresholds.get('weak', 12.0)
        self.sideways_trend_adx = adx_thresholds.get('sideways', 0.0)

        # Параметры расчета уверенности
        confidence_params = analysis_config.get('confidence_params', {})
        self.base_confidence_multiplier = confidence_params.get('base_multiplier', 0.08)
        self.adx_confidence_boost = confidence_params.get('adx_boost', 0.3)
        self.volume_confirmation_bonus = confidence_params.get('volume_bonus', 0.1)
        self.sideways_penalty = confidence_params.get('sideways_penalty', 0.8)
        self.max_confidence = signal_config.get('max_confidence', 0.95)

        # Пороговые значения
        thresholds_config = analysis_config.get('thresholds', {})
        self.bullish_conditions_threshold = thresholds_config.get('bullish_conditions_threshold', 4)
        self.bearish_conditions_threshold = thresholds_config.get('bearish_conditions_threshold', 4)
        self.volume_confirmation_threshold = thresholds_config.get('volume_confirmation_threshold', 0.6)
        self.adx_boost_start = thresholds_config.get('adx_boost_start', 15.0)
        self.strong_trend_boost = thresholds_config.get('strong_trend_boost', 0.15)
        self.moderate_trend_boost = thresholds_config.get('moderate_trend_boost', 0.05)

        # Параметры поиска уровней
        levels_config = analysis_config.get('levels_config', {})
        self.pivot_lookback = levels_config.get('pivot_lookback', 20)
        self.support_candidate_lookback = levels_config.get('support_candidate_lookback', 50)
        self.resistance_multiplier = levels_config.get('resistance_multiplier', 1.02)

        logger.info(f"✅ Screen1TrendAnalyzer инициализирован с параметрами из конфига")
        logger.info(f"   EMA периоды: {self.ema_periods}, ADX период: {self.adx_period}")
        logger.info(f"   Порог определения тренда: {self.trend_detection_threshold}")
        logger.info(f"   ADX уровни: Strong>{self.strong_trend_adx}, Moderate>{self.moderate_trend_adx}")

    def analyze_daily_trend(self, symbol: str, d1_klines: List) -> Screen1Result:
        """Основной метод анализа дневного тренда"""
        logger.info(f"📊 {symbol} - Анализ дневного тренда")
        result = Screen1Result()

        try:
            if len(d1_klines) < self.min_candles_for_analysis:
                reason = f"недостаточно данных: {len(d1_klines)} свечей (мин: {self.min_candles_for_analysis})"
                logger.warning(f"❌ {symbol}: ЭКРАН 1 не пройден — {reason}")
                result.rejection_reason = reason
                return result

            close_prices = [float(k[4]) for k in d1_klines]
            high_prices = [float(k[2]) for k in d1_klines]
            low_prices = [float(k[3]) for k in d1_klines]
            volumes = [float(k[5]) for k in d1_klines]
            current_close = close_prices[-1]

            logger.info(f"Анализ D1 данных: {len(d1_klines)} свечей, текущая цена: {current_close:.2f}")

            # Рассчитываем индикаторы
            ema_20 = self._calculate_ema(close_prices, 20)
            ema_50 = self._calculate_ema(close_prices, 50)
            ema_100 = self._calculate_ema(close_prices, 100)
            macd_data = self._calculate_macd(close_prices)
            adx_data = self._calculate_adx(high_prices, low_prices, close_prices, self.adx_period)

            if not ema_50 or not ema_100:
                reason = "не удалось рассчитать EMA"
                logger.error(f"❌ {symbol}: {reason}")
                result.rejection_reason = reason
                return result

            current_ema_20 = ema_20[-1] if len(ema_20) > 0 else 0
            current_ema_50 = ema_50[-1] if len(ema_50) > 0 else 0
            current_ema_100 = ema_100[-1] if len(ema_100) > 0 else 0

            logger.info(f"Индикаторы D1: EMA20={current_ema_20:.2f}, EMA50={current_ema_50:.2f}, "
                        f"EMA100={current_ema_100:.2f}, ADX={adx_data.get('adx', 0):.1f}")

            # Анализ тренда
            trend_info = self._determine_trend_direction(
                current_close, current_ema_50, current_ema_100,
                macd_data, adx_data, high_prices, low_prices, volumes
            )

            # Формируем результат
            result.trend_direction = trend_info['direction']
            result.trend_strength = trend_info['strength']
            result.confidence_score = trend_info['confidence']
            result.key_levels = self._find_key_levels(high_prices, low_prices)
            result.indicators = trend_info['indicators']

            # Добавляем EMA20 в indicators
            result.indicators['ema_20'] = round(current_ema_20, 2)

            result.passed = self._check_screen1_passed(trend_info)

            if not result.passed:
                reason = f"{result.trend_direction} {result.trend_strength} (уверенность: {result.confidence_score:.1%}, ADX: {adx_data.get('adx', 0):.1f})"
                logger.info(f"❌ {symbol}: ЭКРАН 1 не пройден — {reason}")
                result.rejection_reason = reason
            else:
                status = "✅" if result.passed else "❌"
                logger.info(f"{status} {symbol} ЭКРАН 1: {result.trend_direction} {result.trend_strength} "
                            f"(уверенность: {result.confidence_score:.1%}, ADX: {adx_data.get('adx', 0):.1f})")

            return result

        except Exception as e:
            logger.error(f"❌ Ошибка анализа тренда для {symbol}: {e}")
            result.rejection_reason = f"Ошибка: {str(e)}"
            return result

    @lru_cache(maxsize=128)
    def _calculate_ema_cached(self, prices_tuple: tuple, period: int) -> List[float]:
        """Оптимизированный расчет EMA с кэшированием"""
        prices = list(prices_tuple)

        if len(prices) < period:
            return []

        ema_values = []
        multiplier = 2 / (period + 1)

        ema = sum(prices[:period]) / period
        ema_values.append(ema)

        for price in prices[period:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
            ema_values.append(ema)

        return ema_values

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Расчет EMA"""
        return self._calculate_ema_cached(tuple(prices), period)

    def _calculate_macd(self, prices: List[float]) -> Dict[str, Any]:
        """Расчет MACD"""
        logger.debug(f"Расчет MACD для {len(prices)} цен")

        if len(prices) < 26:
            logger.warning(f"Недостаточно данных для MACD: {len(prices)} < 26")
            return {"macd_line": 0, "signal_line": 0, "histogram": 0}

        ema_12 = self._calculate_ema(prices, 12)
        ema_26 = self._calculate_ema(prices, 26)

        if not ema_12 or not ema_26:
            return {"macd_line": 0, "signal_line": 0, "histogram": 0}

        min_length = min(len(ema_12), len(ema_26))
        macd_line = [ema_12[i] - ema_26[i] for i in range(min_length)]
        signal_line = self._calculate_ema(macd_line, 9) if macd_line else []

        histogram = []
        if macd_line and signal_line:
            min_signal_length = min(len(macd_line), len(signal_line))
            histogram = [macd_line[i] - signal_line[i] for i in range(min_signal_length)]

        result = {
            "macd_line": macd_line[-1] if macd_line else 0,
            "signal_line": signal_line[-1] if signal_line else 0,
            "histogram": histogram[-1] if histogram else 0
        }

        return result

    def _calculate_adx(self, high_prices: List[float], low_prices: List[float],
                       close_prices: List[float], period: int = 14) -> Dict[str, float]:
        """Расчет ADX"""
        logger.debug(f"Расчет ADX({period})")

        if len(high_prices) < period * 2:
            logger.warning(f"Недостаточно данных для ADX: {len(high_prices)} < {period * 2}")
            return {"adx": 0, "plus_di": 0, "minus_di": 0}

        try:
            # Расчет True Range
            tr_values = []
            for i in range(1, len(high_prices)):
                tr = max(
                    high_prices[i] - low_prices[i],
                    abs(high_prices[i] - close_prices[i - 1]),
                    abs(low_prices[i] - close_prices[i - 1])
                )
                tr_values.append(tr)

            # Расчет Directional Movement
            plus_dm_values = []
            minus_dm_values = []

            for i in range(1, len(high_prices)):
                plus_dm = high_prices[i] - high_prices[i - 1]
                minus_dm = low_prices[i - 1] - low_prices[i]

                if plus_dm < 0: plus_dm = 0
                if minus_dm < 0: minus_dm = 0
                if plus_dm < minus_dm: plus_dm = 0
                if minus_dm < plus_dm: minus_dm = 0

                plus_dm_values.append(plus_dm)
                minus_dm_values.append(minus_dm)

            # Сглаживание
            def smooth_values(values, period):
                if len(values) < period:
                    return []
                smoothed = [sum(values[:period]) / period]
                for i in range(period, len(values)):
                    smoothed.append((smoothed[-1] * (period - 1) + values[i]) / period)
                return smoothed

            tr_smoothed = smooth_values(tr_values, period)
            plus_dm_smoothed = smooth_values(plus_dm_values, period)
            minus_dm_smoothed = smooth_values(minus_dm_values, period)

            if not tr_smoothed or not plus_dm_smoothed or not minus_dm_smoothed:
                return {"adx": 0, "plus_di": 0, "minus_di": 0}

            # Расчет DI
            plus_di = []
            minus_di = []
            for i in range(len(tr_smoothed)):
                if tr_smoothed[i] == 0:
                    plus_di.append(0)
                    minus_di.append(0)
                else:
                    plus_di.append((plus_dm_smoothed[i] / tr_smoothed[i]) * 100)
                    minus_di.append((minus_dm_smoothed[i] / tr_smoothed[i]) * 100)

            # Расчет DX и ADX
            dx_values = []
            for i in range(len(plus_di)):
                if plus_di[i] + minus_di[i] == 0:
                    dx_values.append(0)
                else:
                    dx_values.append(abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]) * 100)

            adx = sum(dx_values[-period:]) / period if len(dx_values) >= period else 0

            result = {
                "adx": adx,
                "plus_di": plus_di[-1] if plus_di else 0,
                "minus_di": minus_di[-1] if minus_di else 0
            }

            return result

        except Exception as e:
            logger.error(f"Ошибка расчета ADX: {e}")
            return {"adx": 0, "plus_di": 0, "minus_di": 0}

    def _determine_trend_direction(self, current_close: float, ema_50: float, ema_100: float,
                                   macd_data: Dict[str, Any], adx_data: Dict[str, float],
                                   high_prices: List[float], low_prices: List[float],
                                   volumes: List[float]) -> Dict[str, Any]:
        """Определение направления и силы тренда"""

        bull_conditions = [
            current_close > ema_50 if ema_50 > 0 else False,
            ema_50 > ema_100 if ema_50 > 0 and ema_100 > 0 else False,
            macd_data["macd_line"] > macd_data["signal_line"],
            adx_data["plus_di"] > adx_data["minus_di"],
            self._is_uptrend_structure(high_prices, low_prices),
            current_close > self._get_pivot_level(high_prices, low_prices, "support")
        ]

        bear_conditions = [
            current_close < ema_50 if ema_50 > 0 else False,
            ema_50 < ema_100 if ema_50 > 0 and ema_100 > 0 else False,
            macd_data["macd_line"] < macd_data["signal_line"],
            adx_data["minus_di"] > adx_data["plus_di"],
            self._is_downtrend_structure(high_prices, low_prices),
            current_close < self._get_pivot_level(high_prices, low_prices, "resistance")
        ]

        bull_score = sum(bull_conditions)
        bear_score = sum(bear_conditions)

        logger.info(f"Бычьи условия: {bull_score}/6, Медвежьи условия: {bear_score}/6")

        trend_direction = "RANGE"
        trend_strength = "SIDEWAYS"
        confidence_score = 0.0

        if bull_score >= self.bullish_conditions_threshold:
            trend_direction = "BULL"
            base_confidence = 0.5 + (bull_score * self.base_confidence_multiplier)
            adx_boost = min(adx_data["adx"] / self.adx_boost_start, self.adx_confidence_boost) if adx_data[
                                                                                                      "adx"] > self.adx_boost_start else 0
            confidence_score = min(base_confidence + adx_boost, self.max_confidence)
            logger.info(f"✅ Определен БЫЧИЙ тренд, уверенность: {confidence_score:.1%}")

        elif bear_score >= self.bearish_conditions_threshold:
            trend_direction = "BEAR"
            base_confidence = 0.5 + (bear_score * self.base_confidence_multiplier)
            adx_boost = min(adx_data["adx"] / self.adx_boost_start, self.adx_confidence_boost) if adx_data[
                                                                                                      "adx"] > self.adx_boost_start else 0
            confidence_score = min(base_confidence + adx_boost, self.max_confidence)
            logger.info(f"✅ Определен МЕДВЕЖИЙ тренд, уверенность: {confidence_score:.1%}")

        # Определение силы тренда
        if adx_data["adx"] > self.strong_trend_adx:
            trend_strength = "STRONG"
            confidence_score = min(confidence_score + self.strong_trend_boost, self.max_confidence)
            logger.info(f"Сила тренда: СИЛЬНЫЙ (ADX={adx_data['adx']:.1f})")
        elif adx_data["adx"] > self.moderate_trend_adx:
            trend_strength = "MODERATE"
            confidence_score = min(confidence_score + self.moderate_trend_boost, self.max_confidence)
            logger.info(f"Сила тренда: УМЕРЕННЫЙ (ADX={adx_data['adx']:.1f})")
        elif adx_data["adx"] > self.weak_trend_adx:
            trend_strength = "WEAK"
            logger.info(f"Сила тренда: СЛАБЫЙ (ADX={adx_data['adx']:.1f})")
        else:
            trend_strength = "SIDEWAYS"
            confidence_score *= self.sideways_penalty
            logger.info(f"Сила тренда: ФЛЭТ (ADX={adx_data['adx']:.1f})")

        # Проверка подтверждения объемом
        volume_confirmation = self._has_trend_confirmation(high_prices, low_prices, volumes, trend_direction)
        if volume_confirmation:
            confidence_score = min(confidence_score + self.volume_confirmation_bonus, self.max_confidence)
            logger.info("✅ Тренд подтвержден объемом")

        return {
            "direction": trend_direction,
            "strength": trend_strength,
            "confidence": confidence_score,
            "indicators": {
                "ema_50": round(ema_50, 2),
                "ema_100": round(ema_100, 2),
                "macd_line": round(macd_data["macd_line"], 4),
                "macd_signal": round(macd_data["signal_line"], 4),
                "adx": round(adx_data["adx"], 2),
                "plus_di": round(adx_data["plus_di"], 2),
                "minus_di": round(adx_data["minus_di"], 2),
                "structure_score": bull_score if trend_direction == "BULL" else bear_score,
                "volume_confirmation": volume_confirmation
            }
        }

    def _is_uptrend_structure(self, high_prices: List[float], low_prices: List[float]) -> bool:
        """Проверка структуры восходящего тренда (HH/HL)"""
        if len(high_prices) < 10:
            return False

        recent_highs = high_prices[-6:]
        recent_lows = low_prices[-6:]

        hh_confirmed = all(recent_highs[i] > recent_highs[i - 1] for i in range(1, min(3, len(recent_highs))))
        hl_confirmed = all(recent_lows[i] > recent_lows[i - 1] for i in range(1, min(3, len(recent_lows))))

        result = hh_confirmed and hl_confirmed
        if result:
            logger.info("✅ Обнаружена структура восходящего тренда (HH/HL)")

        return result

    def _is_downtrend_structure(self, high_prices: List[float], low_prices: List[float]) -> bool:
        """Проверка структуры нисходящего тренда (LH/LL)"""
        if len(high_prices) < 10:
            return False

        recent_highs = high_prices[-6:]
        recent_lows = low_prices[-6:]

        lh_confirmed = all(recent_highs[i] < recent_highs[i - 1] for i in range(1, min(3, len(recent_highs))))
        ll_confirmed = all(recent_lows[i] < recent_lows[i - 1] for i in range(1, min(3, len(recent_lows))))

        result = lh_confirmed and ll_confirmed
        if result:
            logger.info("✅ Обнаружена структура нисходящего тренда (LH/LL)")

        return result

    def _get_pivot_level(self, high_prices: List[float], low_prices: List[float], level_type: str) -> float:
        """Получение ключевого уровня"""
        try:
            if level_type == "support":
                result = min(low_prices[-self.pivot_lookback:]) if len(low_prices) >= self.pivot_lookback else min(
                    low_prices)
            else:
                result = max(high_prices[-self.pivot_lookback:]) if len(high_prices) >= self.pivot_lookback else max(
                    high_prices)
            return result
        except:
            return 0.0

    def _has_trend_confirmation(self, high_prices: List[float], low_prices: List[float],
                                volumes: List[float], direction: str) -> bool:
        """Проверка подтверждения тренда объемом"""
        if len(high_prices) < 10 or len(volumes) < 10:
            return False

        confirmation_score = self._calculate_trend_confirmation_score(high_prices, low_prices, volumes, direction)
        return confirmation_score > self.volume_confirmation_threshold

    def _calculate_trend_confirmation_score(self, high_prices: List[float], low_prices: List[float],
                                            volumes: List[float], direction: str) -> float:
        """Расчет оценки подтверждения тренда"""
        if len(high_prices) < 10 or len(volumes) < 10:
            return 0.5

        score = 0.5
        recent_highs = high_prices[-5:]
        recent_lows = low_prices[-5:]
        recent_volumes = volumes[-5:]

        non_zero_volumes = [v for v in recent_volumes if v > 0]
        if not non_zero_volumes:
            return score

        avg_volume = sum(non_zero_volumes) / len(non_zero_volumes)

        if direction == "BULL":
            up_moves = sum(1 for i in range(1, len(recent_highs))
                           if recent_highs[i] > recent_highs[i - 1])
            volume_on_up = sum(1 for i in range(1, len(recent_volumes))
                               if recent_volumes[i] > avg_volume and recent_highs[i] > recent_highs[i - 1])

            if up_moves > 0:
                volume_correlation = volume_on_up / up_moves
                score += volume_correlation * 0.3

        elif direction == "BEAR":
            down_moves = sum(1 for i in range(1, len(recent_lows))
                             if recent_lows[i] < recent_lows[i - 1])
            volume_on_down = sum(1 for i in range(1, len(recent_volumes))
                                 if recent_volumes[i] > avg_volume and recent_lows[i] < recent_lows[i - 1])

            if down_moves > 0:
                volume_correlation = volume_on_down / down_moves
                score += volume_correlation * 0.3

        return min(max(score, 0), 1)

    def _find_key_levels(self, high_prices: List[float], low_prices: List[float]) -> Dict[str, float]:
        """Поиск ключевых уровней поддержки и сопротивления"""
        try:
            support = self._find_strong_support(low_prices)
            resistance = self._find_strong_resistance(high_prices)

            return {
                "support": round(support, 2),
                "resistance": round(resistance, 2),
                "pivot": round((support + resistance) / 2, 2)
            }
        except:
            return {"support": 0.0, "resistance": 0.0, "pivot": 0.0}

    def _find_strong_support(self, low_prices: List[float]) -> float:
        """Поиск сильного уровня поддержки"""
        try:
            if len(low_prices) < 20:
                return min(low_prices) if low_prices else 0.0

            recent_lows = low_prices[-self.support_candidate_lookback:] if len(
                low_prices) > self.support_candidate_lookback else low_prices
            support_candidates = []

            for i in range(2, len(recent_lows) - 2):
                if (recent_lows[i] < recent_lows[i - 1] and
                        recent_lows[i] < recent_lows[i - 2] and
                        recent_lows[i] < recent_lows[i + 1] and
                        recent_lows[i] < recent_lows[i + 2]):
                    support_candidates.append(recent_lows[i])

            result = min(support_candidates) if support_candidates else min(recent_lows)
            logger.info(f"✅ Сильный уровень поддержки: {result:.2f}")
            return result
        except:
            result = min(low_prices[-self.pivot_lookback:]) if len(low_prices) >= self.pivot_lookback else min(
                low_prices)
            return result

    def _find_strong_resistance(self, high_prices: List[float]) -> float:
        """Поиск уровня сопротивления"""
        try:
            if len(high_prices) < 20:
                return max(high_prices) if high_prices else 0.0

            recent_highs = high_prices[-self.support_candidate_lookback:] if len(
                high_prices) > self.support_candidate_lookback else high_prices
            resistance_candidates = []

            for i in range(2, len(recent_highs) - 2):
                if (recent_highs[i] > recent_highs[i - 1] and
                        recent_highs[i] > recent_highs[i - 2] and
                        recent_highs[i] > recent_highs[i + 1] and
                        recent_highs[i] > recent_highs[i + 2]):
                    resistance_candidates.append(recent_highs[i])

            if resistance_candidates:
                last_candidates = resistance_candidates[-3:] if len(
                    resistance_candidates) >= 3 else resistance_candidates
                result = sum(last_candidates) / len(last_candidates)
            else:
                result = recent_highs[-1] * self.resistance_multiplier if recent_highs else 0.0

            logger.info(f"✅ Уровень сопротивления: {result:.2f}")
            return result
        except:
            result = max(high_prices[-self.pivot_lookback:]) if len(high_prices) >= self.pivot_lookback else max(
                high_prices)
            return result

    def _check_screen1_passed(self, trend_info: Dict[str, Any]) -> bool:
        """Проверка прохождения первого экрана"""
        min_confidence = self.config.get('analysis', {}).get('screen1_min_confidence', 0.7)
        min_adx = self.config.get('analysis', {}).get('screen1_adx_threshold', 20.0)

        direction = trend_info['direction']
        confidence = trend_info['confidence']
        adx = trend_info['indicators'].get('adx', 0)

        if direction not in ["BULL", "BEAR"]:
            return False

        has_clear_structure = self._has_clear_trend_structure  # Это определяется в основном анализаторе

        structure_passed = has_clear_structure and confidence >= min_confidence * 0.9

        passed = (
                direction in ["BULL", "BEAR"] and
                (confidence >= min_confidence or structure_passed) and
                (adx >= min_adx or structure_passed)
        )

        return passed

    def _has_clear_trend_structure(self, high_prices: List[float], low_prices: List[float]) -> bool:
        """Проверка наличия четкой структуры тренда"""
        return (self._is_uptrend_structure(high_prices, low_prices) or
                self._is_downtrend_structure(high_prices, low_prices))


# Экспорт для импорта в другие модули
__all__ = ['Screen1Result', 'Screen1TrendAnalyzer']