# analyzer/core/screen3_signal_generator.py (ПОЛНОСТЬЮ - ФАЗА 1.4.1)
"""
🎯 ЭКРАН 3 - ГЕНЕРАЦИЯ СИГНАЛОВ M15 (только M15, рыночный ордер)

ФАЗА 1.4.1:
- ИСПРАВЛЕНО: убрана перезапись entry_price
- ИСПРАВЛЕНО: entry_price берётся из best_zone, а не из closes[-1]
- ИСПРАВЛЕНО: унифицирован формат цен (4 знака для CRVUSDT)
- ИСПРАВЛЕНО: проверка SL < Entry для BUY, SL > Entry для SELL
- ДОБАВЛЕНО: логирование реальной цены
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from enum import Enum
import numpy as np

logger = logging.getLogger('screen3_analyzer')


class PatternType(Enum):
    """Типы паттернов"""
    PIN_BAR = "PIN_BAR"
    ENGULFING = "ENGULFING"
    MORNING_STAR = "MORNING_STAR"
    EVENING_STAR = "EVENING_STAR"
    BULLISH_DIVERGENCE = "BULLISH_DIVERGENCE"
    BEARISH_DIVERGENCE = "BEARISH_DIVERGENCE"
    BULLISH_BREAKOUT = "BULLISH_BREAKOUT"
    BEARISH_BREAKOUT = "BEARISH_BREAKOUT"
    MA_CROSSOVER = "MA_CROSSOVER"
    MA_BOUNCE = "MA_BOUNCE"


@dataclass
class Screen3Result:
    """Результат анализа 3-го экрана (сигналы)"""
    signal_type: str = ""  # BUY/SELL
    signal_subtype: str = "M15"
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    signal_strength: str = "WEAK"
    trigger_pattern: str = ""
    confidence: float = 0.0
    expiration_time: Optional[datetime] = None
    passed: bool = False
    indicators: Dict[str, Any] = field(default_factory=dict)
    rejection_reason: str = ""
    order_type: str = "MARKET"
    current_price_at_signal: float = 0.0  # ФАЗА 1.4.1: реальная цена

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_type": self.signal_type,
            "signal_subtype": self.signal_subtype,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "signal_strength": self.signal_strength,
            "trigger_pattern": self.trigger_pattern,
            "confidence": self.confidence,
            "expiration_time": self.expiration_time.isoformat() if self.expiration_time else None,
            "passed": self.passed,
            "indicators": self.indicators,
            "order_type": self.order_type,
            "current_price_at_signal": self.current_price_at_signal
        }


class Screen3SignalGenerator:
    """
    Анализатор для третьего экрана - генерация сигналов M15
    ФАЗА 1.4.1: Исправлены источники цены, унифицирован формат
    """

    def __init__(self, config=None):
        self.config = config or {}
        analysis_config = self.config.get('analysis', {})

        # Настройки M15
        m15_config = analysis_config.get('signal_types', {}).get('m15', {})
        self.min_rr_ratio = m15_config.get('min_rr_ratio', 3.0)
        self.expiration_hours = m15_config.get('expiration_hours', 3)
        self.order_type = m15_config.get('order_type', 'MARKET')

        self.max_risk_pct = analysis_config.get('max_risk_per_trade_pct', 2.0) / 100
        self.max_sl_distance_pct = analysis_config.get('risk_management', {}).get('max_sl_distance_pct', 5.0) / 100
        self.max_entry_distance_pct = analysis_config.get('max_entry_distance_pct', 10.0)

        indicators_config = analysis_config.get('indicators', {})
        self.stochastic_periods = {
            'k': indicators_config.get('stochastic_k_period', 14),
            'd': indicators_config.get('stochastic_d_period', 3),
            'slowing': indicators_config.get('stochastic_slowing', 3)
        }
        self.rsi_period = indicators_config.get('rsi_period', 14)
        self.atr_period = indicators_config.get('atr_period', 14)

        self.stochastic_oversold_level = indicators_config.get('stochastic_oversold_level', 20)
        self.stochastic_overbought_level = indicators_config.get('stochastic_overbought_level', 80)
        self.stochastic_weak_oversold = indicators_config.get('stochastic_weak_oversold', 30)
        self.stochastic_weak_overbought = indicators_config.get('stochastic_weak_overbought', 70)

        risk_management_config = analysis_config.get('risk_management', {})
        self.min_sl_distance_absolute_pct = risk_management_config.get('min_sl_distance_absolute_pct', 0.5)
        self.sl_safety_buffer_pct = risk_management_config.get('sl_safety_buffer_pct', 0.1)
        self.tp_safety_buffer_pct = risk_management_config.get('tp_safety_buffer_pct', 0.5)
        self.tp_atr_multiplier = risk_management_config.get('tp_atr_multiplier', 4.0)
        self.min_tp_floor_pct = risk_management_config.get('min_tp_floor_pct', 0.97)
        self.max_tp_ceiling_pct = risk_management_config.get('max_tp_ceiling_pct', 1.20)

        patterns_config = analysis_config.get('patterns', {})
        self.pin_bar_min_close_position_bullish = patterns_config.get('pin_bar_min_close_position_bullish', 0.66)
        self.pin_bar_min_close_position_bearish = patterns_config.get('pin_bar_min_close_position_bearish', 0.33)
        self.pin_bar_close_position_mid = patterns_config.get('pin_bar_close_position_mid', 0.5)
        self.engulfing_confidence_base = patterns_config.get('engulfing_confidence_base', 0.6)
        self.engulfing_confidence_multiplier = patterns_config.get('engulfing_confidence_multiplier', 0.2)
        self.engulfing_max_confidence = patterns_config.get('engulfing_max_confidence', 0.85)
        self.morning_star_confidence_base = patterns_config.get('morning_star_confidence_base', 0.75)
        self.morning_star_body_multiplier = patterns_config.get('morning_star_body_multiplier', 0.1)
        self.morning_star_max_confidence = patterns_config.get('morning_star_max_confidence', 0.9)

        confirmation_config = analysis_config.get('confirmation', {})
        self.screen1_min_confidence = confirmation_config.get('screen1_min_confidence', 0.65)
        self.screen2_min_confidence = confirmation_config.get('screen2_min_confidence', 0.55)
        self.rr_quality_tolerance = confirmation_config.get('rr_quality_tolerance', 0.1)

        structure_config = analysis_config.get('structure', {})
        self.ma_crossover_min_candles = structure_config.get('ma_crossover_min_candles', 25)
        self.ma_bounce_max_distance_pct = structure_config.get('ma_bounce_max_distance_pct', 2.0)
        self.ma_touch_tolerance_pct = structure_config.get('ma_touch_tolerance_pct', 0.5)

        signal_config = analysis_config.get('signal_generation', {})
        self.strong_confidence_threshold = signal_config.get('strong_confidence_threshold', 0.8)
        self.moderate_confidence_threshold = signal_config.get('moderate_confidence_threshold', 0.65)
        self.pattern_confidence_bonus = signal_config.get('pattern_confidence_bonus', 0.1)
        self.divergence_confidence_bonus = signal_config.get('divergence_confidence_bonus', 0.1)
        self.stochastic_confidence_bonus = signal_config.get('stochastic_confidence_bonus', 0.05)
        self.max_confidence = signal_config.get('max_confidence', 0.95)
        self.ma_crossover_lookback_candles = signal_config.get('ma_crossover_lookback_candles', 5)
        self.ma_bounce_tolerance_upper_pct = signal_config.get('ma_bounce_tolerance_upper_pct', 1.0)
        self.ma_bounce_tolerance_lower_pct = signal_config.get('ma_bounce_tolerance_lower_pct', 0.5)

        self.min_sl_distance_absolute_pct = risk_management_config.get('min_sl_distance_absolute_pct', 0.5)
        self.min_tp_distance_pct = risk_management_config.get('min_tp_distance_pct', 1.0)

        self.m15_config = m15_config

        logger.info(f"✅ Screen3SignalGenerator настроен для M15 (Фаза 1.4.1)")
        logger.info(f"   Min R/R: {self.min_rr_ratio}:1, {self.expiration_hours}ч, ордер: {self.order_type}")
        logger.info(f"   Проверка SL: BUY → SL < Entry, SELL → SL > Entry")
        logger.info(f"   Entry = best_zone (из Screen2)")

    def _format_price(self, price: float) -> str:
        """Унифицированное форматирование цены"""
        if price < 0.01:
            return f"{price:.6f}"
        elif price < 0.1:
            return f"{price:.5f}"
        elif price < 1:
            return f"{price:.4f}"
        elif price < 10:
            return f"{price:.3f}"
        elif price < 100:
            return f"{price:.2f}"
        else:
            return f"{price:.2f}"

    def _round_price(self, price: float, symbol: str = "") -> float:
        """Округление цены с учётом tick_size"""
        try:
            if price < 0.001:
                return round(price, 6)
            elif price < 0.01:
                return round(price, 5)
            elif price < 0.1:
                return round(price, 4)
            elif price < 1:
                return round(price, 3)
            elif price < 10:
                return round(price, 2)
            else:
                return round(price, 2)
        except:
            return round(price, 2)

    def generate_signal(self, symbol: str, m15_klines: List, m5_klines: List,
                        screen1_result: Any, screen2_result: Any,
                        real_current_price: float = None) -> Screen3Result:
        """
        Основной метод генерации M15 сигнала

        ФАЗА 1.5.2 — ИСПРАВЛЕНИЕ: ЗОНА ИМЕЕТ ПРИОРИТЕТ!
        - Если цена В ЗОНЕ или БЛИЗКО (≤2%) → входим по рынку
        - Если цена ДАЛЕКО от зоны → ОТКАЗ
        """
        logger.info(f"⚡ {symbol} - Генерация M15 сигнала")
        result = Screen3Result()
        result.signal_subtype = "M15"
        result.order_type = "MARKET"

        try:
            if not m15_klines or len(m15_klines) < 10:
                reason = f"Недостаточно M15 данных: {len(m15_klines) if m15_klines else 0}"
                logger.warning(f"❌ {symbol}: {reason}")
                result.rejection_reason = reason
                return result

            logger.info(f"Анализ M15 данных: {len(m15_klines)} свечей")

            patterns = self._find_chart_patterns_m15(m15_klines, screen1_result.trend_direction)
            rsi_divergence = self._analyze_rsi_divergence_m15(m15_klines, screen1_result.trend_direction)

            highs = [float(k[2]) for k in m15_klines]
            lows = [float(k[3]) for k in m15_klines]
            closes = [float(k[4]) for k in m15_klines]

            stochastic_data = self._calculate_stochastic(highs, lows, closes,
                                                         self.stochastic_periods['k'],
                                                         self.stochastic_periods['d'],
                                                         self.stochastic_periods['slowing'])

            has_trigger = bool(patterns or rsi_divergence)

            stochastic_signal = False
            if stochastic_data:
                if screen1_result.trend_direction == "BULL":
                    stochastic_signal = (
                            (stochastic_data["oversold"] or stochastic_data.get("k_line",
                                                                                50) < self.stochastic_weak_oversold) and
                            stochastic_data["k_line"] > stochastic_data["d_line"]
                    )
                else:
                    stochastic_signal = (
                            (stochastic_data["overbought"] or stochastic_data.get("k_line",
                                                                                  50) > self.stochastic_weak_overbought) and
                            stochastic_data["k_line"] < stochastic_data["d_line"]
                    )

            should_generate = (
                    screen1_result.passed and
                    screen2_result.passed and
                    screen1_result.confidence_score > self.screen1_min_confidence and
                    screen2_result.confidence > self.screen2_min_confidence and
                    (has_trigger or stochastic_signal)
            )

            if not should_generate:
                reason = f"Условия не выполнены: screen1={screen1_result.passed}, screen2={screen2_result.passed}, patterns={len(patterns)}"
                logger.info(f"⏭️ {symbol}: {reason}")
                result.rejection_reason = reason
                return result

            signal_type = "BUY" if screen1_result.trend_direction == "BULL" else "SELL"

            # ========== ФАЗА 1.5.2: ИСПРАВЛЕНИЕ — ЗОНА ИМЕЕТ ПРИОРИТЕТ! ==========
            zone_low = getattr(screen2_result, 'zone_low', 0)
            zone_high = getattr(screen2_result, 'zone_high', 0)
            best_zone = getattr(screen2_result, 'best_zone', 0)

            # Максимальное допустимое расстояние до зоны (2%)
            MAX_DISTANCE_TO_ZONE_PCT = 2.0

            if zone_low > 0 and zone_high > 0 and real_current_price and real_current_price > 0:
                if zone_low <= real_current_price <= zone_high:
                    # ✅ Цена В ЗОНЕ — идеальный вход!
                    entry_price = real_current_price
                    logger.info(
                        f"✅ {symbol}: цена {self._format_price(real_current_price)} В ЗОНЕ {self._format_price(zone_low)}-{self._format_price(zone_high)}")
                else:
                    # Цена вне зоны — проверяем расстояние
                    zone_center = (zone_low + zone_high) / 2
                    distance_to_zone = abs(real_current_price - zone_center) / zone_center * 100

                    if distance_to_zone <= MAX_DISTANCE_TO_ZONE_PCT:
                        # ⚠️ Цена близко к зоне — разрешаем вход
                        entry_price = real_current_price
                        logger.info(
                            f"⚠️ {symbol}: цена {self._format_price(real_current_price)} близко к зоне ({distance_to_zone:.1f}% ≤ {MAX_DISTANCE_TO_ZONE_PCT}%)")
                    else:
                        # ❌ Цена далеко от зоны — ОТКАЗ!
                        result.rejection_reason = f"Цена {self._format_price(real_current_price)} далеко от зоны {self._format_price(zone_low)}-{self._format_price(zone_high)} ({distance_to_zone:.1f}% > {MAX_DISTANCE_TO_ZONE_PCT}%)"
                        logger.warning(f"❌ {symbol}: {result.rejection_reason}")
                        return result
            elif real_current_price is not None and real_current_price > 0:
                # Нет зоны — используем текущую цену
                entry_price = real_current_price
                logger.info(f"📊 {symbol}: нет зоны, используем реальную цену {self._format_price(entry_price)}")
            elif best_zone and best_zone > 0:
                entry_price = best_zone
                logger.info(f"📊 {symbol}: используем best_zone {self._format_price(entry_price)}")
            else:
                entry_price = closes[-1] if closes else 0
                logger.info(f"📊 {symbol}: fallback на M15 close {self._format_price(entry_price)}")

            if entry_price <= 0:
                result.rejection_reason = "Некорректная цена входа"
                return result

            # Сохраняем реальную цену
            result.current_price_at_signal = entry_price

            atr = self._calculate_atr(highs, lows, closes, self.atr_period, entry_price)

            stop_loss = self._calculate_stop_loss(
                entry_price=entry_price,
                signal_type=signal_type,
                atr=atr
            )

            if stop_loss is None:
                result.rejection_reason = "Не удалось рассчитать Stop Loss"
                return result

            # Проверка SL относительно Entry
            if signal_type == "BUY" and stop_loss >= entry_price:
                result.rejection_reason = f"SL ({self._format_price(stop_loss)}) >= Entry ({self._format_price(entry_price)}) для BUY"
                logger.warning(f"❌ {symbol}: {result.rejection_reason}")
                return result

            if signal_type == "SELL" and stop_loss <= entry_price:
                result.rejection_reason = f"SL ({self._format_price(stop_loss)}) <= Entry ({self._format_price(entry_price)}) для SELL"
                logger.warning(f"❌ {symbol}: {result.rejection_reason}")
                return result

            risk = abs(entry_price - stop_loss)
            reward = risk * self.min_rr_ratio

            if signal_type == "BUY":
                take_profit = entry_price + reward
            else:
                take_profit = entry_price - reward

            take_profit = self._round_price(take_profit)

            # Проверка корректности SL и TP
            if signal_type == "BUY":
                if stop_loss >= entry_price:
                    result.rejection_reason = f"SL >= Entry для BUY"
                    return result
                if take_profit <= entry_price:
                    result.rejection_reason = f"TP <= Entry для BUY"
                    return result
            else:
                if stop_loss <= entry_price:
                    result.rejection_reason = f"SL <= Entry для SELL"
                    return result
                if take_profit >= entry_price:
                    result.rejection_reason = f"TP >= Entry для SELL"
                    return result

            min_sl_distance = entry_price * (self.min_sl_distance_absolute_pct / 100)
            if abs(stop_loss - entry_price) < min_sl_distance:
                result.rejection_reason = f"SL слишком близко к Entry"
                return result

            rr_ratio = abs(take_profit - entry_price) / abs(stop_loss - entry_price)
            if rr_ratio < self.min_rr_ratio - 0.01:
                result.rejection_reason = f"R/R {rr_ratio:.2f}:1 < {self.min_rr_ratio}:1"
                return result

            # Расчёт уверенности
            base_confidence = (screen1_result.confidence_score + screen2_result.confidence) / 2

            if patterns:
                best_pattern = max(patterns, key=lambda x: x.get("confidence", 0))
                pattern_confidence = best_pattern.get("confidence", 0)
                base_confidence = (base_confidence + pattern_confidence) / 2

            if rsi_divergence:
                base_confidence = min(base_confidence + self.divergence_confidence_bonus, self.max_confidence)

            if stochastic_signal:
                base_confidence = min(base_confidence + self.stochastic_confidence_bonus, self.max_confidence)

            if rr_ratio >= self.min_rr_ratio * 1.5:
                base_confidence = min(base_confidence + self.pattern_confidence_bonus, self.max_confidence)

            if base_confidence > self.strong_confidence_threshold:
                signal_strength = "STRONG"
            elif base_confidence > self.moderate_confidence_threshold:
                signal_strength = "MODERATE"
            else:
                signal_strength = "WEAK"

            trigger_pattern = "TREND_FOLLOW"
            if patterns:
                trigger_pattern = patterns[0].get("type", "UNKNOWN")
            elif rsi_divergence:
                trigger_pattern = rsi_divergence.get("type", "DIVERGENCE")
            elif stochastic_signal:
                trigger_pattern = "STOCHASTIC_CROSS"

            result.signal_type = signal_type
            result.entry_price = self._round_price(entry_price)
            result.stop_loss = self._round_price(stop_loss)
            result.take_profit = take_profit
            result.signal_strength = signal_strength
            result.trigger_pattern = trigger_pattern
            result.confidence = base_confidence
            result.expiration_time = datetime.now() + timedelta(hours=self.expiration_hours)
            result.passed = True
            result.indicators = {
                "stochastic_k": stochastic_data.get("k_line", 50),
                "stochastic_d": stochastic_data.get("d_line", 50),
                "stochastic_oversold": stochastic_data.get("oversold", False),
                "stochastic_overbought": stochastic_data.get("overbought", False),
                "has_rsi_divergence": bool(rsi_divergence),
                "has_pattern": bool(patterns),
                "pattern_count": len(patterns),
                "atr": round(atr, 4),
                "risk_reward_ratio": rr_ratio,
                "risk_pct": abs(stop_loss - entry_price) / entry_price * 100
            }

            logger.info(
                f"✅ M15 сигнал сгенерирован: {signal_type} @ {self._format_price(entry_price)}, R/R={rr_ratio:.2f}:1")
            return result

        except Exception as e:
            logger.error(f"❌ Ошибка генерации M15 сигнала для {symbol}: {str(e)}")
            result.rejection_reason = f"Ошибка: {str(e)}"
            return result

    def _validate_price_range(self, price: float, symbol: str, market_data: Dict = None) -> bool:
        try:
            if price <= 0:
                return False
            return True
        except:
            return True

    def _calculate_stop_loss(self, entry_price: float, signal_type: str, atr: float,
                             resistance_level: Optional[float] = None,
                             support_level: Optional[float] = None) -> Optional[float]:
        logger.info(
            f"🔍 РАСЧЕТ STOP LOSS для {signal_type} @ {self._format_price(entry_price)}, ATR={self._format_price(atr)}")

        if atr == 0 or atr is None:
            atr = entry_price * 0.005 if entry_price > 0 else 0.01

        if entry_price <= 0:
            return None

        if signal_type == "SELL":
            stop_by_atr = entry_price + (atr * 1.5)
        else:
            stop_by_atr = entry_price - (atr * 1.5)

        min_distance_pct = self.min_sl_distance_absolute_pct / 100
        min_distance = entry_price * min_distance_pct

        if entry_price < 0.01:
            min_distance = max(min_distance, 0.0001)

        if signal_type == "SELL":
            if stop_by_atr <= entry_price + min_distance:
                stop_by_atr = entry_price + min_distance
            if stop_by_atr <= entry_price:
                stop_by_atr = entry_price * (1 + self.sl_safety_buffer_pct / 100)
        else:
            if stop_by_atr >= entry_price - min_distance:
                stop_by_atr = entry_price - min_distance
            if stop_by_atr >= entry_price:
                stop_by_atr = entry_price * (1 - self.sl_safety_buffer_pct / 100)

        max_risk_distance = entry_price * self.max_risk_pct

        candidates = [stop_by_atr]

        if signal_type == "SELL":
            max_stop = entry_price + max_risk_distance
            candidates.append(max_stop)
            if resistance_level and resistance_level > entry_price:
                candidates.append(resistance_level)
            stop_loss = min(candidates)
        else:
            max_stop = entry_price - max_risk_distance
            candidates.append(max_stop)
            if support_level and support_level < entry_price:
                candidates.append(support_level)
            stop_loss = max(candidates)

        if signal_type == "SELL":
            max_stop_allowed = entry_price * (1 + self.max_sl_distance_pct)
            min_stop_required = entry_price * (1 + self.sl_safety_buffer_pct / 100)
            stop_loss = max(min(stop_loss, max_stop_allowed), min_stop_required)
        else:
            min_stop_allowed = entry_price * (1 - self.max_sl_distance_pct)
            max_stop_required = entry_price * (1 - self.sl_safety_buffer_pct / 100)
            stop_loss = min(max(stop_loss, min_stop_allowed), max_stop_required)

        if abs(stop_loss - entry_price) < entry_price * 0.0001:
            if signal_type == 'BUY':
                stop_loss = entry_price - (entry_price * 0.005)
            else:
                stop_loss = entry_price + (entry_price * 0.005)

        stop_loss = self._round_price(stop_loss)

        distance_pct = abs((stop_loss - entry_price) / entry_price * 100) if entry_price > 0 else 0
        if distance_pct < self.min_sl_distance_absolute_pct * 0.8:
            return None

        logger.info(f"✅ Stop Loss для {signal_type}: {self._format_price(stop_loss)} (расстояние: {distance_pct:.3f}%)")
        return stop_loss

    def _calculate_atr(self, high_prices: List[float], low_prices: List[float],
                       close_prices: List[float], period: int = None,
                       entry_price: float = None) -> float:
        if period is None:
            period = self.atr_period

        try:
            if len(high_prices) < period + 1:
                if entry_price is not None and entry_price > 0:
                    return entry_price * 0.005
                return 0.01

            tr_values = []
            for i in range(1, len(high_prices)):
                tr = max(
                    high_prices[i] - low_prices[i],
                    abs(high_prices[i] - close_prices[i - 1]),
                    abs(low_prices[i] - close_prices[i - 1])
                )
                tr_values.append(tr)

            recent_tr = tr_values[-period:] if len(tr_values) >= period else tr_values
            atr = sum(recent_tr) / len(recent_tr)

            if atr == 0:
                atr = entry_price * 0.005 if entry_price else 0.01

            return atr

        except Exception as e:
            logger.error(f"❌ Ошибка расчета ATR: {e}")
            return 0.01

    def _calculate_stochastic(self, high_prices: List[float], low_prices: List[float],
                              close_prices: List[float], k_period: int = None,
                              d_period: int = None, slowing: int = None) -> Dict[str, Any]:
        if k_period is None:
            k_period = self.stochastic_periods['k']
        if d_period is None:
            d_period = self.stochastic_periods['d']
        if slowing is None:
            slowing = self.stochastic_periods['slowing']

        try:
            if len(high_prices) < k_period + d_period:
                return {"k_line": 50, "d_line": 50, "oversold": False, "overbought": False}

            k_values = []
            for i in range(k_period - 1, len(high_prices)):
                if i >= len(close_prices):
                    break

                high_range = high_prices[i - k_period + 1:i + 1]
                low_range = low_prices[i - k_period + 1:i + 1]
                current_close = close_prices[i]

                highest_high = max(high_range)
                lowest_low = min(low_range)

                if highest_high - lowest_low == 0:
                    k_value = 50
                else:
                    k_value = 100 * (current_close - lowest_low) / (highest_high - lowest_low)

                if len(k_values) >= slowing - 1:
                    k_slowed = sum(k_values[-(slowing - 1):] + [k_value]) / slowing
                    k_values.append(k_slowed)
                else:
                    k_values.append(k_value)

            d_values = []
            for i in range(d_period - 1, len(k_values)):
                d_value = sum(k_values[i - d_period + 1:i + 1]) / d_period
                d_values.append(d_value)

            current_k = k_values[-1] if k_values else 50
            current_d = d_values[-1] if d_values else 50

            oversold = current_k < self.stochastic_oversold_level and current_d < self.stochastic_oversold_level
            overbought = current_k > self.stochastic_overbought_level and current_d > self.stochastic_overbought_level

            return {
                "k_line": round(current_k, 2),
                "d_line": round(current_d, 2),
                "oversold": oversold,
                "overbought": overbought,
                "k_values": k_values,
                "d_values": d_values
            }

        except Exception as e:
            logger.error(f"Ошибка расчета Stochastic: {e}")
            return {"k_line": 50, "d_line": 50, "oversold": False, "overbought": False}

    def _find_chart_patterns_m15(self, m15_klines: List, trend_direction: str) -> List[Dict]:
        patterns = []

        try:
            if len(m15_klines) < 3:
                return patterns

            pin_bar = self._analyze_pin_bar_m15(m15_klines, trend_direction)
            if pin_bar:
                patterns.append(pin_bar)

            engulfing = self._analyze_engulfing_m15(m15_klines, trend_direction)
            if engulfing:
                patterns.append(engulfing)

            morning_evening_star = self._analyze_morning_evening_star_m15(m15_klines, trend_direction)
            if morning_evening_star:
                patterns.append(morning_evening_star)

            ma_crossover = self._analyze_ma_crossover_m15(m15_klines, trend_direction)
            if ma_crossover:
                patterns.append(ma_crossover)

            ma_bounce = self._analyze_ma_bounce_m15(m15_klines, trend_direction)
            if ma_bounce:
                patterns.append(ma_bounce)

            return patterns

        except Exception as e:
            logger.error(f"Ошибка поиска паттернов: {e}")
            return patterns

    def _analyze_pin_bar_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        try:
            if len(m15_klines) < 2:
                return None

            current_candle = m15_klines[-1]
            open_price = float(current_candle[1])
            high_price = float(current_candle[2])
            low_price = float(current_candle[3])
            close_price = float(current_candle[4])

            body_size = abs(close_price - open_price)
            total_range = high_price - low_price

            if total_range == 0:
                return None

            upper_shadow = high_price - max(open_price, close_price)
            lower_shadow = min(open_price, close_price) - low_price
            body_ratio = body_size / total_range
            is_bullish = close_price > open_price

            signal = None
            confidence = 0.0

            patterns_config = self.config.get('analysis', {}).get('patterns', {})
            pin_bar_shadow_ratio = patterns_config.get('pin_bar_shadow_ratio', 2.0)
            pin_bar_body_ratio = patterns_config.get('pin_bar_body_ratio', 0.3)

            if (trend_direction == "BULL" and
                    lower_shadow > pin_bar_shadow_ratio * body_size and
                    body_ratio < pin_bar_body_ratio and
                    upper_shadow < body_size and
                    is_bullish):

                close_position = (close_price - low_price) / total_range
                if close_position > self.pin_bar_min_close_position_bullish:
                    confidence = 0.8
                elif close_position > self.pin_bar_close_position_mid:
                    confidence = 0.7
                else:
                    confidence = 0.6

                signal = {
                    "type": PatternType.PIN_BAR.value,
                    "subtype": "BULLISH_HAMMER",
                    "confidence": confidence,
                    "price_level": close_price
                }

            elif (trend_direction == "BEAR" and
                  upper_shadow > pin_bar_shadow_ratio * body_size and
                  body_ratio < pin_bar_body_ratio and
                  lower_shadow < body_size and
                  not is_bullish):

                close_position = (close_price - low_price) / total_range
                if close_position < self.pin_bar_min_close_position_bearish:
                    confidence = 0.8
                elif close_position < self.pin_bar_close_position_mid:
                    confidence = 0.7
                else:
                    confidence = 0.6

                signal = {
                    "type": PatternType.PIN_BAR.value,
                    "subtype": "BEARISH_SHOOTING_STAR",
                    "confidence": confidence,
                    "price_level": close_price
                }

            return signal

        except Exception as e:
            logger.error(f"Ошибка анализа Pin Bar: {e}")
            return None

    def _analyze_engulfing_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        try:
            if len(m15_klines) < 2:
                return None

            prev_candle = m15_klines[-2]
            current_candle = m15_klines[-1]

            prev_open = float(prev_candle[1])
            prev_close = float(prev_candle[4])
            current_open = float(current_candle[1])
            current_close = float(current_candle[4])

            prev_body_size = abs(prev_close - prev_open)
            current_body_size = abs(current_close - current_open)

            if prev_body_size == 0 or current_body_size == 0:
                return None

            patterns_config = self.config.get('analysis', {}).get('patterns', {})
            engulfing_min_ratio = patterns_config.get('engulfing_min_ratio', 1.0)

            if trend_direction == "BULL":
                is_bullish_engulfing = (
                        prev_close < prev_open and
                        current_close > current_open and
                        current_open <= prev_close and
                        current_close >= prev_open
                )

                if is_bullish_engulfing:
                    engulfing_ratio = current_body_size / prev_body_size
                    if engulfing_ratio >= engulfing_min_ratio:
                        confidence = min(
                            self.engulfing_confidence_base + (
                                        engulfing_ratio - 1) * self.engulfing_confidence_multiplier,
                            self.engulfing_max_confidence
                        )
                        return {
                            "type": PatternType.ENGULFING.value,
                            "subtype": "BULLISH_ENGULFING",
                            "confidence": confidence,
                            "price_level": current_close
                        }

            elif trend_direction == "BEAR":
                is_bearish_engulfing = (
                        prev_close > prev_open and
                        current_close < current_open and
                        current_open >= prev_close and
                        current_close <= prev_open
                )

                if is_bearish_engulfing:
                    engulfing_ratio = current_body_size / prev_body_size
                    if engulfing_ratio >= engulfing_min_ratio:
                        confidence = min(
                            self.engulfing_confidence_base + (
                                        engulfing_ratio - 1) * self.engulfing_confidence_multiplier,
                            self.engulfing_max_confidence
                        )
                        return {
                            "type": PatternType.ENGULFING.value,
                            "subtype": "BEARISH_ENGULFING",
                            "confidence": confidence,
                            "price_level": current_close
                        }

            return None

        except Exception as e:
            logger.error(f"Ошибка анализа Engulfing: {e}")
            return None

    def _analyze_morning_evening_star_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        try:
            if len(m15_klines) < 3:
                return None

            candles = m15_klines[-3:]
            opens = [float(c[1]) for c in candles]
            closes = [float(c[4]) for c in candles]

            candle_types = []
            for i in range(3):
                if closes[i] > opens[i]:
                    candle_types.append("BULLISH")
                elif closes[i] < opens[i]:
                    candle_types.append("BEARISH")
                else:
                    candle_types.append("DOJI")

            if trend_direction == "BULL":
                is_morning_star = (
                        candle_types[0] == "BEARISH" and
                        candle_types[1] in ["DOJI", "BEARISH"] and
                        candle_types[2] == "BULLISH" and
                        closes[2] > opens[0]
                )

                if is_morning_star:
                    confidence = self.morning_star_confidence_base
                    return {
                        "type": PatternType.MORNING_STAR.value,
                        "subtype": "MORNING_STAR",
                        "confidence": confidence,
                        "price_level": closes[2]
                    }

            elif trend_direction == "BEAR":
                is_evening_star = (
                        candle_types[0] == "BULLISH" and
                        candle_types[1] in ["DOJI", "BULLISH"] and
                        candle_types[2] == "BEARISH" and
                        closes[2] < opens[0]
                )

                if is_evening_star:
                    confidence = self.morning_star_confidence_base
                    return {
                        "type": PatternType.EVENING_STAR.value,
                        "subtype": "EVENING_STAR",
                        "confidence": confidence,
                        "price_level": closes[2]
                    }

            return None

        except Exception as e:
            logger.error(f"Ошибка анализа Morning/Evening Star: {e}")
            return None

    def _analyze_ma_crossover_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        try:
            if len(m15_klines) < self.ma_crossover_min_candles:
                return None

            closes = [float(k[4]) for k in m15_klines[-50:]]

            def calculate_simple_ema(prices, period):
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

            ema_20 = calculate_simple_ema(closes, 20)
            ema_50 = calculate_simple_ema(closes, 50)

            if len(ema_20) < 2 or len(ema_50) < 2:
                return None

            current_close = closes[-1]
            current_ema20 = ema_20[-1]
            current_ema50 = ema_50[-1]

            if trend_direction == "BULL" and current_close > current_ema20 and current_ema20 > current_ema50:
                return {
                    "type": PatternType.MA_CROSSOVER.value,
                    "subtype": "BULLISH_CROSSOVER",
                    "confidence": 0.7,
                    "price_level": current_close
                }

            elif trend_direction == "BEAR" and current_close < current_ema20 and current_ema20 < current_ema50:
                return {
                    "type": PatternType.MA_CROSSOVER.value,
                    "subtype": "BEARISH_CROSSOVER",
                    "confidence": 0.7,
                    "price_level": current_close
                }

            return None

        except Exception as e:
            logger.error(f"Ошибка анализа MA кроссовера: {e}")
            return None

    def _analyze_ma_bounce_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        try:
            if len(m15_klines) < 10:
                return None

            closes = [float(k[4]) for k in m15_klines[-50:]]

            def calculate_simple_ema(prices, period):
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

            ema_20 = calculate_simple_ema(closes, 20)

            if len(ema_20) < 2:
                return None

            current_close = closes[-1]
            current_ema20 = ema_20[-1]
            distance_pct = abs(current_close - current_ema20) / current_ema20 * 100

            if distance_pct > self.ma_bounce_max_distance_pct:
                return None

            if trend_direction == "BULL" and current_close > current_ema20:
                return {
                    "type": PatternType.MA_BOUNCE.value,
                    "subtype": "BULLISH_BOUNCE",
                    "confidence": 0.7,
                    "price_level": current_close
                }

            elif trend_direction == "BEAR" and current_close < current_ema20:
                return {
                    "type": PatternType.MA_BOUNCE.value,
                    "subtype": "BEARISH_BOUNCE",
                    "confidence": 0.7,
                    "price_level": current_close
                }

            return None

        except Exception as e:
            logger.error(f"Ошибка анализа MA отскока: {e}")
            return None

    def _analyze_rsi_divergence_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        try:
            if len(m15_klines) < 15:
                return None

            closes = [float(k[4]) for k in m15_klines]
            rsi = self._calculate_rsi(closes, self.rsi_period)

            if len(rsi) < 10:
                return None

            recent_closes = closes[-10:]
            recent_rsi = rsi[-10:]

            price_lows = []
            rsi_lows = []

            for i in range(1, len(recent_closes) - 1):
                if recent_closes[i] < recent_closes[i - 1] and recent_closes[i] < recent_closes[i + 1]:
                    price_lows.append((i, recent_closes[i]))

            for i in range(1, len(recent_rsi) - 1):
                if recent_rsi[i] < recent_rsi[i - 1] and recent_rsi[i] < recent_rsi[i + 1]:
                    rsi_lows.append((i, recent_rsi[i]))

            if len(price_lows) >= 2 and len(rsi_lows) >= 2 and trend_direction == "BULL":
                last_price_low = price_lows[-1][1]
                prev_price_low = price_lows[-2][1]
                last_rsi_low = rsi_lows[-1][1]
                prev_rsi_low = rsi_lows[-2][1]

                if last_price_low < prev_price_low and last_rsi_low > prev_rsi_low:
                    return {
                        "type": PatternType.BULLISH_DIVERGENCE.value,
                        "confidence": 0.8,
                        "price_level": recent_closes[-1]
                    }

            return None

        except Exception as e:
            logger.error(f"Ошибка анализа дивергенции RSI: {e}")
            return None

    def _calculate_rsi(self, prices: List[float], period: int = None) -> List[float]:
        if period is None:
            period = self.rsi_period

        try:
            if len(prices) < period + 1:
                return []

            deltas = np.diff(prices)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)

            avg_gains = np.zeros(len(prices))
            avg_losses = np.zeros(len(prices))

            avg_gains[period] = np.mean(gains[:period])
            avg_losses[period] = np.mean(losses[:period])

            for i in range(period + 1, len(prices)):
                avg_gains[i] = (avg_gains[i - 1] * (period - 1) + gains[i - 1]) / period
                avg_losses[i] = (avg_losses[i - 1] * (period - 1) + losses[i - 1]) / period

            rs = np.zeros_like(avg_gains)
            for i in range(len(avg_gains)):
                if avg_losses[i] == 0:
                    rs[i] = 100 if avg_gains[i] > 0 else 50
                else:
                    rs[i] = avg_gains[i] / avg_losses[i]

            rsi = 100 - (100 / (1 + rs))
            return rsi.tolist()

        except Exception as e:
            logger.error(f"Ошибка расчета RSI: {e}")
            return []


__all__ = ['Screen3Result', 'Screen3SignalGenerator', 'PatternType']