# core/screen3_signal_generator.py
"""
🎯 ЭКРАН 3 - ПОЛНАЯ ГЕНЕРАЦИЯ СИГНАЛОВ (M15/M5 таймфреймы)
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
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
    signal_type: str = ""
    signal_subtype: str = "LIMIT"  # НОВОЕ для Фазы 1.2
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    signal_strength: str = "WEAK"
    trigger_pattern: str = ""
    confidence: float = 0.0
    expiration_time: Optional[datetime] = None
    passed: bool = False
    indicators: Dict[str, Any] = field(default_factory=dict)

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
            "indicators": self.indicators
        }


class Screen3SignalGenerator:
    """
    Анализатор для третьего экрана - генерация сигналов
    """

    def __init__(self, config=None):
        self.config = config or {}
        analysis_config = self.config.get('analysis', {})

        # Получаем параметры из конфига с значениями по умолчанию
        self.min_rr_ratio = analysis_config.get('min_rr_ratio', 3.0)
        self.max_risk_pct = analysis_config.get('max_risk_per_trade_pct', 0.02) / 100
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

        # Параметры стохастика
        self.stochastic_oversold_level = indicators_config.get('stochastic_oversold_level', 20)
        self.stochastic_overbought_level = indicators_config.get('stochastic_overbought_level', 80)
        self.stochastic_weak_oversold = indicators_config.get('stochastic_weak_oversold', 30)
        self.stochastic_weak_overbought = indicators_config.get('stochastic_weak_overbought', 70)

        # Параметры рисков
        risk_management_config = analysis_config.get('risk_management', {})
        self.min_sl_distance_absolute_pct = risk_management_config.get('min_sl_distance_absolute_pct', 0.5)
        self.sl_safety_buffer_pct = risk_management_config.get('sl_safety_buffer_pct', 0.1)
        self.tp_safety_buffer_pct = risk_management_config.get('tp_safety_buffer_pct', 0.5)
        self.tp_atr_multiplier = risk_management_config.get('tp_atr_multiplier', 4.0)
        self.min_tp_floor_pct = risk_management_config.get('min_tp_floor_pct', 0.97)
        self.max_tp_ceiling_pct = risk_management_config.get('max_tp_ceiling_pct', 1.20)
        self.rr_quality_bonus_threshold = risk_management_config.get('rr_quality_bonus_threshold', 2.0)

        # Параметры паттернов
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

        # Параметры подтверждения
        confirmation_config = analysis_config.get('confirmation', {})
        self.screen1_min_confidence = confirmation_config.get('screen1_min_confidence', 0.65)
        self.screen2_min_confidence = confirmation_config.get('screen2_min_confidence', 0.55)
        self.rr_quality_tolerance = confirmation_config.get('rr_quality_tolerance', 0.1)

        # Параметры структуры
        structure_config = analysis_config.get('structure', {})
        self.ma_crossover_min_candles = structure_config.get('ma_crossover_min_candles', 25)
        self.ma_bounce_max_distance_pct = structure_config.get('ma_bounce_max_distance_pct', 2.0)
        self.ma_touch_tolerance_pct = structure_config.get('ma_touch_tolerance_pct', 0.5)
        self.rsi_divergence_min_candles = structure_config.get('rsi_divergence_min_candles', 15)

        # Параметры генерации сигналов
        signal_config = analysis_config.get('signal_generation', {})
        self.expiration_hours = signal_config.get('expiration_hours', 4)
        self.strong_confidence_threshold = signal_config.get('strong_confidence_threshold', 0.8)
        self.moderate_confidence_threshold = signal_config.get('moderate_confidence_threshold', 0.65)
        self.pattern_confidence_bonus = signal_config.get('pattern_confidence_bonus', 0.1)
        self.divergence_confidence_bonus = signal_config.get('divergence_confidence_bonus', 0.1)
        self.stochastic_confidence_bonus = signal_config.get('stochastic_confidence_bonus', 0.05)
        self.max_confidence = signal_config.get('max_confidence', 0.95)
        self.ma_crossover_lookback_candles = signal_config.get('ma_crossover_lookback_candles', 5)
        self.ma_bounce_tolerance_upper_pct = signal_config.get('ma_bounce_tolerance_upper_pct', 1.0)
        self.ma_bounce_tolerance_lower_pct = signal_config.get('ma_bounce_tolerance_lower_pct', 0.5)

        # НОВОЕ для Фазы 1.2: Параметры для разных типов сигналов
        signal_types_config = analysis_config.get('signal_types', {})
        limit_config = signal_types_config.get('limit', {})
        self.limit_min_rr = limit_config.get('min_rr_ratio', 3.0)
        self.limit_expiration_hours = limit_config.get('expiration_hours', 24)

        instant_config = signal_types_config.get('instant', {})
        self.instant_min_rr = instant_config.get('min_rr_ratio', 2.0)
        self.instant_expiration_hours = instant_config.get('expiration_hours', 3)

        logger.info(f"✅ Screen3SignalGenerator настроен. Min R/R: {self.min_rr_ratio}:1")
        logger.info(f"   LIMIT: R/R ≥ {self.limit_min_rr}:1, {self.limit_expiration_hours}ч")
        logger.info(f"   INSTANT: R/R ≥ {self.instant_min_rr}:1, {self.instant_expiration_hours}ч")

    def generate_signal(self, symbol: str, m15_klines: List, m5_klines: List,
                        screen1_result: Any, screen2_result: Any) -> Screen3Result:
        """Основной метод генерации сигнала"""
        logger.info(f"⚡ {symbol} - Генерация сигналов M15/M5")
        result = Screen3Result()

        try:
            if not m15_klines or len(m15_klines) < 10:
                logger.warning(f"❌ Недостаточно M15 данных для {symbol}")
                return result

            logger.info(f"Анализ M15 данных: {len(m15_klines)} свечей")

            # Поиск паттернов
            patterns = self._find_chart_patterns_m15(m15_klines, screen1_result.trend_direction)

            # Анализ RSI дивергенции
            rsi_divergence = self._analyze_rsi_divergence_m15(m15_klines, screen1_result.trend_direction)

            # Расчет стохастика
            highs = [float(k[2]) for k in m15_klines]
            lows = [float(k[3]) for k in m15_klines]
            closes = [float(k[4]) for k in m15_klines]

            stochastic_data = self._calculate_stochastic(highs, lows, closes,
                                                         self.stochastic_periods['k'],
                                                         self.stochastic_periods['d'],
                                                         self.stochastic_periods['slowing'])

            # Генерация торгового сигнала
            signal_data = self._generate_trading_signal(
                patterns, rsi_divergence, stochastic_data,
                screen1_result, screen2_result, m15_klines
            )

            if signal_data:
                result.signal_type = signal_data['signal_type']
                result.entry_price = signal_data['entry_price']
                result.stop_loss = signal_data['stop_loss']
                result.take_profit = signal_data['take_profit']
                result.signal_strength = signal_data['strength']
                result.trigger_pattern = signal_data['pattern']
                result.confidence = signal_data['confidence']
                result.passed = True

                # НОВОЕ для Фазы 1.2: Определяем подтип сигнала (LIMIT или INSTANT)
                rr_ratio = signal_data.get('risk_reward_ratio', 0)
                if rr_ratio >= self.limit_min_rr:
                    result.signal_subtype = "LIMIT"
                    result.expiration_time = datetime.now() + timedelta(hours=self.limit_expiration_hours)
                    logger.info(f"📌 Сигнал типа LIMIT (R/R {rr_ratio:.2f}:1 ≥ {self.limit_min_rr}:1)")
                elif rr_ratio >= self.instant_min_rr:
                    result.signal_subtype = "INSTANT"
                    result.expiration_time = datetime.now() + timedelta(hours=self.instant_expiration_hours)
                    logger.info(f"⚡ Сигнал типа INSTANT (R/R {rr_ratio:.2f}:1 ≥ {self.instant_min_rr}:1)")
                else:
                    logger.warning(f"❌ R/R {rr_ratio:.2f}:1 ниже минимального порога INSTANT {self.instant_min_rr}:1")
                    result.passed = False
                    return result

                result.indicators = {
                    "stochastic_k": stochastic_data.get("k_line", 50),
                    "stochastic_d": stochastic_data.get("d_line", 50),
                    "stochastic_oversold": stochastic_data.get("oversold", False),
                    "stochastic_overbought": stochastic_data.get("overbought", False),
                    "has_rsi_divergence": signal_data.get("has_rsi_divergence", False),
                    "has_pattern": signal_data.get("has_pattern", False),
                    "pattern_count": len(patterns),
                    "atr": signal_data.get("atr", 0),
                    "risk_reward_ratio": signal_data.get("risk_reward_ratio", 0),
                    "risk_pct": signal_data.get("risk_pct", 0),
                    "quality_metrics": signal_data.get("quality_metrics", {})
                }

                logger.info(f"✅ Сгенерирован торговый сигнал: {result.signal_type}")

            status = "✅" if result.passed else "❌"
            logger.info(f"{status} {symbol} ЭКРАН 3: {result.signal_type} по {result.entry_price} "
                        f"(SL: {result.stop_loss}, TP: {result.take_profit}, "
                        f"R/R: {result.indicators.get('risk_reward_ratio', 0):.2f}:1)")

            return result

        except Exception as e:
            logger.error(f"❌ Ошибка генерации сигнала для {symbol}: {str(e)}")
            return result

    def _validate_price_range(self, price: float, symbol: str, market_data: Dict = None) -> bool:
        """Проверка реалистичности цены"""
        try:
            if price <= 0:
                logger.warning(f"⚠️ {symbol}: цена <= 0: ${price:.2f}")
                return False

            if market_data:
                high_24h = market_data.get('high_24h', 0)
                low_24h = market_data.get('low_24h', 0)

                if high_24h > 0 and price > high_24h * 1.5:
                    logger.warning(f"⚠️ {symbol}: цена слишком высокая относительно 24h high")
                    return False

                if low_24h > 0 and price < low_24h * 0.5:
                    logger.warning(f"⚠️ {symbol}: цена слишком низкая относительно 24h low")
                    return False

            return True

        except Exception as e:
            logger.error(f"❌ Ошибка проверки цены {symbol}: {e}")
            return True

    def _calculate_stop_loss(self, entry_price: float, signal_type: str, atr: float,
                             resistance_level: Optional[float] = None,
                             support_level: Optional[float] = None) -> float:
        """Расчет Stop Loss с правильной логикой"""

        logger.info(f"🔍 РАСЧЕТ STOP LOSS для {signal_type} @ {entry_price:.6f}, ATR={atr:.6f}")

        if atr == 0:
            logger.warning(f"⚠️ ATR = 0, используем минимальное расстояние")
            atr = entry_price * 0.001

        # Базовый стоп на основе ATR (1.5 ATR)
        if signal_type == "SELL":
            stop_by_atr = entry_price + (atr * 1.5)
            logger.debug(f"  SELL: базовый стоп по ATR: {stop_by_atr:.6f} (entry + {atr * 1.5:.6f})")
        else:
            stop_by_atr = entry_price - (atr * 1.5)
            logger.debug(f"  BUY: базовый стоп по ATR: {stop_by_atr:.6f} (entry - {atr * 1.5:.6f})")

        min_distance_pct = self.min_sl_distance_absolute_pct / 100
        min_distance = entry_price * min_distance_pct

        logger.debug(f"  Минимальное расстояние: {min_distance:.6f} ({min_distance_pct * 100:.3f}%)")

        if signal_type == "SELL":
            if stop_by_atr <= entry_price + min_distance:
                stop_by_atr = entry_price + min_distance
                logger.warning(f"⚠️ SELL: стоп слишком близко, увеличиваем до {stop_by_atr:.6f}")

            if stop_by_atr <= entry_price:
                stop_by_atr = entry_price * (1 + self.sl_safety_buffer_pct / 100)
                logger.warning(f"⚠️ SELL: стоп ниже entry, исправляем на {stop_by_atr:.6f}")

        else:
            if stop_by_atr >= entry_price - min_distance:
                stop_by_atr = entry_price - min_distance
                logger.warning(f"⚠️ BUY: стоп слишком близко, увеличиваем до {stop_by_atr:.6f}")

            if stop_by_atr >= entry_price:
                stop_by_atr = entry_price * (1 - self.sl_safety_buffer_pct / 100)
                logger.warning(f"⚠️ BUY: стоп выше entry, исправляем на {stop_by_atr:.6f}")

        max_risk_distance = entry_price * self.max_risk_pct
        logger.debug(f"  Максимальный риск: {max_risk_distance:.6f} ({self.max_risk_pct * 100:.2f}%)")

        candidates = [stop_by_atr]

        if signal_type == "SELL":
            max_stop = entry_price + max_risk_distance
            candidates.append(max_stop)

            if resistance_level and resistance_level > entry_price:
                candidates.append(resistance_level)
                logger.debug(f"  Кандидат: уровневый стоп по сопротивлению: {resistance_level:.6f}")
        else:
            max_stop = entry_price - max_risk_distance
            candidates.append(max_stop)

            if support_level and support_level < entry_price:
                candidates.append(support_level)
                logger.debug(f"  Кандидат: уровневый стоп по поддержке: {support_level:.6f}")

        if signal_type == "SELL":
            stop_loss = min(candidates)
        else:
            stop_loss = max(candidates)

        logger.debug(f"  Выбран стоп из кандидатов {[f'{c:.6f}' for c in candidates]}: {stop_loss:.6f}")

        if signal_type == "SELL":
            max_stop_allowed = entry_price * (1 + self.max_sl_distance_pct)
            min_stop_required = entry_price * (1 + self.sl_safety_buffer_pct / 100)

            stop_loss = max(min(stop_loss, max_stop_allowed), min_stop_required)
            logger.debug(f"  Корректировка SELL стопа: min={min_stop_required:.6f}, max={max_stop_allowed:.6f}")
        else:
            min_stop_allowed = entry_price * (1 - self.max_sl_distance_pct)
            max_stop_required = entry_price * (1 - self.sl_safety_buffer_pct / 100)

            stop_loss = min(max(stop_loss, min_stop_allowed), max_stop_required)
            logger.debug(f"  Корректировка BUY стопа: min={min_stop_allowed:.6f}, max={max_stop_required:.6f}")

        if signal_type == "SELL" and stop_loss <= entry_price:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: SELL стоп {stop_loss:.6f} <= entry {entry_price:.6f}")
            stop_loss = entry_price * (1.01)
            logger.warning(f"  Аварийная корректировка SELL стопа: {stop_loss:.6f}")

        elif signal_type == "BUY" and stop_loss >= entry_price:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: BUY стоп {stop_loss:.6f} >= entry {entry_price:.6f}")
            stop_loss = entry_price * (0.99)
            logger.warning(f"  Аварийная корректировка BUY стопа: {stop_loss:.6f}")

        distance_pct = abs((stop_loss - entry_price) / entry_price * 100)
        logger.info(f"✅ Stop Loss для {signal_type}: {stop_loss:.6f} "
                    f"(расстояние: {distance_pct:.3f}%, entry: {entry_price:.6f})")

        return stop_loss

    def _calculate_take_profit(self, entry_price: float, stop_loss: float,
                               signal_type: str, atr: float) -> float:
        """Расчет Take Profit с правильным расчетом R/R"""

        logger.info(f"🔍 РАСЧЕТ TP для {signal_type} @ {entry_price:.6f}, SL={stop_loss:.6f}, ATR={atr:.6f}")

        if abs(stop_loss - entry_price) < entry_price * 0.00001:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: стоп слишком близко к entry!")
            if signal_type == "SELL":
                stop_loss = entry_price * (1 + 0.005)
                logger.warning(f"   Аварийная корректировка SELL стопа: {stop_loss:.6f} (+0.5%)")
            else:
                stop_loss = entry_price * (1 - 0.005)
                logger.warning(f"   Аварийная корректировка BUY стопа: {stop_loss:.6f} (-0.5%)")

        if signal_type == "SELL" and stop_loss <= entry_price:
            logger.error(f"❌ НЕПРАВИЛЬНЫЙ SELL СТОП: {stop_loss:.6f} <= {entry_price:.6f}")
            stop_loss = entry_price * 1.005
            logger.warning(f"   Исправляем SELL стоп: {stop_loss:.6f}")

        elif signal_type == "BUY" and stop_loss >= entry_price:
            logger.error(f"❌ НЕПРАВИЛЬНЫЙ BUY СТОП: {stop_loss:.6f} >= {entry_price:.6f}")
            stop_loss = entry_price * 0.995
            logger.warning(f"   Исправляем BUY стоп: {stop_loss:.6f}")

        risk_distance = abs(stop_loss - entry_price)

        if risk_distance < entry_price * 0.0001:
            logger.warning(f"⚠️ Слишком маленький риск: {risk_distance:.6f} ({risk_distance / entry_price * 100:.4f}%)")
            risk_distance = entry_price * 0.005

        target_reward_distance = risk_distance * self.min_rr_ratio

        logger.debug(f"🔍 TP CALCULATION DEBUG:")
        logger.debug(f"   Entry: {entry_price:.6f}")
        logger.debug(f"   Stop Loss: {stop_loss:.6f}")
        logger.debug(f"   Risk distance: {risk_distance:.6f} ({risk_distance / entry_price * 100:.3f}%)")
        logger.debug(f"   min_rr_ratio: {self.min_rr_ratio}")
        logger.debug(f"   Target reward distance: {target_reward_distance:.6f} ({target_reward_distance / entry_price * 100:.3f}%)")
        logger.debug(f"   ATR: {atr:.6f} ({atr / entry_price * 100:.3f}%)")
        logger.debug(f"   ATR multiplier: {self.tp_atr_multiplier}")
        logger.debug(f"   Target by ATR: {atr * self.tp_atr_multiplier:.6f}")

        tp_by_atr = atr * self.tp_atr_multiplier
        if target_reward_distance < tp_by_atr * 0.5:
            logger.warning(f"⚠️ TP слишком близкий по сравнению с ATR")
            target_reward_distance = max(target_reward_distance, tp_by_atr)
            logger.warning(f"   Используем: {target_reward_distance:.6f}")

        if signal_type == "SELL":
            take_profit = entry_price - target_reward_distance

            if take_profit >= entry_price:
                logger.error(f"❌ SELL TP выше entry: {take_profit:.6f} >= {entry_price:.6f}")
                take_profit = entry_price * (1 - self.tp_safety_buffer_pct / 100)
                logger.warning(f"   Исправляем SELL TP: {take_profit:.6f}")

            min_tp = entry_price * self.min_tp_floor_pct
            if take_profit < min_tp:
                logger.warning(f"⚠️ SELL TP слишком низкий: {take_profit:.6f} < {min_tp:.6f}")
                take_profit = min_tp
                logger.warning(f"   Устанавливаем минимальный TP: {take_profit:.6f}")

        else:
            take_profit = entry_price + target_reward_distance

            if take_profit <= entry_price:
                logger.error(f"❌ BUY TP ниже entry: {take_profit:.6f} <= {entry_price:.6f}")
                take_profit = entry_price * (1 + self.tp_safety_buffer_pct / 100)
                logger.warning(f"   Исправляем BUY TP: {take_profit:.6f}")

            max_tp = entry_price * self.max_tp_ceiling_pct
            if take_profit > max_tp:
                logger.warning(f"⚠️ BUY TP слишком высокий: {take_profit:.6f} > {max_tp:.6f}")
                take_profit = max_tp
                logger.warning(f"   Устанавливаем максимальный TP: {take_profit:.6f}")

        risk_management_config = self.config.get('analysis', {}).get('risk_management', {})
        min_tp_distance_pct = risk_management_config.get('min_tp_distance_pct', 1.5)

        tp_distance = abs(take_profit - entry_price)
        tp_distance_pct = (tp_distance / entry_price) * 100

        if tp_distance_pct < min_tp_distance_pct:
            logger.warning(f"⚠️ TP слишком близко к Entry ({tp_distance_pct:.3f}% < {min_tp_distance_pct}%)")
            if signal_type == "SELL":
                take_profit = entry_price * (1 - min_tp_distance_pct / 100)
            else:
                take_profit = entry_price * (1 + min_tp_distance_pct / 100)
            logger.warning(f"   Устанавливаем минимальный TP: {take_profit:.6f} ({min_tp_distance_pct}%)")

            tp_distance = abs(take_profit - entry_price)
            tp_distance_pct = (tp_distance / entry_price) * 100

        final_risk_distance = abs(stop_loss - entry_price)
        final_reward_distance = abs(take_profit - entry_price)

        if final_risk_distance > 0:
            actual_rr_ratio = final_reward_distance / final_risk_distance
        else:
            actual_rr_ratio = 0.0
            logger.error(f"❌ НУЛЕВОЙ РИСК! final_risk_distance={final_risk_distance}")

        if actual_rr_ratio < self.min_rr_ratio * 0.9:
            logger.warning(f"⚠️ Плохой R/R: {actual_rr_ratio:.2f}:1 < {self.min_rr_ratio}:1 (min)")

            if signal_type == "SELL":
                take_profit = entry_price - (final_risk_distance * self.min_rr_ratio)
            else:
                take_profit = entry_price + (final_risk_distance * self.min_rr_ratio)

            logger.warning(f"   Пересчитываем TP для минимального R/R: {take_profit:.6f}")

            final_reward_distance = abs(take_profit - entry_price)
            if final_risk_distance > 0:
                actual_rr_ratio = final_reward_distance / final_risk_distance
            else:
                actual_rr_ratio = 0.0

        logger.info(f"✅ Рассчитан Take Profit: {take_profit:.6f}")
        logger.info(f"   Расстояние: {tp_distance_pct:.3f}%")
        logger.info(f"   R/R: {actual_rr_ratio:.2f}:1 (требуется: {self.min_rr_ratio}:1)")
        logger.info(f"   Риск: {final_risk_distance / entry_price * 100:.2f}%")
        logger.info(f"   Награда: {final_reward_distance / entry_price * 100:.2f}%")

        if signal_type == "SELL" and take_profit > entry_price:
            logger.error(f"❌ ФИНАЛЬНАЯ ОШИБКА: SELL TP {take_profit:.6f} > entry {entry_price:.6f}")
            take_profit = entry_price * 0.99
            logger.warning(f"   Экстренная корректировка SELL TP: {take_profit:.6f}")

        elif signal_type == "BUY" and take_profit < entry_price:
            logger.error(f"❌ ФИНАЛЬНАЯ ОШИБКА: BUY TP {take_profit:.6f} < entry {entry_price:.6f}")
            take_profit = entry_price * 1.01
            logger.warning(f"   Экстренная корректировка BUY TP: {take_profit:.6f}")

        return take_profit

    def _check_signal_quality(self, entry_price: float, stop_loss: float,
                              take_profit: float, signal_type: str,
                              current_price: float) -> Tuple[bool, Dict[str, float]]:
        """Проверка качества сигнала"""

        entry_distance_pct = abs(entry_price - current_price) / current_price * 100
        if entry_distance_pct > self.max_entry_distance_pct:
            logger.warning(
                f"❌ Зона входа слишком далеко: {entry_distance_pct:.1f}% (макс: {self.max_entry_distance_pct}%)")
            return False, {}

        risk = abs(stop_loss - entry_price)
        reward = abs(take_profit - entry_price)

        if risk == 0:
            logger.warning("❌ Риск = 0 - невозможный SL")
            return False, {}

        rr_ratio = reward / risk

        logger.debug(f"🔍 SIGNAL QUALITY CHECK:")
        logger.debug(f"   Entry:          {entry_price:.6f}")
        logger.debug(f"   Stop Loss:      {stop_loss:.6f}")
        logger.debug(f"   Take Profit:    {take_profit:.6f}")
        logger.debug(f"   Signal Type:    {signal_type}")
        logger.debug(f"   Risk:           {risk:.6f}")
        logger.debug(f"   Reward:         {reward:.6f}")
        logger.debug(f"   R/R Ratio:      {rr_ratio:.2f}:1")
        logger.debug(f"   Min R/R:        {self.min_rr_ratio}:1")
        logger.debug(f"   Tolerance:      {self.rr_quality_tolerance}")
        logger.debug(f"   Required R/R:   ≥ {self.min_rr_ratio - self.rr_quality_tolerance:.2f}:1")

        if rr_ratio < self.min_rr_ratio - self.rr_quality_tolerance:
            logger.warning(
                f"❌ Плохой R/R: {rr_ratio:.2f}:1 (нужно {self.min_rr_ratio}:1, с допуском {self.min_rr_ratio - self.rr_quality_tolerance:.2f})")
            return False, {}

        risk_pct = risk / entry_price * 100
        if risk_pct > self.max_risk_pct * 100:
            logger.warning(f"❌ Слишком большой риск: {risk_pct:.1f}% (макс {self.max_risk_pct * 100:.1f}%)")
            return False, {}

        if signal_type == "SELL":
            if stop_loss <= entry_price:
                logger.warning(f"❌ Для SELL Stop Loss должен быть ВЫШЕ цены входа")
                logger.warning(f"   Entry: {entry_price:.6f}, Stop: {stop_loss:.6f}")
                return False, {}
        elif signal_type == "BUY":
            if stop_loss >= entry_price:
                logger.warning(f"❌ Для BUY Stop Loss должен быть НИЖЕ цены входа")
                logger.warning(f"   Entry: {entry_price:.6f}, Stop: {stop_loss:.6f}")
                return False, {}

        metrics = {
            "rr_ratio": rr_ratio,
            "risk_pct": risk_pct,
            "reward_pct": reward / entry_price * 100,
            "entry_distance_pct": entry_distance_pct
        }

        logger.info(
            f"✅ Качество сигнала OK: R/R={rr_ratio:.2f}:1, риск={risk_pct:.1f}%, доход={metrics['reward_pct']:.1f}%")
        return True, metrics

    def _calculate_atr(self, high_prices: List[float], low_prices: List[float],
                       close_prices: List[float], period: int = None,
                       entry_price: float = None) -> float:
        """Расчет Average True Range"""
        if period is None:
            period = self.atr_period

        logger.debug(f"🔍 Расчет ATR({period}) для {len(high_prices)} свечей")

        try:
            if len(high_prices) < period + 1:
                logger.warning(f"⚠️ Недостаточно данных для ATR: {len(high_prices)} < {period + 1}")
                if entry_price is not None:
                    return entry_price * 0.005
                elif close_prices:
                    avg_price = sum(close_prices[-10:]) / min(10, len(close_prices))
                    return avg_price * 0.005
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

            check_price = entry_price if entry_price is not None else close_prices[-1] if close_prices else 0

            if check_price > 0:
                if atr == 0:
                    logger.warning(f"⚠️ ATR = 0, используем 0.1% от цены проверки")
                    atr = check_price * 0.001
                elif atr < check_price * 0.0005:
                    logger.warning(f"⚠️ ATR слишком мал ({atr:.6f}), увеличиваем до 0.1%")
                    atr = check_price * 0.001

                atr_pct = atr / check_price * 100
                logger.info(f"✅ ATR({period}): {atr:.6f} ({atr_pct:.3f}% от {check_price:.6f})")

            return atr

        except Exception as e:
            logger.error(f"❌ Ошибка расчета ATR: {e}")
            return 0.01

    def _calculate_stochastic(self, high_prices: List[float], low_prices: List[float],
                              close_prices: List[float], k_period: int = None,
                              d_period: int = None, slowing: int = None) -> Dict[str, Any]:
        """Расчет стохастического осциллятора"""
        if k_period is None:
            k_period = self.stochastic_periods['k']
        if d_period is None:
            d_period = self.stochastic_periods['d']
        if slowing is None:
            slowing = self.stochastic_periods['slowing']

        logger.debug(f"Расчет Stochastic, период K={k_period}, D={d_period}")

        try:
            if len(high_prices) < k_period + d_period:
                logger.warning(f"Недостаточно данных для расчета Stochastic: {len(high_prices)}")
                return {"k_line": 0, "d_line": 0, "oversold": False, "overbought": False}

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

            result = {
                "k_line": round(current_k, 2),
                "d_line": round(current_d, 2),
                "oversold": oversold,
                "overbought": overbought,
                "k_values": k_values,
                "d_values": d_values
            }

            logger.debug(f"Stochastic рассчитан: K={current_k:.1f}, D={current_d:.1f}, "
                         f"oversold={oversold}, overbought={overbought}")
            return result

        except Exception as e:
            logger.error(f"Ошибка расчета Stochastic: {e}")
            return {"k_line": 50, "d_line": 50, "oversold": False, "overbought": False}

    def _find_chart_patterns_m15(self, m15_klines: List, trend_direction: str) -> List[Dict]:
        """Поиск графических паттернов на M15"""
        logger.debug(f"Поиск паттернов M15, тренд: {trend_direction}")
        patterns = []

        try:
            if len(m15_klines) < 3:
                logger.warning("Недостаточно данных M15 для поиска паттернов")
                return patterns

            pin_bar = self._analyze_pin_bar_m15(m15_klines, trend_direction)
            if pin_bar: patterns.append(pin_bar)

            engulfing = self._analyze_engulfing_m15(m15_klines, trend_direction)
            if engulfing: patterns.append(engulfing)

            morning_evening_star = self._analyze_morning_evening_star_m15(m15_klines, trend_direction)
            if morning_evening_star: patterns.append(morning_evening_star)

            ma_crossover = self._analyze_ma_crossover_m15(m15_klines, trend_direction)
            if ma_crossover: patterns.append(ma_crossover)

            ma_bounce = self._analyze_ma_bounce_m15(m15_klines, trend_direction)
            if ma_bounce: patterns.append(ma_bounce)

            logger.info(f"Найдено паттернов M15: {len(patterns)}")
            return patterns

        except Exception as e:
            logger.error(f"Ошибка поиска паттернов: {e}")
            return patterns

    def _analyze_pin_bar_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        """Анализ Pin Bar паттерна"""
        logger.debug(f"Анализ Pin Bar M15, тренд: {trend_direction}")

        try:
            if len(m15_klines) < 2:
                logger.warning("Недостаточно данных M15 для анализа Pin Bar")
                return None

            current_candle = m15_klines[-1]
            open_price = float(current_candle[1])
            high_price = float(current_candle[2])
            low_price = float(current_candle[3])
            close_price = float(current_candle[4])

            body_size = abs(close_price - open_price)
            total_range = high_price - low_price

            if total_range == 0:
                logger.debug("Свеча без диапазона")
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

                logger.info(f"✅ Обнаружен бычий Pin Bar: тело={body_ratio:.2%}, "
                            f"нижняя тень={lower_shadow:.2f}, закрытие={close_position:.2%}")
                signal = {
                    "type": PatternType.PIN_BAR.value,
                    "subtype": "BULLISH_HAMMER",
                    "confidence": confidence,
                    "price_level": close_price,
                    "body_ratio": round(body_ratio, 2),
                    "close_position": round(close_position, 2)
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

                logger.info(f"✅ Обнаружен медвежий Pin Bar: тело={body_ratio:.2%}, "
                            f"верхняя тень={upper_shadow:.2f}, закрытие={close_position:.2%}")
                signal = {
                    "type": PatternType.PIN_BAR.value,
                    "subtype": "BEARISH_SHOOTING_STAR",
                    "confidence": confidence,
                    "price_level": close_price,
                    "body_ratio": round(body_ratio, 2),
                    "close_position": round(close_position, 2)
                }

            if signal is None:
                logger.debug("Pin Bar не обнаружен")

            return signal

        except Exception as e:
            logger.error(f"Ошибка анализа Pin Bar: {e}")
            return None

    def _analyze_engulfing_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        """Анализ Engulfing паттерна"""
        logger.debug(f"Анализ Engulfing M15, тренд: {trend_direction}")

        try:
            if len(m15_klines) < 2:
                logger.warning("Недостаточно данных M15 для анализа Engulfing")
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
                logger.debug("Свечи без тела")
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

                        logger.info(f"✅ Обнаружено бычье поглощение: "
                                    f"тело={current_body_size:.2f}, "
                                    f"поглощение={engulfing_ratio:.1f}x")
                        return {
                            "type": PatternType.ENGULFING.value,
                            "subtype": "BULLISH_ENGULFING",
                            "confidence": confidence,
                            "price_level": current_close,
                            "engulfing_ratio": round(engulfing_ratio, 2)
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

                        logger.info(f"✅ Обнаружено медвежье поглощение: "
                                    f"тело={current_body_size:.2f}, "
                                    f"поглощение={engulfing_ratio:.1f}x")
                        return {
                            "type": PatternType.ENGULFING.value,
                            "subtype": "BEARISH_ENGULFING",
                            "confidence": confidence,
                            "price_level": current_close,
                            "engulfing_ratio": round(engulfing_ratio, 2)
                        }

            logger.debug("Engulfing не обнаружен")
            return None

        except Exception as e:
            logger.error(f"Ошибка анализа Engulfing: {e}")
            return None

    def _analyze_morning_evening_star_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        """Анализ Morning/Evening Star паттерна"""
        logger.debug(f"Анализ Morning/Evening Star M15, тренд: {trend_direction}")

        try:
            if len(m15_klines) < 3:
                logger.warning("Недостаточно данных M15 для анализа Morning/Evening Star")
                return None

            candles = m15_klines[-3:]
            opens = [float(c[1]) for c in candles]
            closes = [float(c[4]) for c in candles]
            highs = [float(c[2]) for c in candles]
            lows = [float(c[3]) for c in candles]

            candle_types = []
            for i in range(3):
                if closes[i] > opens[i]:
                    candle_types.append("BULLISH")
                elif closes[i] < opens[i]:
                    candle_types.append("BEARISH")
                else:
                    candle_types.append("DOJI")

            signal = None

            if trend_direction == "BULL":
                is_morning_star = (
                        candle_types[0] == "BEARISH" and
                        candle_types[1] in ["DOJI", "BEARISH"] and
                        candle_types[2] == "BULLISH" and
                        closes[2] > opens[0] and
                        opens[1] < closes[0] and
                        closes[1] < opens[0] and
                        closes[2] > (opens[0] + closes[0]) / 2
                )

                if is_morning_star:
                    body_ratio = abs(closes[2] - opens[2]) / (highs[2] - lows[2]) if (highs[2] - lows[2]) > 0 else 0
                    confidence = min(
                        self.morning_star_confidence_base + (body_ratio * self.morning_star_body_multiplier),
                        self.morning_star_max_confidence
                    )

                    logger.info(f"✅ Обнаружена Утренняя звезда (бычий разворот)")
                    signal = {
                        "type": PatternType.MORNING_STAR.value,
                        "subtype": "MORNING_STAR",
                        "confidence": confidence,
                        "price_level": closes[2],
                        "gap_size": round(closes[0] - opens[1], 4)
                    }

            elif trend_direction == "BEAR":
                is_evening_star = (
                        candle_types[0] == "BULLISH" and
                        candle_types[1] in ["DOJI", "BULLISH"] and
                        candle_types[2] == "BEARISH" and
                        closes[2] < opens[0] and
                        opens[1] > closes[0] and
                        closes[1] > opens[0] and
                        closes[2] < (opens[0] + closes[0]) / 2
                )

                if is_evening_star:
                    body_ratio = abs(closes[2] - opens[2]) / (highs[2] - lows[2]) if (highs[2] - lows[2]) > 0 else 0
                    confidence = min(
                        self.morning_star_confidence_base + (body_ratio * self.morning_star_body_multiplier),
                        self.morning_star_max_confidence
                    )

                    logger.info(f"✅ Обнаружена Вечерняя звезда (медвежий разворот)")
                    signal = {
                        "type": PatternType.EVENING_STAR.value,
                        "subtype": "EVENING_STAR",
                        "confidence": confidence,
                        "price_level": closes[2],
                        "gap_size": round(opens[1] - closes[0], 4)
                    }

            if signal is None:
                logger.debug("Morning/Evening Star не обнаружен")

            return signal

        except Exception as e:
            logger.error(f"Ошибка анализа Morning/Evening Star: {e}")
            return None

    def _analyze_ma_crossover_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        """Анализ MA кроссовера"""
        logger.debug(f"Анализ MA кроссовера M15, тренд: {trend_direction}")

        try:
            if len(m15_klines) < self.ma_crossover_min_candles:
                logger.warning(
                    f"❌ Недостаточно данных M15 для анализа MA кроссовера: {len(m15_klines)} < {self.ma_crossover_min_candles}")
                return None

            recent_klines = m15_klines[-50:] if len(m15_klines) > 50 else m15_klines
            closes = [float(k[4]) for k in recent_klines]

            logger.debug(f"Анализируем {len(closes)} свечей для MA кроссовера")

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
                logger.warning("❌ EMA не рассчитаны для анализа кроссовера")
                return None

            def contains_nan(values):
                for value in values:
                    if value != value:
                        return True
                    try:
                        import math
                        if math.isnan(value):
                            return True
                    except:
                        pass
                return False

            if contains_nan(ema_20[-5:]) or contains_nan(ema_50[-5:]):
                logger.warning("❌ EMA содержат NaN значения")
                return None

            if any(ema == 0 for ema in ema_20[-5:]) or any(ema == 0 for ema in ema_50[-5:]):
                logger.warning("❌ EMA содержат нулевые значения")
                return None

            current_close = closes[-1]
            current_ema20 = ema_20[-1]
            prev_ema20 = ema_20[-2] if len(ema_20) >= 2 else current_ema20
            current_ema50 = ema_50[-1]
            prev_close = closes[-2] if len(closes) >= 2 else current_close

            signal = None

            if trend_direction == "BULL":
                bullish_crossover = (
                        prev_close <= prev_ema20 and
                        current_close > current_ema20 and
                        current_ema20 > prev_ema20 and
                        current_close > current_ema50
                )

                if bullish_crossover:
                    lookback = min(self.ma_crossover_lookback_candles, len(closes) - 1)
                    was_below = True
                    for i in range(2, lookback + 1):
                        if i < len(closes) and i < len(ema_20):
                            if closes[-i] > ema_20[-i]:
                                was_below = False
                                break

                    if was_below:
                        distance_pct = ((current_close - current_ema20) / current_ema20) * 100
                        confidence = 0.7 + min(distance_pct / 10, 0.15)

                        logger.info(f"✅ Обнаружен бычий MA кроссовер: {current_close:.2f} > EMA20={current_ema20:.2f} "
                                    f"(расстояние: {distance_pct:.1f}%)")
                        signal = {
                            "type": PatternType.MA_CROSSOVER.value,
                            "subtype": "BULLISH_CROSSOVER",
                            "confidence": confidence,
                            "price_level": current_close,
                            "ema20": current_ema20,
                            "ema50": current_ema50,
                            "crossover_distance_pct": distance_pct
                        }

            elif trend_direction == "BEAR":
                bearish_crossover = (
                        prev_close >= prev_ema20 and
                        current_close < current_ema20 and
                        current_ema20 < prev_ema20 and
                        current_close < current_ema50
                )

                if bearish_crossover:
                    lookback = min(self.ma_crossover_lookback_candles, len(closes) - 1)
                    was_above = True
                    for i in range(2, lookback + 1):
                        if i < len(closes) and i < len(ema_20):
                            if closes[-i] < ema_20[-i]:
                                was_above = False
                                break

                    if was_above:
                        distance_pct = ((current_ema20 - current_close) / current_close) * 100
                        confidence = 0.7 + min(distance_pct / 10, 0.15)

                        logger.info(
                            f"✅ Обнаружен медвежий MA кроссовер: {current_close:.2f} < EMA20={current_ema20:.2f} "
                            f"(расстояние: {distance_pct:.1f}%)")
                        signal = {
                            "type": PatternType.MA_CROSSOVER.value,
                            "subtype": "BEARISH_CROSSOVER",
                            "confidence": confidence,
                            "price_level": current_close,
                            "ema20": current_ema20,
                            "ema50": current_ema50,
                            "crossover_distance_pct": distance_pct
                        }

            if signal is None:
                logger.debug("MA кроссовер не обнаружен")
                return None

            return signal

        except Exception as e:
            logger.error(f"❌ Ошибка анализа MA кроссовера: {e}")
            import traceback
            logger.debug(f"Трассировка: {traceback.format_exc()}")
            return None

    def _analyze_ma_bounce_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        """Анализ отскока от MA"""
        logger.debug(f"Анализ MA отскока M15, тренд: {trend_direction}")

        try:
            if len(m15_klines) < 10:
                logger.warning("Недостаточно данных M15 для анализа MA отскока")
                return None

            closes = [float(k[4]) for k in m15_klines]
            highs = [float(k[2]) for k in m15_klines]
            lows = [float(k[3]) for k in m15_klines]

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

            if len(ema_20) < 3:
                logger.warning("EMA20 не рассчитана для анализа отскока")
                return None

            current_close = closes[-1]
            current_low = lows[-1]
            current_high = highs[-1]
            current_ema20 = ema_20[-1]

            ema_distance_pct = abs(current_close - current_ema20) / current_ema20 * 100

            if ema_distance_pct > self.ma_bounce_max_distance_pct:
                logger.debug(f"Цена слишком далеко от EMA20: {ema_distance_pct:.2f}%")
                return None

            signal = None

            if trend_direction == "BULL":
                prev_close = closes[-2]
                prev_ema20 = ema_20[-2]
                prev_low = lows[-2]

                was_near_ema = (prev_low <= prev_ema20 * (1 + self.ma_touch_tolerance_pct / 100) and
                                prev_close <= prev_ema20 * (1 + self.ma_bounce_tolerance_upper_pct / 100))

                current_bullish = (current_close > current_ema20 and
                                   current_close > prev_close)

                if was_near_ema and current_bullish:
                    logger.info(f"✅ Обнаружен бычий отскок от EMA20: {current_close:.2f} "
                                f"(расстояние: {ema_distance_pct:.2f}%)")
                    signal = {
                        "type": PatternType.MA_BOUNCE.value,
                        "subtype": "BULLISH_BOUNCE",
                        "confidence": 0.7,
                        "price_level": current_close,
                        "ema20": current_ema20,
                        "touch_distance": ema_distance_pct
                    }

            elif trend_direction == "BEAR":
                prev_close = closes[-2]
                prev_ema20 = ema_20[-2]
                prev_high = highs[-2]

                was_near_ema = (prev_high >= prev_ema20 * (1 - self.ma_touch_tolerance_pct / 100) and
                                prev_close >= prev_ema20 * (1 - self.ma_bounce_tolerance_lower_pct / 100))

                current_bearish = (current_close < current_ema20 and
                                   current_close < prev_close)

                if was_near_ema and current_bearish:
                    logger.info(f"✅ Обнаружен медвежий отскок от EMA20: {current_close:.2f} "
                                f"(расстояние: {ema_distance_pct:.2f}%)")
                    signal = {
                        "type": PatternType.MA_BOUNCE.value,
                        "subtype": "BEARISH_BOUNCE",
                        "confidence": 0.7,
                        "price_level": current_close,
                        "ema20": current_ema20,
                        "touch_distance": ema_distance_pct
                    }

            if signal is None:
                logger.debug("MA отскок не обнаружен")

            return signal

        except Exception as e:
            logger.error(f"Ошибка анализа MA отскока: {e}")
            return None

    def _analyze_rsi_divergence_m15(self, m15_klines: List, trend_direction: str) -> Optional[Dict]:
        """Анализ RSI дивергенции"""
        logger.debug(f"Анализ RSI дивергенции M15, тренд: {trend_direction}")

        try:
            if len(m15_klines) < self.rsi_divergence_min_candles:
                logger.warning(
                    f"Недостаточно данных M15 для анализа RSI дивергенции: {len(m15_klines)} < {self.rsi_divergence_min_candles}")
                return None

            closes = [float(k[4]) for k in m15_klines]
            rsi = self._calculate_rsi(closes, self.rsi_period)

            if len(rsi) < 10:
                logger.warning("RSI не рассчитан для анализа дивергенции")
                return None

            recent_closes = closes[-10:]
            recent_rsi = rsi[-10:]

            price_highs = []
            price_lows = []
            rsi_highs = []
            rsi_lows = []

            for i in range(1, len(recent_closes) - 1):
                if recent_closes[i] > recent_closes[i - 1] and recent_closes[i] > recent_closes[i + 1]:
                    price_highs.append((i, recent_closes[i]))
                elif recent_closes[i] < recent_closes[i - 1] and recent_closes[i] < recent_closes[i + 1]:
                    price_lows.append((i, recent_closes[i]))

            for i in range(1, len(recent_rsi) - 1):
                if recent_rsi[i] > recent_rsi[i - 1] and recent_rsi[i] > recent_rsi[i + 1]:
                    rsi_highs.append((i, recent_rsi[i]))
                elif recent_rsi[i] < recent_rsi[i - 1] and recent_rsi[i] < recent_rsi[i + 1]:
                    rsi_lows.append((i, recent_rsi[i]))

            if (len(price_lows) >= 2 and len(rsi_lows) >= 2 and trend_direction == "BULL"):
                last_price_low = price_lows[-1][1]
                prev_price_low = price_lows[-2][1]
                last_rsi_low = rsi_lows[-1][1]
                prev_rsi_low = rsi_lows[-2][1]

                if last_price_low < prev_price_low and last_rsi_low > prev_rsi_low:
                    logger.info(f"✅ Обнаружена бычья RSI дивергенция: "
                                f"цена {last_price_low:.2f} < {prev_price_low:.2f}, "
                                f"RSI {last_rsi_low:.1f} > {prev_rsi_low:.1f}")
                    return {
                        "type": PatternType.BULLISH_DIVERGENCE.value,
                        "confidence": 0.8,
                        "price_level": recent_closes[-1]
                    }

            elif (len(price_highs) >= 2 and len(rsi_highs) >= 2 and trend_direction == "BEAR"):
                last_price_high = price_highs[-1][1]
                prev_price_high = price_highs[-2][1]
                last_rsi_high = rsi_highs[-1][1]
                prev_rsi_high = rsi_highs[-2][1]

                if last_price_high > prev_price_high and last_rsi_high < prev_rsi_high:
                    logger.info(f"✅ Обнаружена медвежья RSI дивергенция: "
                                f"цена {last_price_high:.2f} > {prev_price_high:.2f}, "
                                f"RSI {last_rsi_high:.1f} < {prev_rsi_high:.1f}")
                    return {
                        "type": PatternType.BEARISH_DIVERGENCE.value,
                        "confidence": 0.8,
                        "price_level": recent_closes[-1]
                    }

            logger.debug("RSI дивергенция не обнаружена")
            return None

        except Exception as e:
            logger.error(f"Ошибка анализа дивергенции RSI: {e}")
            return None

    def _calculate_rsi(self, prices: List[float], period: int = None) -> List[float]:
        """Расчет RSI"""
        if period is None:
            period = self.rsi_period

        logger.debug(f"Расчет RSI({period})")

        try:
            if len(prices) < period + 1:
                logger.warning(f"Недостаточно данных для RSI({period}): {len(prices)}")
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

            last_rsi = rsi[-1] if len(rsi) > 0 else 50
            logger.debug(f"RSI рассчитан, последнее значение: {last_rsi:.1f}")

            return rsi.tolist()

        except Exception as e:
            logger.error(f"Ошибка расчета RSI: {e}")
            return []

    def _generate_trading_signal(self, patterns: List[Dict], rsi_divergence: Optional[Dict],
                                 stochastic_data: Dict[str, Any],
                                 screen1: Any, screen2: Any, m15_klines: List) -> Optional[Dict]:
        """Генерация торгового сигнала"""
        logger.debug("Генерация торгового сигнала")

        try:
            if not m15_klines:
                logger.warning("Нет данных M15 для генерации сигнала")
                return None

            current_close = float(m15_klines[-1][4])
            current_price = current_close

            has_trigger = bool(patterns or rsi_divergence)

            stochastic_signal = False
            if stochastic_data:
                if screen1.trend_direction == "BULL":
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

            should_generate_signal = (
                    screen1.passed and
                    screen2.passed and
                    screen1.confidence_score > self.screen1_min_confidence and
                    screen2.confidence > self.screen2_min_confidence and
                    (has_trigger or stochastic_signal)
            )

            if not should_generate_signal:
                logger.warning("Условия для генерации сигнала не выполнены")
                return None

            signal_type = "BUY" if screen1.trend_direction == "BULL" else "SELL"

            if screen2.best_zone:
                entry_price = screen2.best_zone
                logger.info(f"Используем лучшую зону входа: {entry_price:.2f}")
            else:
                entry_price = current_close
                logger.warning(f"Нет лучшей зоны, используем текущую цену: {entry_price:.2f}")

            if not self._validate_price_range(entry_price, "BTCUSDT"):
                logger.warning("❌ Цена входа нереалистична")
                return None

            highs = [float(k[2]) for k in m15_klines]
            lows = [float(k[3]) for k in m15_klines]
            closes = [float(k[4]) for k in m15_klines]

            current_price = closes[-1] if closes else 0
            atr = self._calculate_atr(highs, lows, closes, self.atr_period, current_price)

            stop_loss = self._calculate_stop_loss(
                entry_price=entry_price,
                signal_type=signal_type,
                atr=atr,
                resistance_level=screen1.key_levels.get("resistance") if screen1.key_levels else None,
                support_level=screen1.key_levels.get("support") if screen1.key_levels else None
            )

            take_profit = self._calculate_take_profit(
                entry_price=entry_price,
                stop_loss=stop_loss,
                signal_type=signal_type,
                atr=atr
            )

            quality_ok, quality_metrics = self._check_signal_quality(
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                signal_type=signal_type,
                current_price=current_price
            )

            if not quality_ok:
                logger.warning("❌ Сигнал не прошел проверку качества")
                return None

            base_confidence = (screen1.confidence_score + screen2.confidence) / 2

            if patterns:
                best_pattern = max(patterns, key=lambda x: x.get("confidence", 0))
                pattern_confidence = best_pattern.get("confidence", 0)
                base_confidence = (base_confidence + pattern_confidence) / 2

            if rsi_divergence:
                base_confidence = min(base_confidence + self.divergence_confidence_bonus, self.max_confidence)

            if stochastic_signal:
                base_confidence = min(base_confidence + self.stochastic_confidence_bonus, self.max_confidence)

            if quality_metrics["rr_ratio"] >= self.min_rr_ratio * self.rr_quality_bonus_threshold:
                base_confidence = min(base_confidence + self.pattern_confidence_bonus, self.max_confidence)
                logger.info(f"✅ Бонус за отличный R/R ≥ {self.min_rr_ratio * self.rr_quality_bonus_threshold:.1f}:1")

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

            logger.info(f"🔧 Сгенерирован сигнал {signal_type} {entry_price:.2f} → "
                        f"SL: {stop_loss:.2f}, TP: {take_profit:.2f}, "
                        f"R/R: {quality_metrics['rr_ratio']:.2f}:1")

            signal_data = {
                "signal_type": signal_type,
                "entry_price": round(entry_price, 2),
                "stop_loss": round(stop_loss, 2),
                "take_profit": round(take_profit, 2),
                "strength": signal_strength,
                "pattern": trigger_pattern,
                "confidence": base_confidence,
                "stochastic": stochastic_data,
                "has_rsi_divergence": bool(rsi_divergence),
                "has_pattern": bool(patterns),
                "atr": round(atr, 4),
                "risk_reward_ratio": quality_metrics["rr_ratio"],
                "risk_pct": quality_metrics["risk_pct"],
                "quality_metrics": quality_metrics
            }

            return signal_data

        except Exception as e:
            logger.error(f"❌ Ошибка генерации сигнала: {e}")
            return None


# Экспорт для импорта в другие модули
__all__ = ['Screen3Result', 'Screen3SignalGenerator', 'PatternType']