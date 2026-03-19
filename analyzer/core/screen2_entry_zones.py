# core/screen2_entry_zones.py
"""
🎯 ЭКРАН 2 - ПОЛНЫЙ ПОИСК ЗОН ВХОДА (H4/H1 таймфреймы)
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

logger = logging.getLogger('screen2_analyzer')


@dataclass
class Screen2Result:
    """Результат анализа 2-го экрана (зоны входа)"""
    entry_zones: List[Dict] = field(default_factory=list)
    best_zone: Optional[float] = None
    invalidated_zones: List[float] = field(default_factory=list)
    fib_levels: Dict[str, float] = field(default_factory=dict)
    volume_confirmation: bool = False
    passed: bool = False
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_zones": self.entry_zones,
            "best_zone": self.best_zone,
            "invalidated_zones": self.invalidated_zones,
            "fib_levels": self.fib_levels,
            "volume_confirmation": self.volume_confirmation,
            "passed": self.passed,
            "confidence": self.confidence
        }


class Screen2EntryZonesAnalyzer:
    """
    Анализатор для второго экрана - поиск зон входа
    """

    def __init__(self, config=None):
        self.config = config or {}

        # Получаем параметры анализа из конфига
        analysis_config = self.config.get('analysis', {})
        patterns_config = analysis_config.get('patterns', {})
        zones_config = analysis_config.get('zones_config', {})
        thresholds_config = analysis_config.get('thresholds', {})
        confirmation_config = analysis_config.get('confirmation', {})

        # Параметры Фибоначчи
        self.fibonacci_levels = patterns_config.get('fibonacci_levels', [0.236, 0.382, 0.5, 0.618, 0.786])
        self.key_fib_levels = zones_config.get('key_fibonacci_levels', ["0.382", "0.500", "0.618"])

        # Параметры зон входа
        self.default_zone_width = zones_config.get('default_zone_width_pct', 0.5)
        self.zone_match_tolerance = zones_config.get('zone_match_tolerance_pct', 1.0)
        self.level_break_tolerance = zones_config.get('level_break_tolerance_pct', 2.0)

        # Пороговые значения
        self.min_h4_candles = thresholds_config.get('min_h4_candles_for_analysis', 20)
        self.volume_spike_threshold = confirmation_config.get('volume_spike_threshold', 1.5)

        # Параметры уверенности
        self.base_confidence = zones_config.get('base_confidence', 0.6)
        self.volume_confirmation_bonus = zones_config.get('volume_confirmation_bonus', 0.2)
        self.strong_zones_bonus = zones_config.get('strong_zones_bonus', 0.15)
        self.strong_with_volume_bonus = zones_config.get('strong_with_volume_bonus', 0.1)
        self.max_confidence = analysis_config.get('signal_generation', {}).get('max_confidence', 0.95)

        # Минимальные требования для анализа
        self.min_data_points = thresholds_config.get('min_data_points_for_zones', 10)

        logger.info(f"✅ Screen2EntryZonesAnalyzer инициализирован с параметрами из конфига")
        logger.info(f"   Уровни Фибоначчи: {self.fibonacci_levels}")
        logger.info(f"   Ширина зоны по умолчанию: {self.default_zone_width}%")
        logger.info(f"   Порог спайка объема: {self.volume_spike_threshold}x")

    def analyze_entry_zones(self, symbol: str, h4_klines: List, h1_klines: List,
                            trend_direction: str) -> Screen2Result:
        """Основной метод анализа зон входа"""
        logger.info(f"🎯 {symbol} - Поиск зон входа H4/H1")
        result = Screen2Result()

        try:
            if not h4_klines or len(h4_klines) < self.min_h4_candles:
                logger.warning(
                    f"❌ Недостаточно H4 данных для {symbol}: {len(h4_klines) if h4_klines else 0} < {self.min_h4_candles}")
                return result

            h4_highs = [float(k[2]) for k in h4_klines]
            h4_lows = [float(k[3]) for k in h4_klines]
            h4_closes = [float(k[4]) for k in h4_klines]
            h4_volumes = [float(k[5]) for k in h4_klines]
            current_h4_close = h4_closes[-1] if h4_closes else 0

            logger.info(f"Анализ H4 данных: {len(h4_klines)} свечей, текущая цена: {current_h4_close:.2f}")

            # Поиск уровней поддержки/сопротивления
            support_levels = self._find_support_levels_h4(h4_lows)
            resistance_levels = self._find_resistance_levels_h4(h4_highs)

            # Расчет уровней Фибоначчи
            fib_levels = self._calculate_fibonacci_levels_h4(h4_highs, h4_lows, trend_direction)

            # Поиск зон входа
            entry_zones = self._find_fibonacci_entry_zones(
                fib_levels, support_levels, resistance_levels, trend_direction
            )

            # Анализ объема в зонах (FIXED: теперь анализируем объём в зонах, а не только последнюю свечу)
            volume_confirmation = self._analyze_volume_at_zones_h1(h1_klines, entry_zones, current_h4_close)

            # Выбор лучшей зоны
            best_zone = self._select_best_zone(entry_zones)

            # Расчет уверенности
            confidence = self._calculate_confidence(entry_zones, volume_confirmation)

            # Обновление пробитых зон
            invalidated_zones = self._update_invalidated_zones(
                current_h4_close, support_levels, resistance_levels, trend_direction
            )

            # Формируем результат
            result.entry_zones = entry_zones
            result.best_zone = best_zone
            result.invalidated_zones = [round(z, 2) for z in invalidated_zones]
            result.fib_levels = {k: round(v, 2) for k, v in fib_levels.items()}
            result.volume_confirmation = volume_confirmation
            result.passed = bool(best_zone and len(entry_zones) > 0)
            result.confidence = confidence

            status = "✅" if result.passed else "❌"
            logger.info(f"{status} {symbol} ЭКРАН 2: {len(entry_zones)} зон, "
                        f"лучшая: {best_zone}, уверенность: {confidence:.1%}")

            return result

        except Exception as e:
            logger.error(f"❌ Ошибка анализа зон входа для {symbol}: {str(e)}")
            return result

    def _find_support_levels_h4(self, low_prices: List[float]) -> List[float]:
        """Поиск уровней поддержки H4"""
        logger.debug("Поиск уровней поддержки H4")

        try:
            if len(low_prices) < self.min_data_points:
                logger.warning(
                    f"Недостаточно данных для поиска уровней поддержки H4: {len(low_prices)} < {self.min_data_points}")
                return []

            support_levels = []
            for i in range(2, len(low_prices) - 2):
                if (low_prices[i] < low_prices[i - 1] and
                        low_prices[i] < low_prices[i - 2] and
                        low_prices[i] < low_prices[i + 1] and
                        low_prices[i] < low_prices[i + 2]):
                    support_levels.append(low_prices[i])

            result = sorted(set(support_levels))[:3]
            logger.info(f"Найдено уровней поддержки H4: {len(result)}")
            return result

        except Exception as e:
            logger.error(f"Ошибка поиска поддержки H4: {e}")
            return []

    def _find_resistance_levels_h4(self, high_prices: List[float]) -> List[float]:
        """Поиск уровней сопротивления H4"""
        logger.debug("Поиск уровней сопротивления H4")

        try:
            if len(high_prices) < self.min_data_points:
                logger.warning(
                    f"Недостаточно данных для поиска уровней сопротивления H4: {len(high_prices)} < {self.min_data_points}")
                return []

            resistance_levels = []
            for i in range(2, len(high_prices) - 2):
                if (high_prices[i] > high_prices[i - 1] and
                        high_prices[i] > high_prices[i - 2] and
                        high_prices[i] > high_prices[i + 1] and
                        high_prices[i] > high_prices[i + 2]):
                    resistance_levels.append(high_prices[i])

            result = sorted(set(resistance_levels))[-3:]
            logger.info(f"Найдено уровней сопротивления H4: {len(result)}")
            return result

        except Exception as e:
            logger.error(f"Ошибка поиска сопротивления H4: {e}")
            return []

    def _calculate_fibonacci_levels_h4(self, high_prices: List[float], low_prices: List[float],
                                       trend_direction: str) -> Dict[str, float]:
        """Расчет уровней Фибоначчи H4"""
        logger.debug(f"Расчет уровней Фибоначчи H4, тренд: {trend_direction}")

        try:
            if len(high_prices) < 20 or len(low_prices) < 20:
                logger.warning("Недостаточно данных для расчета Фибоначчи H4")
                return {}

            recent_high = max(high_prices[-20:])
            recent_low = min(low_prices[-20:])

            if trend_direction == "BULL":
                # Для бычьего тренда ищем коррекцию к уровням Фибо
                swing_low = min(low_prices[-40:-20]) if len(low_prices) > 40 else recent_low
                swing_high = recent_high
                diff = swing_high - swing_low

                fib_levels = {
                    "0.236": swing_high - diff * 0.236,
                    "0.382": swing_high - diff * 0.382,
                    "0.500": swing_high - diff * 0.500,
                    "0.618": swing_high - diff * 0.618,
                    "0.786": swing_high - diff * 0.786
                }
                logger.info(f"Фибоначчи для бычьего тренда: Swing Low={swing_low:.2f}, Swing High={swing_high:.2f}")

            else:  # BEAR
                # Для медвежьего тренда ищем отскок к уровням Фибо
                swing_high = max(high_prices[-40:-20]) if len(high_prices) > 40 else recent_high
                swing_low = recent_low
                diff = swing_high - swing_low

                fib_levels = {
                    "0.236": swing_low + diff * 0.236,
                    "0.382": swing_low + diff * 0.382,
                    "0.500": swing_low + diff * 0.500,
                    "0.618": swing_low + diff * 0.618,
                    "0.786": swing_low + diff * 0.786
                }
                logger.info(f"Фибоначчи для медвежьего тренда: Swing High={swing_high:.2f}, Swing Low={swing_low:.2f}")

            logger.info(f"Уровни Фибоначчи рассчитаны")
            return fib_levels

        except Exception as e:
            logger.error(f"Ошибка расчета Фибоначчи H4: {e}")
            return {}

    def _find_fibonacci_entry_zones(self, fib_levels: Dict[str, float],
                                    support_levels: List[float], resistance_levels: List[float],
                                    trend_direction: str) -> List[Dict]:
        """Поиск зон входа на основе Фибоначчи"""
        logger.debug(f"Поиск зон входа Фибо, тренд: {trend_direction}")
        entry_zones = []

        try:
            if not fib_levels:
                logger.warning("Нет уровней Фибоначчи для поиска зон")
                return entry_zones

            for fib_key in self.key_fib_levels:
                if fib_key in fib_levels:
                    fib_price = fib_levels[fib_key]

                    zone_strength = "WEAK"
                    volume_confirmation = False

                    if trend_direction == "BULL":
                        # Для бычьего тренда ищем зоны входа у уровни Фибо (откат в лонг)
                        for support in support_levels:
                            tolerance = self.zone_match_tolerance / 100  # Конвертируем проценты в десятичные
                            if abs(fib_price - support) / support < tolerance:
                                zone_strength = "STRONG"
                                volume_confirmation = True
                                logger.info(f"✅ Сильная зона: Фибо {fib_key} ({fib_price:.2f}) "
                                            f"совпадает с поддержкой {support:.2f} (допуск: {tolerance * 100:.1f}%)")
                                break

                    else:  # BEAR
                        # Для медвежьего тренда ищем зоны входа у уровни Фибо (отскок в шорт)
                        for resistance in resistance_levels:
                            tolerance = self.zone_match_tolerance / 100  # Конвертируем проценты в десятичные
                            if abs(fib_price - resistance) / resistance < tolerance:
                                zone_strength = "STRONG"
                                volume_confirmation = True
                                logger.info(f"✅ Сильная зона: Фибо {fib_key} ({fib_price:.2f}) "
                                            f"совпадает с сопротивлением {resistance:.2f} (допуск: {tolerance * 100:.1f}%)")
                                break

                    zone = {
                        "type": "FIBONACCI",
                        "price_level": round(fib_price, 2),
                        "strength": zone_strength,
                        "zone_width": self.default_zone_width,
                        "fibonacci_level": float(fib_key),
                        "volume_confirmation": volume_confirmation
                    }

                    entry_zones.append(zone)

            logger.info(f"Найдено зон входа по Фибо: {len(entry_zones)}")
            return sorted(entry_zones, key=lambda x: x["strength"], reverse=True)

        except Exception as e:
            logger.error(f"Ошибка поиска фибо-зон: {e}")
            return []

    def _analyze_volume_at_zones_h1(self, h1_klines: List, entry_zones: List[Dict], current_price: float) -> bool:
        """Анализ объема в зонах входа H1 (FIXED: анализируем объём именно в зонах)"""
        logger.debug("Анализ объема в зонах входа H1")

        try:
            if not h1_klines or len(h1_klines) < 20:
                logger.warning("Недостаточно данных H1 для анализа объема в зонах")
                return False

            if not entry_zones:
                logger.warning("Нет зон входа для анализа объема")
                return False

            # Подготовка данных H1
            h1_closes = [float(k[4]) for k in h1_klines]
            h1_volumes = [float(k[5]) for k in h1_klines]
            h1_highs = [float(k[2]) for k in h1_klines]
            h1_lows = [float(k[3]) for k in h1_klines]

            if not h1_volumes or all(v == 0 for v in h1_volumes):
                logger.warning("Нет данных объема H1 или все объемы равны 0")
                return False

            # Фильтруем ненулевые объемы для расчета среднего
            non_zero_volumes = [v for v in h1_volumes if v > 0]
            if not non_zero_volumes:
                logger.warning("Все объемы H1 равны 0")
                return False

            avg_volume = sum(non_zero_volumes) / len(non_zero_volumes)

            # Проверяем каждую зону на подтверждение объемом
            zones_with_volume_confirmation = 0

            for zone in entry_zones:
                zone_price = zone["price_level"]
                zone_strength = zone["strength"]

                # Ищем свечи H1, которые находились в этой зоне
                candles_in_zone = []

                for i in range(len(h1_closes)):
                    close_price = h1_closes[i]
                    high_price = h1_highs[i]
                    low_price = h1_lows[i]

                    # Проверяем, была ли цена в зоне (с учетом ширины зоны)
                    zone_width_pct = zone.get("zone_width", self.default_zone_width)
                    zone_upper = zone_price * (1 + zone_width_pct / 100)
                    zone_lower = zone_price * (1 - zone_width_pct / 100)

                    if zone_lower <= close_price <= zone_upper or \
                            zone_lower <= high_price <= zone_upper or \
                            zone_lower <= low_price <= zone_upper:
                        volume = h1_volumes[i]
                        candles_in_zone.append({
                            "index": i,
                            "close": close_price,
                            "volume": volume,
                            "volume_ratio": volume / avg_volume if avg_volume > 0 else 0
                        })

                # Анализируем объем в зоне
                if candles_in_zone:
                    # Сортируем по объему (самые высокие объемы первыми)
                    candles_in_zone.sort(key=lambda x: x["volume"], reverse=True)

                    # Проверяем были ли спайки объема (объем > threshold от среднего)
                    volume_spikes = [c for c in candles_in_zone if c["volume_ratio"] > self.volume_spike_threshold]

                    if volume_spikes:
                        zones_with_volume_confirmation += 1
                        logger.info(f"✅ Зона {zone_price:.2f} ({zone_strength}): "
                                    f"{len(volume_spikes)} спайков объема (до {volume_spikes[0]['volume_ratio']:.1f}x)")
                    else:
                        logger.debug(f"Зона {zone_price:.2f}: нет спайков объема, "
                                     f"макс объем {candles_in_zone[0]['volume_ratio']:.1f}x")
                else:
                    logger.debug(f"Зона {zone_price:.2f}: нет свечей в зоне на H1")

            # Если есть сильные зоны (STRONG), требуем подтверждения объемом хотя бы для одной
            strong_zones = [z for z in entry_zones if z["strength"] == "STRONG"]

            if strong_zones:
                # Для сильных зон требуется подтверждение объемом
                result = zones_with_volume_confirmation > 0
                if result:
                    logger.info(f"✅ Подтверждение объема: {zones_with_volume_confirmation}/{len(entry_zones)} зон")
                else:
                    logger.warning(f"❌ Нет подтверждения объема для сильных зон")
                return result
            else:
                # Для слабых зон - объем опциональный бонус
                result = zones_with_volume_confirmation >= len(entry_zones) // 2
                if result:
                    logger.info(
                        f"✅ Частичное подтверждение объема: {zones_with_volume_confirmation}/{len(entry_zones)} зон")
                return result

        except Exception as e:
            logger.error(f"Ошибка анализа объема в зонах: {e}")
            return False

    def _select_best_zone(self, entry_zones: List[Dict]) -> Optional[float]:
        """Выбор лучшей зоны входа"""
        if not entry_zones:
            return None

        # Сначала ищем сильные зоны с подтверждением объема
        strong_zones_with_volume = [z for z in entry_zones
                                    if z["strength"] == "STRONG" and z.get("volume_confirmation", False)]

        if strong_zones_with_volume:
            best_zone = strong_zones_with_volume[0]["price_level"]
            logger.info(f"Лучшая зона (сильная с объемом): {best_zone:.2f}")
            return best_zone

        # Затем сильные зоны без объема
        strong_zones = [z for z in entry_zones if z["strength"] == "STRONG"]
        if strong_zones:
            best_zone = strong_zones[0]["price_level"]
            logger.info(f"Лучшая зона (сильная без объема): {best_zone:.2f}")
            return best_zone
        else:
            # Берем первую зону (самую сильную из слабых)
            best_zone = entry_zones[0]["price_level"]
            logger.info(f"Лучшая зона (первая доступная): {best_zone:.2f}")
            return best_zone

    def _calculate_confidence(self, entry_zones: List[Dict], volume_confirmation: bool) -> float:
        """Расчет уверенности для экрана 2"""
        confidence = 0.0

        if entry_zones:
            base_confidence = self.base_confidence

            if volume_confirmation:
                base_confidence += self.volume_confirmation_bonus
                logger.info(f"✅ Бонус за подтверждение объемом: +{self.volume_confirmation_bonus:.2f}")

            strong_zones = [z for z in entry_zones if z["strength"] == "STRONG"]
            if strong_zones:
                base_confidence += self.strong_zones_bonus
                logger.info(f"✅ Бонус за {len(strong_zones)} сильных зон: +{self.strong_zones_bonus:.2f}")

                # Дополнительный бонус если у сильных зон есть подтверждение объема
                strong_with_volume = [z for z in strong_zones if z.get("volume_confirmation", False)]
                if strong_with_volume:
                    base_confidence += self.strong_with_volume_bonus
                    logger.info(
                        f"✅ Дополнительный бонус за подтверждение объема в сильных зонах: +{self.strong_with_volume_bonus:.2f}")

            confidence = min(base_confidence, self.max_confidence)

        return confidence

    def _update_invalidated_zones(self, current_price: float,
                                  support_levels: List[float],
                                  resistance_levels: List[float],
                                  trend_direction: str) -> List[float]:
        """Обновление пробитых уровней"""
        logger.debug(f"Обновление пробитых уровней, тренд: {trend_direction}")
        invalidated = []

        tolerance = self.level_break_tolerance / 100  # Конвертируем проценты в десятичные

        if trend_direction == "BULL":
            for level in support_levels:
                if current_price < level * (1 - tolerance):  # Пробитие вниз
                    invalidated.append(level)
                    logger.info(f"❌ Уровень поддержки {level:.2f} пробит вниз (допуск: {tolerance * 100:.1f}%)")

        elif trend_direction == "BEAR":
            for level in resistance_levels:
                if current_price > level * (1 + tolerance):  # Пробитие вверх
                    invalidated.append(level)
                    logger.info(f"❌ Уровень сопротивления {level:.2f} пробит вверх (допуск: {tolerance * 100:.1f}%)")

        logger.info(f"Пробито уровней: {len(invalidated)}")
        return invalidated

    def _analyze_volume_confirmation(self, klines: List, price_action: str) -> bool:
        """Анализ подтверждения объемом (для совместимости)"""
        logger.debug(f"Анализ подтверждения объемом для: {price_action}")

        try:
            if len(klines) < 5:
                logger.warning("Недостаточно данных для анализа объема")
                return False

            volumes = [float(k[5]) for k in klines[-5:]]
            if not volumes:
                logger.warning("Нет данных объема")
                return False

            if all(v == 0 for v in volumes):
                logger.warning("Все объемы равны 0 - пропускаем анализ объема")
                return False

            current_volume = volumes[-1]
            non_zero_volumes = [v for v in volumes[:-1] if v > 0]

            if not non_zero_volumes:
                avg_volume = current_volume
            else:
                avg_volume = sum(non_zero_volumes) / len(non_zero_volumes)

            if current_volume == 0:
                result = False
            else:
                result = current_volume > avg_volume * self.volume_spike_threshold

            if result:
                logger.info(f"✅ Подтверждение объемом: {current_volume:.0f} > {avg_volume:.0f} "
                            f"(+{current_volume / avg_volume * 100 - 100:.0f}%)")
            else:
                logger.debug(
                    f"Нет подтверждения объемом: {current_volume:.0f} <= {avg_volume * self.volume_spike_threshold:.0f}")

            return result

        except Exception as e:
            logger.error(f"Ошибка анализа объема: {e}")
            return False


# Экспорт для импорта в другие модули
__all__ = ['Screen2Result', 'Screen2EntryZonesAnalyzer']