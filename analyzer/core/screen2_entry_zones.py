"""
screen2_entry_zones.py - поиск зон входа (H4)
ФАЗА 1.3.9:
- Добавлен фильтр ширины диапазона (2-10%) - РАННИЙ ОТСЕВ
- Добавлена проверка стороны зоны (BUY ниже цены, SELL выше цены)
- Добавлен метод _calculate_range_width()
"""
import logging
import numpy as np
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class Screen2Analyzer:
    """Анализатор зон входа (Экран 2)"""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.min_score = self.config.get('screen2_min_score', 4)

        # ФАЗА 1.3.9: Настройки фильтров
        analysis_config = self.config.get('analysis', {})

        # Фильтр ширины диапазона
        range_filter_config = analysis_config.get('range_filter', {})
        self.range_filter_enabled = range_filter_config.get('enabled', True)
        self.range_lookback = range_filter_config.get('lookback', 50)
        self.range_min_width_pct = range_filter_config.get('min_width_pct', 2.0)
        self.range_max_width_pct = range_filter_config.get('max_width_pct', 10.0)

        # Проверка стороны зоны
        zone_side_config = analysis_config.get('zone_side_check', {})
        self.zone_side_check_enabled = zone_side_config.get('enabled', True)

        logger.info(f"✅ Screen2Analyzer: range_filter={self.range_filter_enabled}, "
                    f"zone_side_check={self.zone_side_check_enabled}")

    def analyze(
            self,
            h4_data: List[dict],
            trend_direction: str,
            current_price: float,
            symbol: str
    ) -> Dict[str, Any]:
        """
        Анализирует H4 данные и возвращает зоны входа

        ФАЗА 1.3.9:
        - Фильтр ширины диапазона применяется ПЕРВЫМ (ранний отсев)
        """
        if not h4_data or len(h4_data) < 20:
            return {
                'success': False,
                'score': 0,
                'reason': 'Недостаточно данных H4'
            }

        try:
            # ✅ ПРОВЕРКА ВАЛИДНОСТИ СИМВОЛА
            if not self._validate_symbol(symbol):
                logger.warning(f"⚠️ {symbol}: символ невалидный, пропускаем")
                return {
                    'success': False,
                    'score': 0,
                    'reason': f'Невалидный символ: {symbol}'
                }

            # ========== ФАЗА 1.3.9: ФИЛЬТР ШИРИНЫ ДИАПАЗОНА (РАННИЙ ОТСЕВ) ==========
            if self.range_filter_enabled:
                range_width_pct = self._calculate_range_width(h4_data)

                if range_width_pct is not None:
                    if range_width_pct < self.range_min_width_pct:
                        logger.info(
                            f"❌ {symbol}: Ширина диапазона {range_width_pct:.1f}% < {self.range_min_width_pct}% "
                            f"(слишком узко, шум) — РАННИЙ ОТСЕВ"
                        )
                        return {
                            'success': False,
                            'score': 0,
                            'reason': f'Диапазон слишком узкий: {range_width_pct:.1f}% < {self.range_min_width_pct}%'
                        }

                    if range_width_pct > self.range_max_width_pct:
                        logger.info(
                            f"❌ {symbol}: Ширина диапазона {range_width_pct:.1f}% > {self.range_max_width_pct}% "
                            f"(слишком широко, не флэт) — РАННИЙ ОТСЕВ"
                        )
                        return {
                            'success': False,
                            'score': 0,
                            'reason': f'Диапазон слишком широкий: {range_width_pct:.1f}% > {self.range_max_width_pct}%'
                        }

                    logger.info(f"✅ {symbol}: Ширина диапазона {range_width_pct:.1f}% в норме")

            # 1. Находим уровни поддержки/сопротивления
            support_levels = self._find_support_levels(h4_data)
            resistance_levels = self._find_resistance_levels(h4_data)

            # 2. Рассчитываем уровни Фибоначчи
            fib_levels = self._calculate_fibonacci(h4_data, trend_direction)

            # 3. Анализируем объём в зонах
            volume_zones = self._analyze_volume_zones(h4_data, fib_levels + support_levels + resistance_levels)

            # 4. Оцениваем score
            score = 0
            zones_found = []

            # Критерий 1: Наличие зон входа (макс 1 балл)
            if fib_levels:
                score += 1
                zones_found.extend(fib_levels)

            # Критерий 2: Качество зон (макс 1 балл)
            strong_zones = [z for z in fib_levels if z.get('strength') == 'STRONG']
            if strong_zones:
                score += 1

            # Критерий 3: Подтверждение объёмом (макс 1 балл)
            if volume_zones.get('confirmed', 0) >= 2:
                score += 1

            # Критерий 4: Структура тренда (макс 1 балл)
            if self._check_trend_structure(h4_data, trend_direction):
                score += 1

            # Критерий 5: Отсутствие пробитий (макс 1 балл)
            if not self._check_broken_levels(h4_data, support_levels + resistance_levels):
                score += 1

            # Выбираем лучшую зону
            best_zone = self._select_best_zone(fib_levels, volume_zones, support_levels, resistance_levels)

            # ========== ФАЗА 1.3.9: ПРОВЕРКА СТОРОНЫ ЗОНЫ ==========
            if self.zone_side_check_enabled and best_zone:
                zone_check_passed, zone_check_reason = self._check_zone_side(
                    best_zone, trend_direction, current_price, symbol
                )

                if not zone_check_passed:
                    logger.info(f"❌ {symbol}: {zone_check_reason}")
                    return {
                        'success': False,
                        'score': score,
                        'reason': zone_check_reason
                    }
                else:
                    logger.info(f"✅ {symbol}: {zone_check_reason}")

            # Определяем ожидаемый паттерн
            expected_pattern = self._determine_expected_pattern(h4_data, best_zone, trend_direction)

            # Проверяем успешность
            success = score >= self.min_score

            if success and best_zone:
                logger.info(
                    f"✅ {symbol}: ЭКРАН 2 пройден (score={score}/5, зона: {best_zone.get('low', 0):.4f}-{best_zone.get('high', 0):.4f})")
                return {
                    'success': True,
                    'score': score,
                    'zone_low': best_zone.get('low', 0),
                    'zone_high': best_zone.get('high', 0),
                    'expected_pattern': expected_pattern,
                    'reason': f'Score {score}/5'
                }
            else:
                logger.info(f"❌ {symbol}: ЭКРАН 2 не пройден — score={score}/5, зон={len(zones_found)}")
                return {
                    'success': False,
                    'score': score,
                    'reason': f'Score {score}/5 < {self.min_score}'
                }

        except Exception as e:
            logger.error(f"❌ Ошибка анализа Screen2 для {symbol}: {e}")
            return {
                'success': False,
                'score': 0,
                'reason': str(e)
            }

    def _calculate_range_width(self, h4_data: List[dict]) -> Optional[float]:
        """
        Рассчитывает ширину ценового диапазона на H4
        """
        logger.info(f"DEBUG: h4_data length = {len(h4_data)}")
        logger.info(f"DEBUG: range_lookback = {self.range_lookback}")

        if not h4_data or len(h4_data) < self.range_lookback:
            logger.warning(f"Недостаточно данных для расчёта диапазона: {len(h4_data)}/{self.range_lookback}")
            return None

        # Берём последние N свечей
        recent_data = h4_data[-self.range_lookback:]
        logger.info(f"DEBUG: recent_data length = {len(recent_data)}")

        lows = []
        highs = []

        for c in recent_data:
            low_val = c.get('low', 0)
            high_val = c.get('high', 0)
            if low_val > 0 and high_val > 0:
                lows.append(low_val)
                highs.append(high_val)

        logger.info(f"DEBUG: lows count = {len(lows)}, highs count = {len(highs)}")

        if not lows or not highs:
            logger.warning("Нет валидных low/high значений")
            return None

        range_low = min(lows)
        range_high = max(highs)

        if range_low <= 0:
            return None

        width_pct = (range_high - range_low) / range_low * 100

        logger.info(f"Диапазон: min={range_low:.6f}, max={range_high:.6f}, ширина={width_pct:.2f}%")

        return width_pct

    def _check_zone_side(
            self,
            best_zone: dict,
            trend_direction: str,
            current_price: float,
            symbol: str
    ) -> tuple:
        """
        Проверяет, что зона входа находится с правильной стороны от цены

        ФАЗА 1.3.9.3:
        - BUY (BULL): цена должна быть ВЫШЕ зоны (цена > zone_high)
        - SELL (BEAR): цена должна быть НИЖЕ зоны (цена < zone_low)
        - Если цена внутри зоны — вход НЕ разрешён

        Returns:
            (passed: bool, reason: str)
        """
        if not best_zone:
            return False, "Нет зоны для проверки"

        zone_low = best_zone.get('low', 0)
        zone_high = best_zone.get('high', 0)

        if zone_low <= 0 or zone_high <= 0:
            return False, "Некорректные границы зоны"

        if trend_direction == "BULL":
            # BUY: цена должна быть ВЫШЕ зоны (отскок от поддержки)
            if current_price <= zone_high:
                return False, f"Для BUY цена {current_price:.4f} должна быть выше зоны {zone_low:.4f}-{zone_high:.4f}"
            else:
                return True, f"BUY: цена {current_price:.4f} выше зоны {zone_low:.4f}-{zone_high:.4f}"

        elif trend_direction == "BEAR":
            # SELL: цена должна быть НИЖЕ зоны (отскок от сопротивления)
            if current_price >= zone_low:
                return False, f"Для SELL цена {current_price:.4f} должна быть ниже зоны {zone_low:.4f}-{zone_high:.4f}"
            else:
                return True, f"SELL: цена {current_price:.4f} ниже зоны {zone_low:.4f}-{zone_high:.4f}"

        else:
            # SIDEWAYS - оба направления разрешены
            return True, "SIDEWAYS: оба направления разрешены"

    def _validate_symbol(self, symbol: str) -> bool:
        """Валидирует символ"""
        if not symbol:
            return False

        invalid_symbols = ['4USDT', '0USDT', 'USDTUSDT', 'USDCUSDT', 'BUSDUSDT']

        if symbol in invalid_symbols:
            logger.error(f"❌ Обнаружен невалидный символ: {symbol}")
            return False

        if len(symbol) < 5:
            logger.warning(f"⚠️ Подозрительный символ: {symbol} (слишком короткий)")
            return False

        if not symbol.endswith('USDT'):
            logger.warning(f"⚠️ Подозрительный символ: {symbol} (не заканчивается на USDT)")
            return False

        return True

    def _find_support_levels(self, data: List[dict]) -> List[dict]:
        """Находит уровни поддержки"""
        supports = []
        lows = [c['low'] for c in data]

        for i in range(5, len(data) - 5):
            if lows[i] < min(lows[i - 5:i]) and lows[i] < min(lows[i + 1:i + 6]):
                supports.append({
                    'price': lows[i],
                    'strength': 'WEAK',
                    'type': 'SUPPORT'
                })

        supports = self._group_levels(supports)

        return supports[:3]

    def _find_resistance_levels(self, data: List[dict]) -> List[dict]:
        """Находит уровни сопротивления"""
        resistances = []
        highs = [c['high'] for c in data]

        for i in range(5, len(data) - 5):
            if highs[i] > max(highs[i - 5:i]) and highs[i] > max(highs[i + 1:i + 6]):
                resistances.append({
                    'price': highs[i],
                    'strength': 'WEAK',
                    'type': 'RESISTANCE'
                })

        resistances = self._group_levels(resistances)

        return resistances[:3]

    def _group_levels(self, levels: List[dict], tolerance: float = 0.01) -> List[dict]:
        """Группирует близкие уровни"""
        if not levels:
            return []

        sorted_levels = sorted(levels, key=lambda x: x['price'])
        grouped = []
        current_group = [sorted_levels[0]]

        for level in sorted_levels[1:]:
            if abs(level['price'] - current_group[-1]['price']) / current_group[-1]['price'] < tolerance:
                current_group.append(level)
            else:
                avg_price = sum(l['price'] for l in current_group) / len(current_group)
                strength = 'STRONG' if len(current_group) >= 3 else 'WEAK'
                grouped.append({
                    'price': avg_price,
                    'strength': strength,
                    'type': current_group[0]['type'],
                    'count': len(current_group)
                })
                current_group = [level]

        if current_group:
            avg_price = sum(l['price'] for l in current_group) / len(current_group)
            strength = 'STRONG' if len(current_group) >= 3 else 'WEAK'
            grouped.append({
                'price': avg_price,
                'strength': strength,
                'type': current_group[0]['type'],
                'count': len(current_group)
            })

        return grouped

    def _calculate_fibonacci(self, data: List[dict], trend_direction: str) -> List[dict]:
        """Рассчитывает уровни Фибоначчи"""
        if len(data) < 20:
            return []

        highs = [c['high'] for c in data[-20:]]
        lows = [c['low'] for c in data[-20:]]

        if trend_direction == 'BULL':
            swing_low = min(lows)
            swing_high = max(highs)
            levels = [0.382, 0.5, 0.618]
            fib_prices = []
            for level in levels:
                price = swing_high - (swing_high - swing_low) * level
                fib_prices.append({
                    'price': price,
                    'level': level,
                    'strength': 'STRONG' if level == 0.618 else 'WEAK',
                    'type': 'FIB_RETRACEMENT'
                })
        else:
            swing_high = max(highs)
            swing_low = min(lows)
            levels = [0.382, 0.5, 0.618]
            fib_prices = []
            for level in levels:
                price = swing_low + (swing_high - swing_low) * level
                fib_prices.append({
                    'price': price,
                    'level': level,
                    'strength': 'STRONG' if level == 0.618 else 'WEAK',
                    'type': 'FIB_RETRACEMENT'
                })

        zones = []
        for fib in fib_prices:
            zone_width = fib['price'] * 0.005
            zones.append({
                'low': fib['price'] - zone_width,
                'high': fib['price'] + zone_width,
                'strength': fib['strength'],
                'type': 'FIBONACCI',
                'level': fib['level']
            })

        return zones

    def _analyze_volume_zones(self, data: List[dict], zones: List[dict]) -> Dict[str, Any]:
        """Анализирует объём в зонах"""
        confirmed = 0

        for zone in zones:
            zone_low = zone.get('low', zone.get('price', 0)) * 0.995
            zone_high = zone.get('high', zone.get('price', 0)) * 1.005

            volume_spikes = 0
            for candle in data[-50:]:
                if zone_low <= candle['close'] <= zone_high:
                    avg_volume = np.mean([c['volume'] for c in data[-20:]])
                    if candle['volume'] > avg_volume * 1.5:
                        volume_spikes += 1

            if volume_spikes >= 2:
                confirmed += 1
                zone['volume_confirmed'] = True
            else:
                zone['volume_confirmed'] = False

        return {
            'confirmed': confirmed,
            'total_zones': len(zones)
        }

    def _check_trend_structure(self, data: List[dict], trend_direction: str) -> bool:
        """Проверяет структуру тренда"""
        if len(data) < 20:
            return False

        highs = [c['high'] for c in data[-20:]]
        lows = [c['low'] for c in data[-20:]]

        if trend_direction == 'BULL':
            higher_highs = all(highs[i] < highs[i + 1] for i in range(len(highs) - 1) if i % 3 == 0)
            higher_lows = all(lows[i] < lows[i + 1] for i in range(len(lows) - 1) if i % 3 == 0)
            return higher_highs or higher_lows
        else:
            lower_highs = all(highs[i] > highs[i + 1] for i in range(len(highs) - 1) if i % 3 == 0)
            lower_lows = all(lows[i] > lows[i + 1] for i in range(len(lows) - 1) if i % 3 == 0)
            return lower_highs or lower_lows

    def _check_broken_levels(self, data: List[dict], levels: List[dict]) -> bool:
        """Проверяет, не пробиты ли уровни"""
        if not levels or len(data) < 5:
            return False

        current_price = data[-1]['close']
        broken = 0

        for level in levels:
            level_price = level.get('price', 0)
            if current_price > level_price and level.get('type') == 'RESISTANCE':
                broken += 1
            elif current_price < level_price and level.get('type') == 'SUPPORT':
                broken += 1

        return broken >= 2

    def _select_best_zone(self, fib_zones: List[dict], volume_data: Dict, support_levels: List[dict],
                          resistance_levels: List[dict]) -> dict:
        """Выбирает лучшую зону для входа"""
        best_zone = None

        for zone in fib_zones:
            if zone.get('strength') == 'STRONG' and zone.get('volume_confirmed', False):
                best_zone = zone
                break

        if not best_zone:
            for zone in fib_zones:
                if zone.get('strength') == 'STRONG':
                    best_zone = zone
                    break

        if not best_zone and fib_zones:
            best_zone = fib_zones[0]

        if not best_zone:
            best_zone = {
                'low': 0,
                'high': 0,
                'strength': 'WEAK',
                'type': 'DEFAULT'
            }

        return best_zone

    def _determine_expected_pattern(self, data: List[dict], zone: dict, trend_direction: str) -> str:
        """Определяет ожидаемый паттерн входа"""
        expected = "PIN_BAR"

        if trend_direction == "BULL":
            expected = "PIN_BAR"
        else:
            expected = "ENGULFING"

        if zone.get('strength') == 'STRONG':
            expected = "MORNING_STAR" if trend_direction == "BULL" else "EVENING_STAR"

        return expected