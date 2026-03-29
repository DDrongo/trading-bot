"""
screen2_entry_zones.py - поиск зон входа (H4)
"""
import logging
import numpy as np
from typing import List, Dict, Any


logger = logging.getLogger(__name__)


class Screen2Analyzer:
    """Анализатор зон входа (Экран 2)"""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.min_score = self.config.get('screen2_min_score', 4)

    def analyze(
            self,
            h4_data: List[dict],
            trend_direction: str,
            current_price: float,
            symbol: str
    ) -> Dict[str, Any]:
        """
        Анализирует H4 данные и возвращает зоны входа

        Returns:
            {
                'success': bool,
                'score': int,
                'zone_low': float,
                'zone_high': float,
                'expected_pattern': str,
                'reason': str
            }
        """
        if not h4_data or len(h4_data) < 20:
            return {
                'success': False,
                'score': 0,
                'reason': 'Недостаточно данных H4'
            }

        try:
            # ✅ ПРОВЕРКА ВАЛИДНОСТИ СИМВОЛА (HOTFIX 1.3.6.2)
            if not self._validate_symbol(symbol):
                logger.warning(f"⚠️ {symbol}: символ невалидный, пропускаем")
                return {
                    'success': False,
                    'score': 0,
                    'reason': f'Невалидный символ: {symbol}'
                }

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

            # Определяем ожидаемый паттерн
            expected_pattern = self._determine_expected_pattern(h4_data, best_zone, trend_direction)

            # Проверяем успешность
            success = score >= self.min_score

            if success:
                logger.info(
                    f"✅ {symbol}: ЭКРАН 2 пройден (score={score}/5, зона: {best_zone['low']:.4f}-{best_zone['high']:.4f})")
                return {
                    'success': True,
                    'score': score,
                    'zone_low': best_zone['low'],
                    'zone_high': best_zone['high'],
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

    def _validate_symbol(self, symbol: str) -> bool:
        """
        Валидирует символ

        ✅ HOTFIX 1.3.6.2: возвращает bool, чтобы пропускать невалидные символы

        Проблема: иногда приходит '4USDT' вместо '1000PEPEUSDT' или подобного
        """
        if not symbol:
            return False

        # Список известных некорректных символов
        invalid_symbols = ['4USDT', '0USDT', 'USDTUSDT', 'USDCUSDT', 'BUSDUSDT']

        if symbol in invalid_symbols:
            logger.error(f"❌ Обнаружен невалидный символ: {symbol}")
            return False

        # Проверка на минимальную длину
        if len(symbol) < 5:
            logger.warning(f"⚠️ Подозрительный символ: {symbol} (слишком короткий)")
            return False

        # Проверка, что заканчивается на USDT
        if not symbol.endswith('USDT'):
            logger.warning(f"⚠️ Подозрительный символ: {symbol} (не заканчивается на USDT)")
            return False

        return True

    def _find_support_levels(self, data: List[dict]) -> List[dict]:
        """Находит уровни поддержки"""
        supports = []
        closes = [c['close'] for c in data]
        lows = [c['low'] for c in data]

        # Простая логика: ищем локальные минимумы
        for i in range(5, len(data) - 5):
            if lows[i] < min(lows[i - 5:i]) and lows[i] < min(lows[i + 1:i + 6]):
                supports.append({
                    'price': lows[i],
                    'strength': 'WEAK',
                    'type': 'SUPPORT'
                })

        # Группируем близкие уровни
        supports = self._group_levels(supports)

        return supports[:3]  # топ-3

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
                # Сохраняем группу
                avg_price = sum(l['price'] for l in current_group) / len(current_group)
                strength = 'STRONG' if len(current_group) >= 3 else 'WEAK'
                grouped.append({
                    'price': avg_price,
                    'strength': strength,
                    'type': current_group[0]['type'],
                    'count': len(current_group)
                })
                current_group = [level]

        # Последняя группа
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

        # Ищем последний значительный импульс
        highs = [c['high'] for c in data[-20:]]
        lows = [c['low'] for c in data[-20:]]

        if trend_direction == 'BULL':
            swing_low = min(lows)
            swing_high = max(highs)
            # Для бычьего тренда: ищем уровни отката
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
            # Для медвежьего тренда: ищем уровни коррекции
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

        # Преобразуем в зоны
        zones = []
        for fib in fib_prices:
            zone_width = fib['price'] * 0.005  # 0.5% ширина зоны
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

            # Ищем спайки объёма в зоне
            volume_spikes = 0
            for candle in data[-50:]:  # последние 50 свечей
                if zone_low <= candle['close'] <= zone_high:
                    avg_volume = np.mean([c['volume'] for c in data[-20:]])
                    if candle['volume'] > avg_volume * 1.5:  # спайк
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
            # Проверяем структуру HH/HL
            higher_highs = all(highs[i] < highs[i + 1] for i in range(len(highs) - 1) if i % 3 == 0)
            higher_lows = all(lows[i] < lows[i + 1] for i in range(len(lows) - 1) if i % 3 == 0)
            return higher_highs or higher_lows
        else:
            # Проверяем структуру LH/LL
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
        # Приоритет: сильные зоны Фибоначчи с подтверждением объёма
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
            # Дефолтная зона
            best_zone = {
                'low': 0,
                'high': 0,
                'strength': 'WEAK',
                'type': 'DEFAULT'
            }

        return best_zone

    def _determine_expected_pattern(self, data: List[dict], zone: dict, trend_direction: str) -> str:
        """Определяет ожидаемый паттерн входа"""
        # По умолчанию PIN_BAR
        expected = "PIN_BAR"

        if trend_direction == "BULL":
            # Для бычьего тренда чаще ожидаем пин-бар на поддержке
            expected = "PIN_BAR"
        else:
            # Для медвежьего — поглощение
            expected = "ENGULFING"

        # Если зона сильная, можно ждать более сильный паттерн
        if zone.get('strength') == 'STRONG':
            expected = "MORNING_STAR" if trend_direction == "BULL" else "EVENING_STAR"

        return expected