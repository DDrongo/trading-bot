# analyzer/core/analyst/liquidity_scanner.py (НОВЫЙ - ПОЛНОСТЬЮ)
"""
💧 LIQUIDITY SCANNER - Сканер бассейнов ликвидности

Обнаруживает места скопления стоп-лоссов:
- Локальные минимумы (стопы лонгистов)
- Локальные максимумы (стопы шортистов)
- Equal highs/lows (двойные вершины/основания)
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict

logger = logging.getLogger('liquidity_scanner')


@dataclass
class LiquidityPool:
    """Бассейн ликвидности"""
    type: str  # 'BUY_SIDE' (ликвидность выше цены) или 'SELL_SIDE' (ликвидность ниже цены)
    price: float
    touches: int
    strength: str  # 'STRONG' (3+ касаний), 'NORMAL' (2 касания)
    formed_at: Optional[int] = None  # индекс свечи формирования

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class LiquidityScanner:
    """
    Сканер бассейнов ликвидности на H4 таймфрейме

    Логика:
    1. Находит локальные минимумы (Swing Lows) и максимумы (Swing Highs)
    2. Группирует экстремумы на одном ценовом уровне (погрешность 0.3%)
    3. Если на уровне 2+ касаний — это пул ликвидности
    """

    def __init__(self, lookback_candles: int = 100, price_tolerance_pct: float = 0.3, swing_lookback: int = 3):
        """
        Args:
            lookback_candles: количество свечей для анализа
            price_tolerance_pct: погрешность для группировки уровней (0.3% по умолчанию)
            swing_lookback: количество свечей для определения локальных экстремумов
        """
        self.lookback_candles = lookback_candles
        self.price_tolerance_pct = price_tolerance_pct / 100
        self.swing_lookback = swing_lookback

        logger.info(f"✅ LiquidityScanner инициализирован")
        logger.info(f"   Lookback: {lookback_candles} свечей")
        logger.info(f"   Tolerance: {price_tolerance_pct}%")
        logger.info(f"   Swing lookback: {swing_lookback}")

    def find_liquidity_pools(self, klines_h4: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Находит все пулы ликвидности в H4 данных

        Args:
            klines_h4: список H4 свечей

        Returns:
            Список пулов ликвидности с полями:
                - type: 'BUY_SIDE' или 'SELL_SIDE'
                - price: уровень
                - touches: количество касаний
                - strength: 'STRONG' или 'NORMAL'
        """
        if not klines_h4 or len(klines_h4) < self.swing_lookback * 2 + 1:
            logger.warning("⚠️ Недостаточно данных для поиска ликвидности")
            return []

        # Берём только последние N свечей
        candles = klines_h4[-self.lookback_candles:] if len(klines_h4) > self.lookback_candles else klines_h4

        # Находим локальные экстремумы
        swing_highs = self._find_swing_highs(candles)
        swing_lows = self._find_swing_lows(candles)

        logger.debug(f"📊 Найдено локальных максимумов: {len(swing_highs)}")
        logger.debug(f"📊 Найдено локальных минимумов: {len(swing_lows)}")

        # Группируем уровни
        grouped_highs = self._group_levels(swing_highs, 'BUY_SIDE')
        grouped_lows = self._group_levels(swing_lows, 'SELL_SIDE')

        # Объединяем результаты
        all_pools = grouped_highs + grouped_lows

        # Фильтруем только пулы с 2+ касаниями
        valid_pools = [p for p in all_pools if p['touches'] >= 2]

        # Рассчитываем силу
        for pool in valid_pools:
            pool['strength'] = 'STRONG' if pool['touches'] >= 3 else 'NORMAL'

        if valid_pools:
            logger.info(f"💧 Найдено пулов ликвидности: {len(valid_pools)}")
            for p in valid_pools[:3]:
                logger.debug(f"   {p['type']}: {p['price']:.4f} ({p['touches']} касаний)")

        return valid_pools

    def _find_swing_highs(self, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Находит локальные максимумы (Swing Highs)"""
        swing_highs = []

        for i in range(self.swing_lookback, len(candles) - self.swing_lookback):
            current_high = candles[i]['high']
            left_highs = [candles[j]['high'] for j in range(i - self.swing_lookback, i)]
            right_highs = [candles[j]['high'] for j in range(i + 1, i + self.swing_lookback + 1)]

            if current_high >= max(left_highs) and current_high >= max(right_highs):
                swing_highs.append({
                    'price': current_high,
                    'index': i,
                    'timestamp': candles[i].get('timestamp')
                })

        return swing_highs

    def _find_swing_lows(self, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Находит локальные минимумы (Swing Lows)"""
        swing_lows = []

        for i in range(self.swing_lookback, len(candles) - self.swing_lookback):
            current_low = candles[i]['low']
            left_lows = [candles[j]['low'] for j in range(i - self.swing_lookback, i)]
            right_lows = [candles[j]['low'] for j in range(i + 1, i + self.swing_lookback + 1)]

            if current_low <= min(left_lows) and current_low <= min(right_lows):
                swing_lows.append({
                    'price': current_low,
                    'index': i,
                    'timestamp': candles[i].get('timestamp')
                })

        return swing_lows

    def _group_levels(self, levels: List[Dict[str, Any]], pool_type: str) -> List[Dict[str, Any]]:
        """Группирует близкие ценовые уровни"""
        if not levels:
            return []

        # Сортируем по цене
        sorted_levels = sorted(levels, key=lambda x: x['price'])

        groups = []
        current_group = [sorted_levels[0]]

        for level in sorted_levels[1:]:
            last_price = current_group[-1]['price']
            diff_pct = abs(level['price'] - last_price) / last_price

            if diff_pct < self.price_tolerance_pct:
                current_group.append(level)
            else:
                # Сохраняем группу
                avg_price = sum(l['price'] for l in current_group) / len(current_group)
                groups.append({
                    'type': pool_type,
                    'price': avg_price,
                    'touches': len(current_group),
                    'formed_at': min(l.get('index', 0) for l in current_group)
                })
                current_group = [level]

        # Последняя группа
        if current_group:
            avg_price = sum(l['price'] for l in current_group) / len(current_group)
            groups.append({
                'type': pool_type,
                'price': avg_price,
                'touches': len(current_group),
                'formed_at': min(l.get('index', 0) for l in current_group)
            })

        return groups

    def find_nearest_liquidity_pool(
        self,
        pools: List[Dict[str, Any]],
        current_price: float,
        direction: str = 'SELL_SIDE'
    ) -> Optional[Dict[str, Any]]:
        """
        Находит ближайший пул ликвидности к текущей цене

        Args:
            pools: список пулов ликвидности
            current_price: текущая цена
            direction: 'BUY_SIDE' (выше цены) или 'SELL_SIDE' (ниже цены)

        Returns:
            Ближайший пул или None
        """
        if not pools:
            return None

        filtered = [p for p in pools if p['type'] == direction]

        if not filtered:
            return None

        if direction == 'SELL_SIDE':
            # Ищем пул ниже цены (стопы лонгистов)
            below = [p for p in filtered if p['price'] < current_price]
            if below:
                return max(below, key=lambda x: x['price'])
            return None
        else:
            # Ищем пул выше цены (стопы шортистов)
            above = [p for p in filtered if p['price'] > current_price]
            if above:
                return min(above, key=lambda x: x['price'])
            return None

    def is_liquidity_grab(
        self,
        klines_h4: List[Dict[str, Any]],
        liquidity_pool: Dict[str, Any],
        current_price: float,
        lookback_candles: int = 5
    ) -> Tuple[bool, Optional[float]]:
        """
        Проверяет, было ли снятие ликвидности (Liquidity Grab)

        Liquidity Grab происходит когда:
        1. Цена пробила уровень ликвидности (ниже минимума для SELL_SIDE или выше максимума для BUY_SIDE)
        2. И вернулась обратно

        Args:
            klines_h4: H4 свечи
            liquidity_pool: пул ликвидности
            current_price: текущая цена
            lookback_candles: сколько свечей анализировать

        Returns:
            (был ли греб, цена прокола)
        """
        if not klines_h4 or not liquidity_pool:
            return False, None

        pool_price = liquidity_pool['price']
        pool_type = liquidity_pool['type']

        # Берём последние N свечей
        recent_candles = klines_h4[-lookback_candles:]

        if pool_type == 'SELL_SIDE':
            # Проверяем прокол ниже пула (стопы лонгистов)
            for candle in recent_candles:
                if candle['low'] < pool_price:
                    # Был прокол, проверяем возврат
                    if current_price > pool_price:
                        logger.debug(f"💧 Обнаружен Liquidity Grab: прокол {pool_price:.4f} → возврат {current_price:.4f}")
                        return True, candle['low']
        else:  # BUY_SIDE
            # Проверяем прокол выше пула (стопы шортистов)
            for candle in recent_candles:
                if candle['high'] > pool_price:
                    # Был прокол, проверяем возврат
                    if current_price < pool_price:
                        logger.debug(f"💧 Обнаружен Liquidity Grab: прокол {pool_price:.4f} → возврат {current_price:.4f}")
                        return True, candle['high']

        return False, None