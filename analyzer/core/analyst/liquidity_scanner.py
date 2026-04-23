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
    """

    def __init__(self, lookback_candles: int = 100, price_tolerance_pct: float = 0.3, swing_lookback: int = 3):
        self.lookback_candles = lookback_candles
        self.price_tolerance_pct = price_tolerance_pct / 100
        self.swing_lookback = swing_lookback

        logger.info(f"✅ LiquidityScanner инициализирован")
        logger.info(f"   Lookback: {lookback_candles} свечей")
        logger.info(f"   Tolerance: {price_tolerance_pct}%")
        logger.info(f"   Swing lookback: {swing_lookback}")

    def find_liquidity_pools(self, klines_h4: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Находит все пулы ликвидности в H4 данных"""
        if not klines_h4 or len(klines_h4) < self.swing_lookback * 2 + 1:
            logger.warning("⚠️ Недостаточно данных для поиска ликвидности")
            return []

        candles = klines_h4[-self.lookback_candles:] if len(klines_h4) > self.lookback_candles else klines_h4

        swing_highs = self._find_swing_highs(candles)
        swing_lows = self._find_swing_lows(candles)

        logger.debug(f"📊 Найдено локальных максимумов: {len(swing_highs)}")
        logger.debug(f"📊 Найдено локальных минимумов: {len(swing_lows)}")

        grouped_highs = self._group_levels(swing_highs, 'BUY_SIDE')
        grouped_lows = self._group_levels(swing_lows, 'SELL_SIDE')

        all_pools = grouped_highs + grouped_lows
        valid_pools = [p for p in all_pools if p['touches'] >= 2]

        for pool in valid_pools:
            pool['strength'] = 'STRONG' if pool['touches'] >= 3 else 'NORMAL'

        if valid_pools:
            logger.info(f"💧 Найдено пулов ликвидности: {len(valid_pools)}")
            for p in valid_pools[:3]:
                logger.debug(f"   {p['type']}: {p['price']:.4f} ({p['touches']} касаний)")

        return valid_pools

    def _find_swing_highs(self, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Находит локальные максимумы (Swing Highs) с индексом и timestamp"""
        swing_highs = []

        for i in range(self.swing_lookback, len(candles) - self.swing_lookback):
            current_high = candles[i]['high']
            left_highs = [candles[j]['high'] for j in range(i - self.swing_lookback, i)]
            right_highs = [candles[j]['high'] for j in range(i + 1, i + self.swing_lookback + 1)]

            if current_high >= max(left_highs) and current_high >= max(right_highs):
                swing_highs.append({
                    'price': current_high,
                    'index': i,
                    'timestamp': candles[i].get('timestamp', 0)
                })

        return swing_highs

    def _find_swing_lows(self, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Находит локальные минимумы (Swing Lows) с индексом и timestamp"""
        swing_lows = []

        for i in range(self.swing_lookback, len(candles) - self.swing_lookback):
            current_low = candles[i]['low']
            left_lows = [candles[j]['low'] for j in range(i - self.swing_lookback, i)]
            right_lows = [candles[j]['low'] for j in range(i + 1, i + self.swing_lookback + 1)]

            if current_low <= min(left_lows) and current_low <= min(right_lows):
                swing_lows.append({
                    'price': current_low,
                    'index': i,
                    'timestamp': candles[i].get('timestamp', 0)
                })

        return swing_lows

    def _group_levels(self, levels: List[Dict[str, Any]], pool_type: str) -> List[Dict[str, Any]]:
        """Группирует близкие ценовые уровни с сохранением timestamp"""
        if not levels:
            return []

        sorted_levels = sorted(levels, key=lambda x: x['price'])

        groups = []
        current_group = [sorted_levels[0]]

        for level in sorted_levels[1:]:
            last_price = current_group[-1]['price']
            diff_pct = abs(level['price'] - last_price) / last_price

            if diff_pct < self.price_tolerance_pct:
                current_group.append(level)
            else:
                avg_price = sum(l['price'] for l in current_group) / len(current_group)
                min_timestamp = min(l.get('timestamp', 0) for l in current_group)
                groups.append({
                    'type': pool_type,
                    'price': avg_price,
                    'touches': len(current_group),
                    'formed_at': min_timestamp,
                    'strength': 'STRONG' if len(current_group) >= 3 else 'NORMAL'
                })
                current_group = [level]

        if current_group:
            avg_price = sum(l['price'] for l in current_group) / len(current_group)
            min_timestamp = min(l.get('timestamp', 0) for l in current_group)
            groups.append({
                'type': pool_type,
                'price': avg_price,
                'touches': len(current_group),
                'formed_at': min_timestamp,
                'strength': 'STRONG' if len(current_group) >= 3 else 'NORMAL'
            })

        return groups

    def find_nearest_liquidity_pool(
        self,
        pools: List[Dict[str, Any]],
        current_price: float,
        direction: str = 'SELL_SIDE'
    ) -> Optional[Dict[str, Any]]:
        """Находит ближайший пул ликвидности к текущей цене"""
        if not pools:
            return None

        filtered = [p for p in pools if p['type'] == direction]

        if not filtered:
            return None

        if direction == 'SELL_SIDE':
            below = [p for p in filtered if p['price'] < current_price]
            if below:
                return max(below, key=lambda x: x['price'])
            return None
        else:
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
        """Проверяет, было ли снятие ликвидности (Liquidity Grab)"""
        if not klines_h4 or not liquidity_pool:
            return False, None

        pool_price = liquidity_pool['price']
        pool_type = liquidity_pool['type']

        recent_candles = klines_h4[-lookback_candles:]

        if pool_type == 'SELL_SIDE':
            for candle in recent_candles:
                if candle['low'] < pool_price:
                    if current_price > pool_price:
                        logger.debug(f"💧 Обнаружен Liquidity Grab: прокол {pool_price:.4f} → возврат {current_price:.4f}")
                        return True, candle['low']
        else:
            for candle in recent_candles:
                if candle['high'] > pool_price:
                    if current_price < pool_price:
                        logger.debug(f"💧 Обнаружен Liquidity Grab: прокол {pool_price:.4f} → возврат {current_price:.4f}")
                        return True, candle['high']

        return False, None


__all__ = ['LiquidityScanner', 'LiquidityPool']