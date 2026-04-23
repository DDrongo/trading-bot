#!/usr/bin/env python3
"""
🔍 УНИВЕРСАЛЬНЫЙ АНАЛИЗ FVG — ДЛЯ ТРЁХ ТАЙМФРЕЙМОВ (W1, D1, H4)
Исправленная версия: проверка касаний по ДАТЕ
"""

import asyncio
import sys
import yaml
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider
from core_v2.analyst.fvg_detector import FVGDetector


def format_price(price: float) -> str:
    if price is None:
        return "-"
    if price > 1000:
        return f"{price:.0f}"
    elif price > 100:
        return f"{price:.1f}"
    elif price > 1:
        return f"{price:.2f}"
    else:
        return f"{price:.4f}"


def format_date(timestamp, timeframe: str = '4h') -> str:
    if isinstance(timestamp, (int, float)):
        dt = datetime.fromtimestamp(timestamp / 1000)
    elif isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            return timestamp
    else:
        return str(timestamp)

    if timeframe == '1w':
        monday = dt - timedelta(days=dt.weekday())
        dt = monday

    months = {
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    }
    return f"{dt.day} {months[dt.month]} {dt.strftime('%y')}"


def get_timeframe_name(tf: str) -> str:
    names = {
        '1w': 'НЕДЕЛЬНЫЙ (W1)',
        '1d': 'ДНЕВНОЙ (D1)',
        '4h': '4-ЧАСОВОЙ (H4)',
    }
    return names.get(tf, tf.upper())


# ========== ПРОВЕРКА КАСАНИЙ ПО ДАТЕ (ИСПРАВЛЕНО) ==========

def check_fvg_visited(fvg_zone: Dict, candles: List[Dict]) -> bool:
    """Проверяет, возвращалась ли цена в FVG зону после формирования (по ДАТЕ)"""
    zone_low = fvg_zone.get('low', 0)
    zone_high = fvg_zone.get('high', 0)
    formed_at = fvg_zone.get('formed_at', 0)

    if zone_low == 0 or zone_high == 0 or formed_at == 0:
        return False

    for candle in candles:
        candle_ts = candle.get('timestamp', 0)
        if candle_ts <= formed_at:
            continue

        candle_low = candle.get('low', 0)
        candle_high = candle.get('high', 0)

        if candle_low <= zone_high and candle_high >= zone_low:
            return True

    return False


def count_fvg_touches(fvg_zone: Dict, candles: List[Dict]) -> int:
    """Считает количество касаний FVG зоны (по ДАТЕ)"""
    zone_low = fvg_zone.get('low', 0)
    zone_high = fvg_zone.get('high', 0)
    formed_at = fvg_zone.get('formed_at', 0)

    if zone_low == 0 or zone_high == 0 or formed_at == 0:
        return 0

    touches = 0
    for candle in candles:
        candle_ts = candle.get('timestamp', 0)
        if candle_ts <= formed_at:
            continue

        candle_low = candle.get('low', 0)
        candle_high = candle.get('high', 0)

        if candle_low <= zone_high and candle_high >= zone_low:
            touches += 1

    return touches


def classify_fvg_zones(fvg_zones: List[Dict], candles: List[Dict], max_age: int = 30) -> tuple:
    """Классифицирует зоны: активные (0 касаний), тестированные (≥1 касание), старые (возраст ≥ max_age)"""
    active_zones = []
    tested_zones = []
    expired_zones = []

    for zone in fvg_zones:
        age = zone.get('age', 999)
        touches = count_fvg_touches(zone, candles)
        zone['touches'] = touches

        if age >= max_age:
            expired_zones.append(zone)
        elif touches == 0:
            zone['quality'] = 'FRESH'
            active_zones.append(zone)
        else:
            zone['quality'] = 'TESTED'
            tested_zones.append(zone)

    return active_zones, tested_zones, expired_zones


def find_fvg_with_candles(candles: List[Dict]) -> List[Dict]:
    detector = FVGDetector(lookback_candles=100, min_gap_pct=0.1)
    fvg_zones = detector.find_fvg(candles)

    enriched = []
    for zone in fvg_zones:
        idx = zone.get('formed_at_index', 0)
        if idx >= 2 and idx < len(candles):
            zone['candle_1'] = candles[idx - 2]
            zone['candle_2'] = candles[idx - 1]
            zone['candle_3'] = candles[idx]
            enriched.append(zone)

    return enriched


async def analyze_timeframe(symbol: str, timeframe: str, current_price: float, max_age: int = 30) -> Dict:
    if timeframe == '1w':
        limit = 60
    elif timeframe == '1d':
        limit = 200
    else:
        limit = 100

    klines = await data_provider.get_klines(symbol, timeframe, limit)
    if not klines:
        return {'error': 'no_data', 'timeframe': timeframe}

    candles = []
    for k in klines:
        candles.append({
            'open': float(k[1]),
            'high': float(k[2]),
            'low': float(k[3]),
            'close': float(k[4]),
            'timestamp': int(k[0]),
        })

    all_zones = find_fvg_with_candles(candles)
    active_zones, tested_zones, expired_zones = classify_fvg_zones(all_zones, candles, max_age)

    return {
        'timeframe': timeframe,
        'timeframe_name': get_timeframe_name(timeframe),
        'candles_count': len(candles),
        'total_zones': len(all_zones),
        'active_zones': active_zones,
        'tested_zones': tested_zones,
        'expired_zones': expired_zones,
        'current_price': current_price,
    }


def print_timeframe_result(result: Dict):
    if result.get('error'):
        print(f"\n❌ НЕТ ДАННЫХ для {result['timeframe_name']}")
        return

    tf = result['timeframe']
    current_price = result['current_price']
    active_zones = result['active_zones']
    tested_zones = result['tested_zones']

    print(f"\n{'=' * 80}")
    print(f"🔍 FVG АНАЛИЗ — {result['timeframe_name']}")
    print(f"{'=' * 80}")
    print(f"\n📡 ТЕКУЩАЯ ЦЕНА: {format_price(current_price)} USDT")

    if active_zones:
        print(f"\n   🟢 АКТИВНЫЕ (нет касаний, можно торговать):")
        for idx, zone in enumerate(active_zones, 1):
            zone_type = "БЫЧИЙ" if zone['type'] == 'bullish' else "МЕДВЕЖИЙ"
            print(f"\n   🔹 FVG #{idx} — {zone_type}")
            print(f"      Зона: {format_price(zone['low'])} - {format_price(zone['high'])} USDT")
            print(f"      Возраст: {zone.get('age', '?')} свечей, Касаний: {zone.get('touches', 0)}")

            print(f"\n      📍 КАК НАЙТИ НА ГРАФИКЕ ({tf.upper()}):")
            if 'candle_1' in zone:
                c1 = zone['candle_1']
                print(f"         1. Свеча №1: {format_date(c1['timestamp'], tf)}")
            if 'candle_3' in zone:
                c3 = zone['candle_3']
                print(f"         2. Свеча №3: {format_date(c3['timestamp'], tf)}")
    else:
        print(f"\n   🟢 АКТИВНЫЕ: 0 зон")

    if tested_zones:
        print(f"\n   🟡 ТЕСТИРОВАННЫЕ (были касания, НЕ входить): {len(tested_zones)} шт.")
        for idx, zone in enumerate(tested_zones[:5], 1):
            zone_type = "БЫЧИЙ" if zone['type'] == 'bullish' else "МЕДВЕЖИЙ"
            print(
                f"      {idx}. {zone_type}: {format_price(zone['low'])} - {format_price(zone['high'])} (касаний: {zone.get('touches', 0)})")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', default='BTCUSDT')
    parser.add_argument('--timeframes', default='1w,1d,4h')
    parser.add_argument('--max-age', type=int, default=30)
    args = parser.parse_args()

    timeframes = [tf.strip() for tf in args.timeframes.split(',')]

    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)
    symbol = args.symbol.upper()

    print("=" * 80)
    print(f"🔍 УНИВЕРСАЛЬНЫЙ FVG АНАЛИЗ ДЛЯ {symbol}")
    print(f"   Таймфреймы: {', '.join(timeframes)}")
    print(f"   Максимальный возраст зоны: {args.max_age} свечей")
    print("=" * 80)

    current_price = await data_provider.get_current_price(symbol, force_refresh=True)
    if current_price:
        print(f"\n📡 ТЕКУЩАЯ ЦЕНА: {format_price(current_price)} USDT")

    for tf in timeframes:
        result = await analyze_timeframe(symbol, tf, current_price or 0, args.max_age)
        print_timeframe_result(result)

    print("\n" + "=" * 80)
    print("✅ АНАЛИЗ ЗАВЕРШЁН")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())