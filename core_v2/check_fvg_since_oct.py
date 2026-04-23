#!/usr/bin/env python3
"""
🔍 FVG ЗОНЫ ПОСЛЕ 6 ОКТЯБРЯ 2025 (ХАЙ 126k)
"""

import asyncio
import sys
import yaml
from pathlib import Path
from datetime import datetime, timedelta

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider
from core_v2.analyst.fvg_detector import FVGDetector


def format_price(p):
    if p > 1000:
        return f"{p:.0f}"
    elif p > 100:
        return f"{p:.1f}"
    else:
        return f"{p:.2f}"


async def main():
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)

    symbol = "BTCUSDT"
    timeframe = "1d"  # Дневные свечи для наглядности
    start_date = datetime(2025, 10, 6)

    print("=" * 80)
    print(f"🔍 FVG ЗОНЫ ПОСЛЕ {start_date.strftime('%d %b %y')} (ХАЙ 126k)")
    print(f"   Символ: {symbol}, Таймфрейм: {timeframe}")
    print("=" * 80)

    # Загружаем свечи с запасом (чтобы захватить падение)
    klines = await data_provider.get_klines(symbol, timeframe, 300)

    if not klines:
        print("❌ Нет данных!")
        return

    # Конвертируем
    candles = []
    for k in klines:
        ts = int(k[0]) / 1000
        dt = datetime.fromtimestamp(ts)
        candles.append({
            'date': dt,
            'date_str': dt.strftime('%d %b %y'),
            'open': float(k[1]),
            'high': float(k[2]),
            'low': float(k[3]),
            'close': float(k[4]),
            'timestamp': int(k[0]),
        })

    # Фильтруем после start_date
    candles_after = [c for c in candles if c['date'] >= start_date]

    print(f"\n📊 СВЕЧЕЙ ПОСЛЕ {start_date.strftime('%d %b %y')}: {len(candles_after)}")
    print(f"   Последняя свеча: {candles_after[-1]['date_str']}")

    # Находим FVG зоны
    detector = FVGDetector(lookback_candles=len(candles_after), min_gap_pct=0.1)
    fvg_zones = detector.find_fvg(candles_after)

    if not fvg_zones:
        print("\n❌ FVG зоны не найдены!")
        return

    print(f"\n🕳️ НАЙДЕНО FVG ЗОН: {len(fvg_zones)}")

    # Выводим зоны
    print(
        f"\n{'№':<3} {'Тип':<10} {'Нижняя':>12} {'Верхняя':>12} {'Дата формирования':<15} {'Возраст (дней)':<15} {'Было касаний?':<15}")
    print("-" * 90)

    for idx, zone in enumerate(fvg_zones, 1):
        zone_type = "БЫЧИЙ" if zone['type'] == 'bullish' else "МЕДВЕЖИЙ"
        low = zone.get('low', 0)
        high = zone.get('high', 0)
        formed_at = zone.get('formed_at', 0)

        if formed_at:
            dt = datetime.fromtimestamp(formed_at / 1000)
            date_str = dt.strftime('%d %b %y')
            age = (datetime.now() - dt).days
        else:
            date_str = '?'
            age = 0

        # Проверяем, были ли касания после формирования
        touches = 0
        for c in candles_after:
            if c['timestamp'] <= formed_at:
                continue
            if c['low'] <= high and c['high'] >= low:
                touches += 1

        visited_str = f"ДА ({touches})" if touches > 0 else "НЕТ"

        print(f"{idx:<3} {zone_type:<10} {low:>12.0f} {high:>12.0f} {date_str:<15} {age:<15} {visited_str:<15}")

    # Детальный вывод по зонам, которые могли остаться непротестированными
    print("\n" + "=" * 80)
    print("🎯 ЗОНЫ, КОТОРЫЕ МОГЛИ НЕ ЗАПОЛНИТЬСЯ (касаний = 0):")
    print("=" * 80)

    untouched_zones = []
    for zone in fvg_zones:
        formed_at = zone.get('formed_at', 0)
        low = zone.get('low', 0)
        high = zone.get('high', 0)

        touches = 0
        for c in candles_after:
            if c['timestamp'] <= formed_at:
                continue
            if c['low'] <= high and c['high'] >= low:
                touches += 1

        if touches == 0:
            untouched_zones.append(zone)
            zone_type = "БЫЧИЙ" if zone['type'] == 'bullish' else "МЕДВЕЖИЙ"
            print(f"\n   📍 {zone_type} FVG: {format_price(low)} - {format_price(high)}")
            if formed_at:
                dt = datetime.fromtimestamp(formed_at / 1000)
                print(f"      Сформирована: {dt.strftime('%d %b %y')}")

    if not untouched_zones:
        print("\n   ❌ Нет ни одной непротестированной FVG зоны.")
        print("   Все зоны, которые образовались после 6 октября 2025, уже были закрыты ценой.")
    else:
        print(f"\n   ⚠️ Найдено {len(untouched_zones)} непротестированных FVG зон!")
        print("   Это потенциальные цели для отскока.")

    print("\n" + "=" * 80)
    print("✅ АНАЛИЗ ЗАВЕРШЁН")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())