#!/usr/bin/env python3
"""
🔍 ДИАГНОСТИКА FVG ЗОНЫ - ПОСЛЕДУЮЩИЕ СВЕЧИ
"""

import asyncio
import sys
import yaml
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider


async def main():
    # Загружаем конфиг
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)

    symbol = "BTCUSDT"
    timeframe = "1w"
    limit = 60

    # Параметры зоны
    zone_low = 97868
    zone_high = 100688
    formation_date = datetime(2025, 5, 12)

    print("=" * 80)
    print(f"🔍 ДИАГНОСТИКА FVG ЗОНЫ ДЛЯ {symbol} ({timeframe})")
    print(f"   Зона: {zone_low} - {zone_high} USDT")
    print(f"   Дата формирования: {formation_date.strftime('%d %b %y')}")
    print("=" * 80)

    # Получаем свечи
    klines = await data_provider.get_klines(symbol, timeframe, limit)

    if not klines:
        print("❌ Нет данных!")
        return

    # Конвертируем и выводим
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
            'close': float(k[4])
        })

    # Сортируем по дате
    candles.sort(key=lambda x: x['date'])

    print(f"\nВсего свечей: {len(candles)}")
    print(f"\nСВЕЧИ ПОСЛЕ ФОРМИРОВАНИЯ ЗОНЫ ({formation_date.strftime('%d %b %y')}):")
    print(f"\n{'Дата':<15} {'High':>12} {'Low':>12} {'Close':>12} {'В зоне?'}")
    print("-" * 65)

    after_formation = False
    visited_count = 0
    visited_dates = []

    for c in candles:
        if c['date'] >= formation_date:
            after_formation = True

        if after_formation:
            # Проверяем, касалась ли цена зоны
            # Цена В ЗОНЕ, если High >= zone_low и Low <= zone_high
            is_in_zone = (c['high'] >= zone_low and c['low'] <= zone_high)

            in_zone_marker = "✅ ДА" if is_in_zone else "❌ НЕТ"

            if is_in_zone:
                visited_count += 1
                visited_dates.append(c['date_str'])

            print(f"{c['date_str']:<15} {c['high']:>12.0f} {c['low']:>12.0f} {c['close']:>12.0f} {in_zone_marker}")

    print("\n" + "=" * 80)
    print("📊 ИТОГИ:")
    print(f"   Всего свечей после формирования: {len([c for c in candles if c['date'] >= formation_date])}")
    print(f"   Касаний зоны: {visited_count}")
    if visited_dates:
        print(f"   Даты касаний: {', '.join(visited_dates)}")

    if visited_count > 0:
        print(f"\n   ⚠️ ЗОНА НЕ АКТИВНА! Было {visited_count} касаний.")
    else:
        print(f"\n   ✅ ЗОНА АКТИВНА! Нет касаний после формирования.")

    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())