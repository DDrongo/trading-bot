#!/usr/bin/env python3
"""
ПРОВЕРКА FVG ЗОНЫ — С ТОЧНОЙ ДАТОЙ ФОРМИРОВАНИЯ ИЗ ДЕТЕКТОРА
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


def format_date(timestamp):
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp / 1000)
    return timestamp


def format_price(p):
    if p > 1000:
        return f"{p:.0f}"
    elif p > 100:
        return f"{p:.1f}"
    else:
        return f"{p:.2f}"


async def main():
    # Загружаем конфиг
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)

    symbol = "BTCUSDT"
    timeframe = "1w"

    print("=" * 80)
    print(f"🔍 ПОИСК FVG ЗОН ДЛЯ {symbol} ({timeframe})")
    print("=" * 80)

    # Получаем свечи
    klines = await data_provider.get_klines(symbol, timeframe, 60)

    # Конвертируем для FVG детектора
    candles = []
    for k in klines:
        candles.append({
            'open': float(k[1]),
            'high': float(k[2]),
            'low': float(k[3]),
            'close': float(k[4]),
            'timestamp': int(k[0]),
        })

    # Находим все FVG зоны
    detector = FVGDetector(lookback_candles=60, min_gap_pct=0.1)
    fvg_zones = detector.find_fvg(candles)

    # Находим зону 86444 - 92750
    target_zone = None
    for zone in fvg_zones:
        # Сравниваем границы (округлённо)
        if abs(zone.get('low', 0) - 86444) < 1000 and abs(zone.get('high', 0) - 92750) < 1000:
            target_zone = zone
            break

    if not target_zone:
        print("❌ Зона 86444-92750 не найдена в FVG детекторе!")
        return

    # Данные зоны
    zone_low = target_zone.get('low', 0)
    zone_high = target_zone.get('high', 0)
    formation_ts = target_zone.get('formed_at', 0)
    formation_date = format_date(formation_ts)

    print(f"\n📊 НАЙДЕНА ЗОНА:")
    print(f"   Границы: {format_price(zone_low)} - {format_price(zone_high)}")
    print(f"   Тип: {target_zone.get('type', 'unknown')}")
    print(f"   Дата формирования: {formation_date.strftime('%d %b %y') if formation_date else '?'}")

    # Сортируем свечи по дате
    candles_sorted = []
    for k in klines:
        ts = int(k[0]) / 1000
        dt = datetime.fromtimestamp(ts)
        # Для W1 нормализуем к понедельнику
        monday = dt - timedelta(days=dt.weekday())
        candles_sorted.append({
            'date': monday,
            'date_str': monday.strftime('%d %b %y'),
            'high': float(k[2]),
            'low': float(k[3]),
        })

    candles_sorted.sort(key=lambda x: x['date'])

    print(f"\n{'Дата':<15} {'High':>12} {'Low':>12} {'В зоне?'}")
    print("-" * 55)

    touches = 0
    after_formation = False

    for c in candles_sorted:
        if formation_date and c['date'] >= formation_date:
            after_formation = True

        if after_formation:
            in_zone = (c['high'] >= zone_low and c['low'] <= zone_high)
            marker = "✅ ДА" if in_zone else "❌ НЕТ"
            if in_zone:
                touches += 1
            print(f"{c['date_str']:<15} {c['high']:>12.0f} {c['low']:>12.0f} {marker}")

    print(f"\n📊 ИТОГ: касаний зоны ПОСЛЕ формирования = {touches}")

    if touches == 0:
        print("\n   ✅ ЗОНА АКТИВНАЯ — можно торговать")
    else:
        print(f"\n   ⚠️ ЗОНА НЕ АКТИВНАЯ — было {touches} касаний")

    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())