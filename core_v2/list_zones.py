#!/usr/bin/env python3
"""
ПОКАЗАТЬ ВСЕ FVG ЗОНЫ С ГРАНИЦАМИ
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


async def main():
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)

    symbol = "BTCUSDT"
    timeframe = "1w"

    print("=" * 80)
    print(f"🔍 ВСЕ FVG ЗОНЫ ДЛЯ {symbol} ({timeframe})")
    print("=" * 80)

    klines = await data_provider.get_klines(symbol, timeframe, 60)

    candles = []
    for k in klines:
        candles.append({
            'open': float(k[1]),
            'high': float(k[2]),
            'low': float(k[3]),
            'close': float(k[4]),
            'timestamp': int(k[0]),
        })

    detector = FVGDetector(lookback_candles=60, min_gap_pct=0.1)
    fvg_zones = detector.find_fvg(candles)

    print(f"\n{'№':<3} {'Тип':<10} {'Нижняя':>12} {'Верхняя':>12} {'Дата формирования':<15}")
    print("-" * 65)

    for idx, zone in enumerate(fvg_zones, 1):
        zone_type = zone.get('type', '?')
        low = zone.get('low', 0)
        high = zone.get('high', 0)
        formed_at = zone.get('formed_at', 0)

        if formed_at:
            dt = datetime.fromtimestamp(formed_at / 1000)
            monday = dt - timedelta(days=dt.weekday())
            date_str = monday.strftime('%d %b %y')
        else:
            date_str = '?'

        print(f"{idx:<3} {zone_type:<10} {low:>12.0f} {high:>12.0f} {date_str:<15}")

    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())