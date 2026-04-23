#!/usr/bin/env python3
"""
🔍 ДИАГНОСТИКА D1 — Проверка данных и расчётов дневного тренда
"""

import asyncio
import sys
import yaml
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider
from core_v2.screen1_trend_analyzer import Screen1TrendAnalyzer


async def main():
    # Загружаем конфиг
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)

    symbol = "BTCUSDT"

    print("=" * 70)
    print(f"🔍 ДИАГНОСТИКА D1 ДЛЯ {symbol}")
    print("=" * 70)

    # 1. Текущая цена
    print("\n📡 1. ТЕКУЩАЯ РЕАЛЬНАЯ ЦЕНА:")
    real_price = await data_provider.get_current_price(symbol, force_refresh=True)
    print(f"   {symbol} = {real_price:.2f} USDT")

    # 2. D1 свечи
    print("\n📊 2. D1 СВЕЧИ (последние 15):")
    d1_klines = await data_provider.get_klines(symbol, "1d", 100)

    if not d1_klines:
        print("   ❌ Нет данных!")
        return

    print(f"   Всего свечей: {len(d1_klines)}")
    print(f"\n   {'Дата':<12} {'Open':>12} {'High':>12} {'Low':>12} {'Close':>12}")
    print("   " + "-" * 65)

    from datetime import datetime
    for k in d1_klines[-15:]:
        try:
            ts = int(k[0]) / 1000
            date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            open_p = float(k[1])
            high_p = float(k[2])
            low_p = float(k[3])
            close_p = float(k[4])
            print(f"   {date:<12} {open_p:>12.2f} {high_p:>12.2f} {low_p:>12.2f} {close_p:>12.2f}")
        except Exception as e:
            print(f"   Ошибка: {e}")

    # 3. Цены закрытия
    closes = [float(k[4]) for k in d1_klines]
    print(f"\n📈 3. ПОСЛЕДНИЕ 15 ЦЕН ЗАКРЫТИЯ:")
    for i, c in enumerate(closes[-15:], 1):
        arrow = "↑" if i > 1 and c > closes[-15:][i - 2] else "↓"
        print(f"   {i:2d}: {c:.2f} {arrow}")

    # 4. Структура (higher highs / higher lows)
    print("\n📐 4. СТРУКТУРА ТРЕНДА (последние 10 свечей):")
    highs = [float(k[2]) for k in d1_klines[-10:]]
    lows = [float(k[3]) for k in d1_klines[-10:]]

    hh_count = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i - 1])
    hl_count = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i - 1])
    lh_count = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i - 1])
    ll_count = sum(1 for i in range(1, len(lows)) if lows[i] < lows[i - 1])

    print(f"   Higher Highs: {hh_count}/9")
    print(f"   Higher Lows:  {hl_count}/9")
    print(f"   Lower Highs:   {lh_count}/9")
    print(f"   Lower Lows:    {ll_count}/9")

    if hh_count >= 5 and hl_count >= 5:
        print("   → ВОСХОДЯЩАЯ структура (HH/HL)")
    elif lh_count >= 5 and ll_count >= 5:
        print("   → НИСХОДЯЩАЯ структура (LH/LL)")
    else:
        print("   → НЕОПРЕДЕЛЁННАЯ структура")

    # 5. EMA
    print("\n📐 5. EMA:")

    def calculate_ema(prices, period):
        if len(prices) < period:
            return []
        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for price in prices[period:]:
            ema.append((price * multiplier) + (ema[-1] * (1 - multiplier)))
        return ema

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)

    if ema20 and ema50:
        print(f"   EMA20 = {ema20[-1]:.2f}")
        print(f"   EMA50 = {ema50[-1]:.2f}")
        if ema20[-1] > ema50[-1]:
            print("   → EMA20 > EMA50 (BULL)")
        else:
            print("   → EMA20 < EMA50 (BEAR)")

    # 6. Запускаем анализатор
    print("\n🤖 6. РЕЗУЛЬТАТ Screen1TrendAnalyzer:")
    analyzer = Screen1TrendAnalyzer(config)
    result = analyzer.analyze_daily_trend(symbol, d1_klines)

    print(f"   Тренд: {result.trend_direction}")
    print(f"   Сила: {result.trend_strength}")
    print(f"   ADX: {result.indicators.get('adx', 0):.1f}")
    print(f"   Уверенность: {result.confidence_score:.1%}")

    # 7. Итог
    print("\n🎯 7. ИТОГ:")
    if real_price > ema20[-1] and real_price > ema50[-1]:
        print("   Цена ВЫШЕ обеих EMA → БЫЧИЙ сигнал")
    elif real_price < ema20[-1] and real_price < ema50[-1]:
        print("   Цена НИЖЕ обеих EMA → МЕДВЕЖИЙ сигнал")
    else:
        print("   Цена МЕЖДУ EMA → СМЕШАННЫЙ сигнал")

    print(f"\n   Рекомендация: ТРЕНД = {result.trend_direction}")

    print("\n" + "=" * 70)
    print("✅ Диагностика завершена")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())