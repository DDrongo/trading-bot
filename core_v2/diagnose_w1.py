#!/usr/bin/env python3
"""
🔍 ДИАГНОСТИКА W1 — Проверка данных и расчётов
"""

import asyncio
import sys
import yaml
from pathlib import Path

# Добавляем корень проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider
from core_v2.w1_trend_analyzer import W1TrendAnalyzer


async def main():
    # Загружаем конфиг
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Конфигурируем data_provider
    data_provider.configure(config)

    symbol = "BTCUSDT"

    print("=" * 70)
    print(f"🔍 ДИАГНОСТИКА W1 ДЛЯ {symbol}")
    print("=" * 70)

    # 1. Получаем реальную текущую цену
    print("\n📡 1. ТЕКУЩАЯ РЕАЛЬНАЯ ЦЕНА:")
    real_price = await data_provider.get_current_price(symbol, force_refresh=True)

    if real_price is None:
        print(f"   ❌ Не удалось получить цену {symbol}")
        return

    print(f"   {symbol} = {real_price:.2f} USDT")

    # 2. Получаем W1 свечи
    print("\n📊 2. W1 СВЕЧИ (последние 10):")
    w1_klines = await data_provider.get_klines(symbol, "1w", 52)

    if not w1_klines:
        print("   ❌ Нет данных!")
        return

    print(f"   Всего свечей: {len(w1_klines)}")
    print(f"\n   {'Дата':<12} {'Open':>12} {'High':>12} {'Low':>12} {'Close':>12} {'Volume':>15}")
    print("   " + "-" * 75)

    from datetime import datetime
    for k in w1_klines[-10:]:
        try:
            ts = int(k[0]) / 1000
            date = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            open_p = float(k[1])
            high_p = float(k[2])
            low_p = float(k[3])
            close_p = float(k[4])
            volume = float(k[5]) if len(k) > 5 else 0
            print(f"   {date:<12} {open_p:>12.2f} {high_p:>12.2f} {low_p:>12.2f} {close_p:>12.2f} {volume:>15.0f}")
        except Exception as e:
            print(f"   Ошибка: {e}")

    # 3. Закрытия для EMA
    closes = [float(k[4]) for k in w1_klines]
    print(f"\n📈 3. ПОСЛЕДНИЕ 10 ЦЕН ЗАКРЫТИЯ:")
    for i, c in enumerate(closes[-10:], 1):
        print(f"   {i}: {c:.2f}")

    # 4. Рассчитываем EMA20 и EMA50
    print("\n📐 4. РАСЧЁТ EMA:")

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
        current_ema20 = ema20[-1]
        current_ema50 = ema50[-1]
        print(f"   EMA20 = {current_ema20:.2f}")
        print(f"   EMA50 = {current_ema50:.2f}")

        if current_ema20 > current_ema50:
            ema_trend = "BULL (EMA20 > EMA50)"
        elif current_ema20 < current_ema50:
            ema_trend = "BEAR (EMA20 < EMA50)"
        else:
            ema_trend = "SIDEWAYS"
        print(f"   Тренд по EMA: {ema_trend}")

    # 5. Запускаем анализатор
    print("\n🤖 5. РЕЗУЛЬТАТ W1TrendAnalyzer:")
    analyzer = W1TrendAnalyzer()
    result = analyzer.analyze(symbol, w1_klines)

    print(f"   Тренд: {result.trend}")
    print(f"   Сила: {result.strength:.1f}%")
    print(f"   Структура: {result.structure}")
    print(f"   ADX: {result.adx:.1f}")
    print(f"   EMA20 (из анализатора): {result.ema20:.2f}")
    print(f"   EMA50 (из анализатора): {result.ema50:.2f}")

    # 6. Сравнение с реальной ценой
    print("\n🎯 6. СРАВНЕНИЕ:")
    print(f"   Реальная цена сейчас: {real_price:.2f}")
    print(f"   EMA20: {result.ema20:.2f} → цена {'ВЫШЕ' if real_price > result.ema20 else 'НИЖЕ'} EMA20")
    print(f"   EMA50: {result.ema50:.2f} → цена {'ВЫШЕ' if real_price > result.ema50 else 'НИЖЕ'} EMA50")

    if real_price > result.ema20 and real_price > result.ema50:
        print("   ⚡ Цена ВЫШЕ обеих EMA → БЫЧИЙ сигнал")
    elif real_price < result.ema20 and real_price < result.ema50:
        print("   ⚡ Цена НИЖЕ обеих EMA → МЕДВЕЖИЙ сигнал")
    else:
        print("   ⚡ Цена МЕЖДУ EMA → СМЕШАННЫЙ сигнал")

    print("\n" + "=" * 70)
    print("✅ Диагностика завершена")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())