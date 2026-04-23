#!/usr/bin/env python3
"""
🔍 ДИАГНОСТИКА FVG — АНАЛИЗ ЗОН С РАЗДЕЛЕНИЕМ НА АКТИВНЫЕ И ОТРАБОТАННЫЕ
"""

import asyncio
import sys
import yaml
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analyzer.core.data_provider import data_provider


# ========== ФУНКЦИИ ФИЛЬТРАЦИИ ==========

def check_fvg_visited(fvg_zone, candles):
    """
    Проверяет, возвращалась ли цена в FVG зону после формирования

    Returns:
        True если цена была в зоне, False если нет
    """
    zone_low = fvg_zone.get('low', 0)
    zone_high = fvg_zone.get('high', 0)
    formed_at_index = fvg_zone.get('formed_at_index', 0)

    if zone_low == 0 or zone_high == 0:
        return False

    for i in range(formed_at_index + 1, len(candles)):
        candle = candles[i]
        candle_low = candle.get('low', 0)
        candle_high = candle.get('high', 0)

        # Цена зашла в зону (пересекла границы)
        if candle_low <= zone_high and candle_high >= zone_low:
            return True

    return False


def count_fvg_touches(fvg_zone, candles):
    """
    Считает количество касаний FVG зоны

    Returns:
        Количество касаний
    """
    zone_low = fvg_zone.get('low', 0)
    zone_high = fvg_zone.get('high', 0)
    formed_at_index = fvg_zone.get('formed_at_index', 0)

    if zone_low == 0 or zone_high == 0:
        return 0

    touches = 0
    for i in range(formed_at_index + 1, len(candles)):
        candle = candles[i]
        candle_low = candle.get('low', 0)
        candle_high = candle.get('high', 0)

        # Проверяем касание (цена коснулась границы или вошла внутрь)
        if abs(candle_low - zone_high) / zone_high < 0.001 or \
                abs(candle_high - zone_low) / zone_low < 0.001 or \
                (candle_low <= zone_high and candle_high >= zone_low):
            touches += 1

    return touches


def filter_fvg_by_quality(fvg_zones, candles, max_age=30):
    """
    Фильтрует FVG зоны по качеству для торговли

    Критерии:
    - Возраст < max_age (по умолчанию 30)
    - Не более 1 касания
    - (возвращения в зону не отсекаем, но помечаем)
    """
    quality_zones = []

    for zone in fvg_zones:
        age = zone.get('age', 999)

        if age >= max_age:
            continue

        touches = count_fvg_touches(zone, candles)
        if touches > 1:
            continue

        zone['quality'] = 'FRESH' if touches == 0 else 'TESTED'
        zone['was_visited'] = check_fvg_visited(zone, candles)
        quality_zones.append(zone)

    return quality_zones


def find_fvg_with_candles(candles):
    """
    Находит FVG зоны и возвращает их вместе со свечами, которые их образовали
    """
    from core_v2.analyst.fvg_detector import FVGDetector

    detector = FVGDetector(lookback_candles=100, min_gap_pct=0.1)
    fvg_zones = detector.find_fvg(candles)

    # Обогащаем зоны свечами
    enriched_zones = []
    for zone in fvg_zones:
        idx = zone.get('formed_at_index', 0)
        if idx >= 2 and idx < len(candles):
            zone['candle_1'] = candles[idx - 2]
            zone['candle_2'] = candles[idx - 1]
            zone['candle_3'] = candles[idx]
            enriched_zones.append(zone)

    return enriched_zones


def format_time(timestamp):
    """Форматирует timestamp в строку"""
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M")
    return str(timestamp)


# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

async def main():
    # Загружаем конфиг
    config_path = project_root / "analyzer" / "config" / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    data_provider.configure(config)

    symbol = "BTCUSDT"  # Меняйте на нужный символ
    timeframe = "4h"
    max_age = 30  # Максимальный возраст зоны для торговли

    print("=" * 80)
    print(f"🔍 ДИАГНОСТИКА FVG ДЛЯ {symbol} ({timeframe})")
    print(f"   Максимальный возраст зоны для торговли: {max_age} свечей")
    print("=" * 80)

    # 1. Текущая цена
    print("\n📡 1. ТЕКУЩАЯ ЦЕНА:")
    real_price = await data_provider.get_current_price(symbol, force_refresh=True)
    print(f"   {symbol} = {real_price:.2f} USDT")

    # 2. Получаем свечи
    print(f"\n📊 2. ЗАГРУЗКА {timeframe.upper()} СВЕЧЕЙ:")
    klines = await data_provider.get_klines(symbol, timeframe, 100)

    if not klines:
        print("   ❌ Нет данных!")
        return

    print(f"   Загружено свечей: {len(klines)}")

    # Конвертируем в формат для анализа
    candles = []
    for k in klines:
        candles.append({
            'open': float(k[1]),
            'high': float(k[2]),
            'low': float(k[3]),
            'close': float(k[4]),
            'timestamp': int(k[0]),
            'time_str': datetime.fromtimestamp(int(k[0]) / 1000).strftime("%Y-%m-%d %H:%M")
        })

    # 3. Ищем FVG зоны
    print("\n🕳️ 3. ПОИСК FVG ЗОН:")
    all_fvg_zones = find_fvg_with_candles(candles)
    print(f"   Найдено зон (ВСЕГО): {len(all_fvg_zones)}")

    if not all_fvg_zones:
        print("\n❌ FVG зоны не найдены!")
        return

    # ========== 4. РАЗДЕЛЕНИЕ НА АКТИВНЫЕ И ОТРАБОТАННЫЕ ==========
    print("\n" + "=" * 80)
    print("📊 4. АНАЛИЗ КАЖДОЙ ЗОНЫ")
    print("=" * 80)

    active_zones = []  # Цена НЕ возвращалась
    visited_zones = []  # Цена УЖЕ была
    expired_zones = []  # Возраст >= 30

    for zone in all_fvg_zones:
        age = zone.get('age', 999)
        was_visited = check_fvg_visited(zone, candles)
        touches = count_fvg_touches(zone, candles)

        zone['was_visited'] = was_visited
        zone['touches'] = touches

        if age >= max_age:
            expired_zones.append(zone)
        elif was_visited or touches > 0:
            visited_zones.append(zone)
        else:
            active_zones.append(zone)

    # ВЫВОД СТАТИСТИКИ
    print(f"\n📈 СТАТИСТИКА ПО ЗОНАМ:")
    print(f"   ┌─────────────────────────────────────────────────┐")
    print(f"   │ Всего найдено зон:           {len(all_fvg_zones):>3}                         │")
    print(f"   ├─────────────────────────────────────────────────┤")
    print(f"   │ ✅ АКТИВНЫЕ (цена не была):   {len(active_zones):>3}   ← ПОДХОДЯТ ДЛЯ ТОРГОВЛИ │")
    print(f"   │ ❌ ОТРАБОТАННЫЕ (цена была):  {len(visited_zones):>3}                         │")
    print(f"   │ ⏰ СТАРЫЕ (возраст ≥ 30):     {len(expired_zones):>3}                         │")
    print(f"   └─────────────────────────────────────────────────┘")

    # ========== 5. АКТИВНЫЕ ЗОНЫ (ДЛЯ ТОРГОВЛИ) ==========
    if active_zones:
        print("\n" + "=" * 80)
        print("🎯 АКТИВНЫЕ ЗОНЫ (цена не возвращалась, ПОДХОДЯТ ДЛЯ ВХОДА)")
        print("=" * 80)

        for idx, zone in enumerate(active_zones, 1):
            print(f"\n{'─' * 80}")
            print(f"🟢 FVG #{idx} — {zone['type'].upper()}")
            print(f"   Возраст: {zone['age']} свечей | Сила: {zone['strength']}")
            print(f"   Касаний: {zone.get('touches', 0)} | Возвращение: НЕТ")
            print(f"{'─' * 80}")

            print(f"\n   ЗОНА ДИСБАЛАНСА:")
            print(f"      Нижняя граница: {zone['low']:.2f}")
            print(f"      Верхняя граница: {zone['high']:.2f}")

            if 'candle_1' in zone:
                print(f"\n   СВЕЧА №1 (начало разрыва): {zone['candle_1']['time_str']}")
                print(f"      High: {zone['candle_1']['high']:.2f}, Low: {zone['candle_1']['low']:.2f}")

            if 'candle_3' in zone:
                print(f"\n   СВЕЧА №3 (закрытие разрыва): {zone['candle_3']['time_str']}")
                print(f"      High: {zone['candle_3']['high']:.2f}, Low: {zone['candle_3']['low']:.2f}")

            # Расстояние до текущей цены
            if zone['type'] == 'bullish':
                dist = abs(zone['high'] - real_price)
                direction = "поддержка (ниже цены)" if zone['high'] < real_price else "поддержка (выше цены)"
            else:
                dist = abs(zone['low'] - real_price)
                direction = "сопротивление (выше цены)" if zone['low'] > real_price else "сопротивление (ниже цены)"

            print(f"\n   📍 Относительно текущей цены ({real_price:.2f}):")
            print(f"      {direction}")
            print(f"      Расстояние: {dist:.2f} USDT ({dist / real_price * 100:.2f}%)")

    # ========== 6. ОТРАБОТАННЫЕ ЗОНЫ (ДЛЯ ИНФОРМАЦИИ) ==========
    if visited_zones:
        print("\n" + "=" * 80)
        print("⚠️ ОТРАБОТАННЫЕ ЗОНЫ (цена уже была, НЕ ПОДХОДЯТ ДЛЯ ВХОДА)")
        print("=" * 80)
        print("   (показываем первые 5 для диагностики)\n")

        for idx, zone in enumerate(visited_zones[:5], 1):
            print(f"   {idx}. {zone['type'].upper()} — {zone['low']:.2f} - {zone['high']:.2f}")
            print(f"      Возраст: {zone['age']} свечей, Возвращение: ДА, Касаний: {zone.get('touches', 0)}")
            if 'candle_1' in zone:
                print(f"      Сформирована: {zone['candle_1']['time_str']}")

    # ========== 7. СТАРЫЕ ЗОНЫ ==========
    if expired_zones:
        print(f"\n📌 СТАРЫЕ ЗОНЫ (возраст ≥ {max_age} свечей): {len(expired_zones)} шт.")

    # ========== 8. ВЫВОД ИТОГА ==========
    print("\n" + "=" * 80)
    print("📋 ИТОГОВЫЙ ДИАГНОСТИЧЕСКИЙ ОТЧЁТ")
    print("=" * 80)

    print(f"""
   ✅ FVG ДЕТЕКТОР:
      - Успешно находит FVG зоны: {len(all_fvg_zones)} шт.
      - Определяет возраст зон: ДА
      - Определяет силу зон: ДА
      - Определяет координаты свечей: ДА

   ✅ ФИЛЬТРАЦИЯ КАЧЕСТВА:
      - Максимальный возраст: {max_age} свечей
      - Отслеживает возвращение цены в зону: ДА
      - Отслеживает количество касаний: ДА

   📊 РЕЗУЛЬТАТ ДЛЯ {symbol}:
      - Активных зон (можно торговать): {len(active_zones)}
      - Отработанных зон (нельзя): {len(visited_zones)}
      - Старых зон (нельзя): {len(expired_zones)}
""")

    if active_zones:
        print(f"   🎯 ДОСТУПНЫ СИГНАЛЫ: {len(active_zones)} FVG зон готовы к использованию!")
    else:
        print(f"   ⏳ НЕТ АКТИВНЫХ FVG ЗОН. Рынок пока не создал подходящих условий.")

    print("\n" + "=" * 80)
    print("✅ ДИАГНОСТИКА FVG ЗАВЕРШЕНА")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())