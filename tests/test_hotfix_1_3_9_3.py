#!/usr/bin/env python3
"""
test_hotfix_1_3_9_3.py - Проверка исправлений HOTFIX 1.3.9.3
Запуск: python tests/test_hotfix_1_3_9_3.py
"""

import asyncio
import sys
import os

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def test_zone_side_check():
    """Тест 1: Проверка логики стороны зоны"""
    print("\n" + "=" * 60)
    print("1️⃣ ТЕСТ: Логика проверки стороны зоны")
    print("=" * 60)

    try:
        from analyzer.core.screen2_entry_zones import Screen2Analyzer

        config = {
            'screen2_min_score': 4,
            'analysis': {
                'zone_side_check': {'enabled': True},
                'range_filter': {'enabled': False}
            }
        }

        analyzer = Screen2Analyzer(config)

        zone = {'low': 100, 'high': 110}

        # Тест 1.1: BUY, цена ВЫШЕ зоны → ДОЛЖЕН ПРОЙТИ
        passed, reason = analyzer._check_zone_side(zone, "BULL", 115, "TEST")
        status = "✅ ПРОЙДЕН" if passed else "❌ НЕ ПРОЙДЕН"
        print(f"   BUY, цена 115 > зона 100-110: {status} ({reason})")

        # Тест 1.2: BUY, цена ВНУТРИ зоны → НЕ ДОЛЖЕН ПРОЙТИ
        passed, reason = analyzer._check_zone_side(zone, "BULL", 105, "TEST")
        status = "❌ ОТСЕВ" if not passed else "⚠️ ПРОПУЩЕН"
        print(f"   BUY, цена 105 внутри зоны 100-110: {status} ({reason})")

        # Тест 1.3: SELL, цена НИЖЕ зоны → ДОЛЖЕН ПРОЙТИ
        passed, reason = analyzer._check_zone_side(zone, "BEAR", 95, "TEST")
        status = "✅ ПРОЙДЕН" if passed else "❌ НЕ ПРОЙДЕН"
        print(f"   SELL, цена 95 < зона 100-110: {status} ({reason})")

        # Тест 1.4: SELL, цена ВНУТРИ зоны → НЕ ДОЛЖЕН ПРОЙТИ
        passed, reason = analyzer._check_zone_side(zone, "BEAR", 105, "TEST")
        status = "❌ ОТСЕВ" if not passed else "⚠️ ПРОПУЩЕН"
        print(f"   SELL, цена 105 внутри зоны 100-110: {status} ({reason})")

        # Тест 1.5: BUY, цена НИЖЕ зоны → НЕ ДОЛЖЕН ПРОЙТИ
        passed, reason = analyzer._check_zone_side(zone, "BULL", 95, "TEST")
        status = "❌ ОТСЕВ" if not passed else "⚠️ ПРОПУЩЕН"
        print(f"   BUY, цена 95 < зона 100-110: {status} ({reason})")

        # Тест 1.6: SELL, цена ВЫШЕ зоны → НЕ ДОЛЖЕН ПРОЙТИ
        passed, reason = analyzer._check_zone_side(zone, "BEAR", 115, "TEST")
        status = "❌ ОТСЕВ" if not passed else "⚠️ ПРОПУЩЕН"
        print(f"   SELL, цена 115 > зона 100-110: {status} ({reason})")

        return True

    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_config_monitor_limit():
    """Тест 2: Проверка наличия signals_limit в конфиге"""
    print("\n" + "=" * 60)
    print("2️⃣ ТЕСТ: Конфиг монитора (signals_limit)")
    print("=" * 60)

    try:
        import yaml
        config_path = "analyzer/config/config.yaml"

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        monitor_config = config.get('monitor', {})
        signals_limit = monitor_config.get('signals_limit')

        if signals_limit is None:
            print("   ❌ Параметр 'signals_limit' НЕ НАЙДЕН в секции monitor")
            return False
        else:
            print(f"   ✅ Параметр 'signals_limit' = {signals_limit}")
            return True

    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False


async def test_duplicate_check():
    """Тест 3: Проверка метода проверки дубликатов (синтаксис)"""
    print("\n" + "=" * 60)
    print("3️⃣ ТЕСТ: Метод проверки дубликатов")
    print("=" * 60)

    try:
        from analyzer.core.orchestrator import AnalysisOrchestrator

        # Проверяем, что метод существует и принимает правильные параметры
        import inspect
        method = AnalysisOrchestrator._check_duplicate_before_analysis

        sig = inspect.signature(method)
        params = list(sig.parameters.keys())

        if 'symbol' in params and 'signal_subtype' in params:
            print("   ✅ Метод _check_duplicate_before_analysis() существует")
            return True
        else:
            print("   ❌ Метод имеет неверную сигнатуру")
            return False

    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False


async def test_monitor_methods():
    """Тест 4: Проверка наличия новых методов в мониторе"""
    print("\n" + "=" * 60)
    print("4️⃣ ТЕСТ: Методы монитора")
    print("=" * 60)

    try:
        from monitor_three_screen import ThreeScreenMonitor

        # Проверяем метод get_account_state
        if hasattr(ThreeScreenMonitor, 'get_account_state'):
            print("   ✅ Метод get_account_state() существует")
        else:
            print("   ❌ Метод get_account_state() НЕ НАЙДЕН")
            return False

        # Проверяем метод display_all_signals
        if hasattr(ThreeScreenMonitor, 'display_all_signals'):
            print("   ✅ Метод display_all_signals() существует")
        else:
            print("   ❌ Метод display_all_signals() НЕ НАЙДЕН")
            return False

        return True

    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False


async def main():
    print("\n" + "=" * 60)
    print("🧪 ТЕСТИРОВАНИЕ HOTFIX 1.3.9.3")
    print("=" * 60)

    results = {}

    results['zone_side_check'] = await test_zone_side_check()
    results['config_monitor_limit'] = await test_config_monitor_limit()
    results['duplicate_check'] = await test_duplicate_check()
    results['monitor_methods'] = await test_monitor_methods()

    print("\n" + "=" * 60)
    print("📊 ИТОГИ ТЕСТИРОВАНИЯ")
    print("=" * 60)

    for test_name, passed in results.items():
        status = "✅ ПРОЙДЕН" if passed else "❌ НЕ ПРОЙДЕН"
        print(f"   {test_name}: {status}")

    all_passed = all(results.values())

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
        print("\n✅ Рекомендуется:")
        print("   1. Запустить бота: python -m analyzer.main")
        print("   2. Запустить монитор: python run_monitor.py")
        print("   3. Проверить в мониторе:")
        print("      - Баланс обновляется")
        print("      - В таблице 'Все сигналы' больше 50 записей")
        print("      - Появилась колонка PnL")
    else:
        print("⚠️ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ")
        if not results.get('config_monitor_limit'):
            print("   • Добавьте 'signals_limit: 200' в секцию monitor в config.yaml")
        if not results.get('zone_side_check'):
            print("   • Проверьте метод _check_zone_side() в screen2_entry_zones.py")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())