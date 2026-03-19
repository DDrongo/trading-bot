#!/usr/bin/env python3
# test_fix.py
"""
Тестирование исправленных расчетов SL/TP
"""

import asyncio
import logging
import sys
import yaml
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('test_fix')


async def test_calculations():
    """Тестирование расчетов на примере проблемного символа"""

    # Импортируем исправленные классы
    from analyzer.core.screen3_signal_generator import Screen3SignalGenerator

    # Загружаем конфиг
    config_path = Path(__file__).parent / 'analyzer/config/config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Создаем анализатор
    analyzer = Screen3SignalGenerator(config)

    # Тестовые данные (имитируем реальные данные)
    print(f"\n{'=' * 80}")
    print("🔧 ТЕСТИРОВАНИЕ РАСЧЕТОВ SL/TP")
    print(f"{'=' * 80}")

    # Тест 1: SELL сигнал (как XLMUSDT с проблемой)
    print(f"\n📊 ТЕСТ 1: SELL СИГНАЛ (как XLMUSDT)")

    entry_price = 0.2500
    atr = 0.0005  # Очень маленький ATR (0.2% от цены)

    print(f"   Entry Price: {entry_price:.4f}")
    print(f"   ATR: {atr:.6f} ({atr / entry_price * 100:.3f}% от цены)")

    # Расчет SL
    stop_loss = analyzer._calculate_stop_loss(
        entry_price=entry_price,
        signal_type="SELL",
        atr=atr,
        resistance_level=0.2520,  # Уровень сопротивления чуть выше
        support_level=0.2480
    )

    print(f"   Stop Loss: {stop_loss:.6f}")
    print(f"   Расстояние SL: {abs(stop_loss - entry_price) / entry_price * 100:.3f}%")

    # Расчет TP
    take_profit = analyzer._calculate_take_profit(
        entry_price=entry_price,
        stop_loss=stop_loss,
        signal_type="SELL",
        atr=atr
    )

    print(f"   Take Profit: {take_profit:.6f}")
    print(f"   Расстояние TP: {abs(take_profit - entry_price) / entry_price * 100:.3f}%")

    # Расчет R/R
    risk = abs(stop_loss - entry_price)
    reward = abs(take_profit - entry_price)
    if risk > 0:
        rr_ratio = reward / risk
        print(f"   Risk/Reward: {rr_ratio:.2f}:1")
    else:
        print(f"   ❌ НУЛЕВОЙ РИСК!")

    # Тест 2: BUY сигнал (нормальный ATR)
    print(f"\n📊 ТЕСТ 2: BUY СИГНАЛ (нормальный ATR)")

    entry_price = 100.0
    atr = 1.5  # 1.5% от цены

    print(f"   Entry Price: {entry_price:.2f}")
    print(f"   ATR: {atr:.2f} ({atr / entry_price * 100:.2f}% от цены)")

    # Расчет SL
    stop_loss = analyzer._calculate_stop_loss(
        entry_price=entry_price,
        signal_type="BUY",
        atr=atr,
        resistance_level=105.0,
        support_level=98.0
    )

    print(f"   Stop Loss: {stop_loss:.2f}")
    print(f"   Расстояние SL: {abs(stop_loss - entry_price) / entry_price * 100:.2f}%")

    # Расчет TP
    take_profit = analyzer._calculate_take_profit(
        entry_price=entry_price,
        stop_loss=stop_loss,
        signal_type="BUY",
        atr=atr
    )

    print(f"   Take Profit: {take_profit:.2f}")
    print(f"   Расстояние TP: {abs(take_profit - entry_price) / entry_price * 100:.2f}%")

    # Тест 3: Пограничный случай (ATR = 0)
    print(f"\n📊 ТЕСТ 3: SELL с ATR = 0 (крайний случай)")

    entry_price = 50.0
    atr = 0.0

    print(f"   Entry Price: {entry_price:.2f}")
    print(f"   ATR: {atr:.6f} (НУЛЕВОЙ!)")

    stop_loss = analyzer._calculate_stop_loss(
        entry_price=entry_price,
        signal_type="SELL",
        atr=atr,
        resistance_level=51.0,
        support_level=49.0
    )

    print(f"   Stop Loss: {stop_loss:.6f}")

    if stop_loss <= entry_price:
        print(f"   ❌ ОШИБКА: SELL стоп {stop_loss:.6f} <= entry {entry_price:.6f}")
    else:
        print(f"   ✅ OK: SELL стоп {stop_loss:.6f} > entry {entry_price:.6f}")

    print(f"\n{'=' * 80}")
    print("🎯 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print(f"{'=' * 80}")


async def test_real_symbol():
    """Тестирование на реальном символе через полный анализ"""

    print(f"\n{'=' * 80}")
    print("🚀 ТЕСТ РЕАЛЬНОГО СИМВОЛА")
    print(f"{'=' * 80}")

    try:
        from analyzer.main import SignalGeneratorService

        service = SignalGeneratorService('analyzer/config/config.yaml')

        print("🔄 Инициализация сервиса...")
        if not await service.initialize():
            print("❌ Не удалось инициализировать сервис")
            return

        # Тестируем проблемные символы
        test_symbols = ["XLMUSDT", "XRPUSDT", "BTCUSDT"]

        for symbol in test_symbols:
            print(f"\n🔍 Анализ {symbol}...")

            result = await service.analyze_single_symbol(symbol)

            if result and 'error' not in result:
                print(f"   ✅ Анализ успешен")
                print(f"   Signal: {result.get('signal', 'N/A')}")
                print(f"   Entry: {result.get('entry', 0):.6f}")
                print(f"   Stop Loss: {result.get('stop_loss', 0):.6f}")
                print(f"   Take Profit: {result.get('take_profit', 0):.6f}")
                print(f"   R/R: {result.get('risk_reward', 0):.2f}:1")

                # Проверяем корректность
                entry = result.get('entry', 0)
                sl = result.get('stop_loss', 0)
                tp = result.get('take_profit', 0)

                if result.get('signal') == 'SELL':
                    if sl <= entry:
                        print(f"   ❌ ОШИБКА: SELL SL {sl:.6f} <= Entry {entry:.6f}")
                    if tp >= entry:
                        print(f"   ❌ ОШИБКА: SELL TP {tp:.6f} >= Entry {entry:.6f}")
                elif result.get('signal') == 'BUY':
                    if sl >= entry:
                        print(f"   ❌ ОШИБКА: BUY SL {sl:.6f} >= Entry {entry:.6f}")
                    if tp <= entry:
                        print(f"   ❌ ОШИБКА: BUY TP {tp:.6f} <= Entry {entry:.6f}")
            else:
                print(f"   ❌ Ошибка анализа: {result.get('error', 'Unknown')}")

        await service.cleanup()

    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()


async def main():
    print("🧪 ЗАПУСК ТЕСТОВ ИСПРАВЛЕНИЙ")

    # Запускаем тесты расчетов
    await test_calculations()

    # Запускаем тест реального символа (опционально, требует интернета)
    choice = input("\n📡 Запустить тест реального символа (требуется интернет)? (y/n): ")
    if choice.lower() == 'y':
        await test_real_symbol()

    print(f"\n{'=' * 80}")
    print("🎉 ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    asyncio.run(main())