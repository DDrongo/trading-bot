#!/usr/bin/env python3
# tools/run_backtest.py (ПОЛНОСТЬЮ)
"""
🧪 ЗАПУСК БЭКТЕСТА ИЗ КОМАНДНОЙ СТРОКИ
ФАЗА 1.5.1

Использование:
    python tools/run_backtest.py --start 2025-01-01 --end 2026-01-01 --symbols BTCUSDT,ETHUSDT
    python tools/run_backtest.py --start 2025-01-01 --end 2026-01-01 --all --mode light
    python tools/run_backtest.py --collect-levels --symbols BTCUSDT,ETHUSDT
    python tools/run_backtest.py --preload --symbols BTCUSDT,ETHUSDT --years 2
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from analyzer.core.backtester import Backtester, BacktestMode
from analyzer.core.historical_levels import historical_levels, LevelStrength
from analyzer.core.historical_data_provider import historical_data_provider
from analyzer.core.data_provider import data_provider


def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%H:%M:%S'
    )

    # Уменьшаем спам
    logging.getLogger('api_client_bybit').setLevel(logging.WARNING)
    logging.getLogger('liquidity_prefilter').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)


def parse_date(date_str: str) -> datetime:
    """Парсинг даты"""
    return datetime.strptime(date_str, '%Y-%m-%d')


def parse_symbols(symbols_str: str) -> List[str]:
    """Парсинг списка символов"""
    return [s.strip() for s in symbols_str.split(',') if s.strip()]


async def load_config():
    """Загрузка конфига"""
    try:
        import yaml
        config_path = Path(__file__).parent.parent / 'analyzer' / 'config' / 'config.yaml'
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"⚠️ Не удалось загрузить конфиг: {e}")
        return {}


async def cmd_backtest(args):
    """Команда запуска бэктеста"""
    config = await load_config()

    # Определяем символы
    if args.all:
        symbols = await data_provider.get_all_symbols()
        symbols = [s for s in symbols if s.endswith('USDT')][:20]  # Ограничиваем 20 символами
        print(f"📊 Всего символов: {len(symbols)}")
    else:
        symbols = parse_symbols(args.symbols)

    if not symbols:
        print("❌ Не указаны символы для бэктеста")
        return

    # Парсим даты
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)

    # Устанавливаем режим
    if args.mode:
        config['trading_mode'] = args.mode

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    🧪 ЗАПУСК БЭКТЕСТА                         ║
╠══════════════════════════════════════════════════════════════╣
║  Символы:      {len(symbols)} шт.
║  Период:       {start_date.date()} — {end_date.date()}
║  Режим:        {config.get('trading_mode', 'pro').upper()}
╚══════════════════════════════════════════════════════════════╝
""")

    # Создаём и инициализируем бэктестер
    backtester = Backtester(config)
    await backtester.initialize()

    # Запускаем бэктест
    results = await backtester.run_backtest(symbols, start_date, end_date)

    # Выводим итоги
    print("\n" + "=" * 60)
    print("📊 ИТОГИ БЭКТЕСТА")
    print("=" * 60)

    total_pnl = sum(r.total_pnl for r in results.values())
    total_trades = sum(r.total_trades for r in results.values())
    winning = sum(r.winning_trades for r in results.values())

    print(f"  Всего сделок:     {total_trades}")
    print(f"  Прибыльных:       {winning}")
    print(f"  Убыточных:        {total_trades - winning}")
    print(f"  Общий PnL:        {total_pnl:+.2f} USDT")
    print(f"  Win Rate:         {(winning / total_trades * 100) if total_trades > 0 else 0:.1f}%")
    print("=" * 60)


async def cmd_collect_levels(args):
    """Команда сбора исторических уровней"""
    config = await load_config()
    data_provider.configure(config)

    # ✅ Инициализируем с ПРАВИЛЬНЫМ путём к БД
    historical_config = config.get('historical_data', {})
    db_path = historical_config.get('db_path', 'data/historical.db')

    # Создаём коллектор с указанием БД
    from analyzer.core.historical_levels import HistoricalLevelsCollector
    collector = HistoricalLevelsCollector(db_path=db_path, config=config)
    await collector.initialize()

    symbols = parse_symbols(args.symbols) if args.symbols else await data_provider.get_all_symbols()
    symbols = [s for s in symbols if s.endswith('USDT')][:30]

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                 📊 СБОР ИСТОРИЧЕСКИХ УРОВНЕЙ                  ║
╠══════════════════════════════════════════════════════════════╣
║  Символы:      {len(symbols)} шт.
║  Таймфреймы:   W1 (2 года), D1 (1 год)
║  БД:           {db_path}
╚══════════════════════════════════════════════════════════════╝
""")

    results = await collector.collect_and_save_all(symbols)

    print("\n📊 Результаты сбора уровней:")
    print("-" * 40)
    total = 0
    for symbol, count in results.items():
        if count > 0:
            print(f"  {symbol}: {count} уровней")
            total += count

    print("-" * 40)
    print(f"  ВСЕГО: {total} уровней")

    stats = await collector.get_statistics()
    print(f"\n📈 Статистика БД:")
    print(f"  Всего уровней:    {stats.get('total', 0)}")
    print(f"  W1:               {stats.get('w1_count', 0)}")
    print(f"  D1:               {stats.get('d1_count', 0)}")
    print(f"  VERY_STRONG:      {stats.get('very_strong', 0)}")
    print(f"  STRONG:           {stats.get('strong', 0)}")


async def cmd_preload(args):
    """Команда предзагрузки исторических данных"""
    await historical_data_provider.initialize()

    symbols = parse_symbols(args.symbols) if args.symbols else await data_provider.get_all_symbols()
    symbols = [s for s in symbols if s.endswith('USDT')][:20]

    years = args.years or 1.0

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║              📦 ПРЕДЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ              ║
╠══════════════════════════════════════════════════════════════╣
║  Символы:      {len(symbols)} шт.
║  Период:       {years} год(а)
║  Интервалы:    1d, 4h, 15m
╚══════════════════════════════════════════════════════════════╝
""")

    intervals = ['1d', '4h', '15m']
    results = await historical_data_provider.preload_data(symbols, intervals, years)

    print("\n📊 Результаты загрузки:")
    print("-" * 50)
    total = 0
    for symbol, count in results.items():
        print(f"  {symbol}: {count} свечей")
        total += count

    print("-" * 50)
    print(f"  ВСЕГО: {total} свечей")

    # Статистика
    stats = await historical_data_provider.get_statistics()
    print(f"\n📈 Статистика БД:")
    print(f"  Всего свечей:     {stats.get('total_candles', 0)}")
    print(f"  Уникальных символов: {stats.get('symbols_count', 0)}")
    print(f"  Старейшая свеча:  {stats.get('oldest_candle', 'N/A')}")
    print(f"  Новейшая свеча:   {stats.get('newest_candle', 'N/A')}")


async def cmd_show_levels(args):
    """Команда отображения уровней для символа"""
    await historical_levels.initialize()

    symbol = args.symbol.upper()
    if not symbol.endswith('USDT'):
        symbol += 'USDT'

    # Получаем текущую цену
    data_provider.configure(await load_config())
    current_price = await data_provider.get_current_price(symbol)

    # Загружаем уровни
    levels = await historical_levels.get_historical_levels(symbol, LevelStrength.STRONG)

    supports, resistances = await historical_levels.get_levels_near_price(
        symbol, current_price or 0, range_pct=10.0
    )

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                 📊 ИСТОРИЧЕСКИЕ УРОВНИ                        ║
╠══════════════════════════════════════════════════════════════╣
║  Символ:       {symbol}
║  Текущая цена: {current_price:.6f} USDT
╠══════════════════════════════════════════════════════════════╣
""")

    print("🟢 ПОДДЕРЖКИ (ближайшие):")
    for level in supports[:5]:
        dist_pct = ((current_price - level.price) / current_price * 100) if current_price else 0
        print(f"  {level.price:.6f} ({level.strength.value}, {level.timeframe}, {level.touches} кас.) — {dist_pct:.2f}% ниже")

    print("\n🔴 СОПРОТИВЛЕНИЯ (ближайшие):")
    for level in resistances[:5]:
        dist_pct = ((level.price - current_price) / current_price * 100) if current_price else 0
        print(f"  {level.price:.6f} ({level.strength.value}, {level.timeframe}, {level.touches} кас.) — {dist_pct:.2f}% выше")

    print(f"\n📈 Всего уровней в БД: {len(levels)}")


async def cmd_history(args):
    """Команда отображения истории бэктестов"""
    backtester = Backtester(await load_config())
    await backtester.initialize()

    symbol = args.symbol.upper() if args.symbol else None
    if symbol and not symbol.endswith('USDT'):
        symbol += 'USDT'

    results = await backtester.get_historical_results(symbol, args.limit or 20)

    if not results:
        print("📭 Нет сохранённых результатов бэктеста")
        return

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                 📊 ИСТОРИЯ БЭКТЕСТОВ                          ║
╚══════════════════════════════════════════════════════════════╝
""")

    print(f"{'ID':<6} {'Символ':<12} {'Период':<22} {'Режим':<8} {'Сделок':<8} {'Win Rate':<10} {'P/L':<12}")
    print("-" * 80)

    for r in results:
        period = f"{r.start_date.date()} - {r.end_date.date()}"
        pnl_str = f"{r.total_pnl:+.2f}"
        print(f"{r.id:<6} {r.symbol:<12} {period:<22} {r.mode:<8} "
              f"{r.total_trades:<8} {r.win_rate:<10.1f}% {pnl_str:<12}")


async def main():
    parser = argparse.ArgumentParser(description='🧪 Backtester CLI')
    subparsers = parser.add_subparsers(dest='command', help='Команды')

    # Команда backtest
    backtest_parser = subparsers.add_parser('backtest', help='Запуск бэктеста')
    backtest_parser.add_argument('--start', required=True, help='Дата начала (YYYY-MM-DD)')
    backtest_parser.add_argument('--end', required=True, help='Дата окончания (YYYY-MM-DD)')
    backtest_parser.add_argument('--symbols', help='Символы через запятую')
    backtest_parser.add_argument('--all', action='store_true', help='Все USDT символы')
    backtest_parser.add_argument('--mode', choices=['light', 'pro'], help='Режим торговли')

    # Команда collect-levels
    collect_parser = subparsers.add_parser('collect-levels', help='Сбор исторических уровней')
    collect_parser.add_argument('--symbols', help='Символы через запятую')

    # Команда preload
    preload_parser = subparsers.add_parser('preload', help='Предзагрузка исторических данных')
    preload_parser.add_argument('--symbols', help='Символы через запятую')
    preload_parser.add_argument('--years', type=float, default=1.0, help='Количество лет истории')

    # Команда show-levels
    show_parser = subparsers.add_parser('show-levels', help='Показать уровни для символа')
    show_parser.add_argument('--symbol', required=True, help='Символ')

    # Команда history
    history_parser = subparsers.add_parser('history', help='История бэктестов')
    history_parser.add_argument('--symbol', help='Фильтр по символу')
    history_parser.add_argument('--limit', type=int, default=20, help='Лимит записей')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    setup_logging()

    if args.command == 'backtest':
        await cmd_backtest(args)
    elif args.command == 'collect-levels':
        await cmd_collect_levels(args)
    elif args.command == 'preload':
        await cmd_preload(args)
    elif args.command == 'show-levels':
        await cmd_show_levels(args)
    elif args.command == 'history':
        await cmd_history(args)


if __name__ == '__main__':
    asyncio.run(main())