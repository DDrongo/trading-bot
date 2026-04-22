#!/usr/bin/env python3
# analyzer/main.py

import asyncio
import logging
import yaml
import sys
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from analyzer.core.orchestrator import AnalysisOrchestrator
from analyzer.core.event_bus import event_bus, EventType, Event
from analyzer.core.position_manager import PositionManager
from analyzer.core.data_provider import data_provider

PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / 'logs' / 'bot'
LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging():
    """Настройка логирования"""
    log_file = LOG_DIR / 'signal_generator.log'

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(message)s'))

    from analyzer.utils.logging_filters import ConsoleFilter
    console.addFilter(ConsoleFilter())

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
    ))

    logging.root.setLevel(logging.DEBUG)
    logging.root.addHandler(console)
    logging.root.addHandler(file_handler)


setup_logging()
logger = logging.getLogger('signal_generator')


class SignalGeneratorService:
    def __init__(self, config_path: str = 'analyzer/config/config.yaml'):
        self.config_path = config_path
        self.config = self._load_config()
        self.orchestrator = None
        self.position_manager = None
        data_provider.configure(self.config)
        self.trading_mode = self.config.get('trading_mode', 'pro')

    def _load_config(self) -> Dict[str, Any]:
        try:
            path = PROJECT_ROOT / self.config_path
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки конфига: {e}")
            return {}

    async def _on_signal_generated(self, event: Event):
        data = event.data
        entry_type = data.get('entry_type', 'UNKNOWN')
        icon = "🎯" if entry_type == "SNIPER" else "📈" if entry_type == "TREND" else "📊"
        print(f"\n{icon} СИГНАЛ #{data.get('signal_id')}: {data.get('symbol')} {data.get('signal_type')} [{entry_type}] @ {data.get('entry_price'):.4f}\n")

    async def initialize(self) -> bool:
        print("🚀 Инициализация...")
        try:
            await event_bus.start()
            event_bus.subscribe(EventType.TRADING_SIGNAL_GENERATED, self._on_signal_generated)

            from analyzer.core.signal_repository import signal_repository
            await signal_repository.initialize()

            self.orchestrator = AnalysisOrchestrator(self.config)
            if not await self.orchestrator.initialize():
                print("❌ Ошибка: оркестратор")
                return False

            self.position_manager = PositionManager(self.config)
            await self.position_manager.initialize()

            print("✅ Готов к работе")
            return True
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return False

    async def continuous_monitoring(self, interval: int = 60):
        last_watch = -1
        last_signals = -1
        last_pnl = None

        while True:
            try:
                from analyzer.core.signal_repository import signal_repository

                # Проверяем, есть ли активные WATCH сигналы
                watch_count = await signal_repository.get_watch_count()

                if watch_count > 0:
                    # РЕЖИМ МОНИТОРИНГА: не фильтруем, только ждём цену
                    print(f"👀 Мониторим {watch_count} WATCH сигналов...")

                    # Получаем активные позиции
                    if self.position_manager:
                        stats = await self.position_manager.paper_account.get_statistics()
                        if stats['total_pnl'] != last_pnl:
                            print(f"💰 PnL: {stats['total_pnl']:+.0f} USDT")
                            last_pnl = stats['total_pnl']

                    await asyncio.sleep(interval)
                    continue

                # ПОЛНЫЙ ЦИКЛ (нет WATCH сигналов)
                print("📡 Загружаем список монет...")
                all_symbols = await data_provider.get_all_symbols()
                if not all_symbols:
                    await asyncio.sleep(interval)
                    continue
                print(f"   ✅ Получено {len(all_symbols)} монет")

                print("🔍 Фильтруем монеты по ликвидности...")
                symbols = await self.orchestrator._get_liquid_symbols(all_symbols)
                print(f"   ✅ Осталось {len(symbols)} монет")

                if symbols:
                    print(f"🔬 Анализируем {len(symbols)} монет...")
                    result = await self.orchestrator.analyze_symbols_batch(symbols)

                    signals = sum(1 for a in result.values() if a and a.should_trade)
                    watch = sum(
                        1 for a in result.values() if a and a.screen2 and a.screen2.passed and not a.should_trade)

                    if watch != last_watch or signals != last_signals:
                        print(f"\n👀 WATCH: {watch} | ⚡ СИГНАЛОВ: {signals}")
                        last_watch = watch
                        last_signals = signals

                    if self.position_manager:
                        stats = await self.position_manager.paper_account.get_statistics()
                        if stats['total_pnl'] != last_pnl:
                            print(f"💰 PnL: {stats['total_pnl']:+.0f} USDT")
                            last_pnl = stats['total_pnl']

                print(f"⏳ Пауза {interval} сек...\n")
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                print("\n🛑 Остановлено")
                break
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                await asyncio.sleep(interval)

    async def cleanup(self):
        if self.position_manager:
            await self.position_manager.cleanup()
        if self.orchestrator:
            await self.orchestrator.cleanup()
        await event_bus.stop()
        await data_provider.close()


async def main():
    service = SignalGeneratorService()
    print("=" * 50)
    print("🚀 GANDALF 2.0 SMC")
    print(f"🎯 Режим: {service.trading_mode.upper()}")
    print("=" * 50)

    if not await service.initialize():
        print("❌ Ошибка инициализации")
        return

    interval = service.config.get('analysis', {}).get('monitoring_interval_seconds', 60)
    await service.continuous_monitoring(interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлено")