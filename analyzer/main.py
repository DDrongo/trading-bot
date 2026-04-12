#!/usr/bin/env python3
# analyzer/main.py (ИСПРАВЛЕННЫЙ - ЛОГИ РАБОТАЮТ)
"""
🚀 ГЛАВНЫЙ ЗАПУСКАЕМЫЙ ФАЙЛ ДЛЯ АНАЛИЗАТОРА СИГНАЛОВ
"""

import asyncio
import logging
import yaml
import sys
from typing import List, Dict, Any, Optional
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
    """
    Простая и надёжная настройка логирования
    """
    log_file = LOG_DIR / 'signal_generator.log'

    # Очищаем существующие handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Настраиваем корневой логгер
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )

    # Устанавливаем уровень для консоли (только INFO и выше)
    logging.root.handlers[0].setLevel(logging.INFO)

    # Уменьшаем спам от некоторых модулей
    logging.getLogger('api_client_bybit').setLevel(logging.WARNING)
    logging.getLogger('liquidity_prefilter').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)

    print(f"✅ Логирование: консоль (INFO+) + файл {log_file} (DEBUG)")


# Настраиваем логирование
setup_logging()
logger = logging.getLogger('signal_generator')


class SignalGeneratorService:
    """Главный сервис генерации сигналов"""

    def __init__(self, config_path: str = 'analyzer/config/config.yaml'):
        self.config_path = config_path
        self.config = self._load_config()
        self.orchestrator: Optional[AnalysisOrchestrator] = None
        self.position_manager: Optional[PositionManager] = None

        data_provider.configure(self.config)
        logger.info("✅ DataProvider сконфигурирован")

        self.market_type = self.config.get('market_type', 'linear')
        self.trading_mode = self.config.get('trading_mode', 'pro')
        logger.info(f"🎯 Рынок: {self.market_type.upper()}")
        logger.info(f"🎯 Режим торговли: {self.trading_mode.upper()}")

    def _load_config(self) -> Dict[str, Any]:
        try:
            config_full_path = PROJECT_ROOT / self.config_path
            with open(config_full_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"✅ Конфигурация загружена")
            return config
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфига: {e}")
            return {}

    async def _on_signal_generated(self, event: Event):
        try:
            data = event.data
            logger.info(
                f"🔔 СИГНАЛ #{data.get('signal_id')}: {data.get('symbol')} {data.get('signal_type')} @ {data.get('entry_price'):.4f}")
        except Exception as e:
            logger.error(f"❌ Ошибка в обработчике событий: {e}")

    async def initialize(self) -> bool:
        try:
            logger.info("🚀 Инициализация...")
            await event_bus.start()
            event_bus.subscribe(EventType.TRADING_SIGNAL_GENERATED, self._on_signal_generated)

            from analyzer.core.signal_repository import signal_repository
            await signal_repository.initialize()

            self.orchestrator = AnalysisOrchestrator(self.config)
            if not await self.orchestrator.initialize():
                logger.error("❌ Не удалось инициализировать оркестратор")
                return False

            self.position_manager = PositionManager(self.config)
            await self.position_manager.initialize()

            logger.info("✅ Готов к работе")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return False

    async def continuous_monitoring(self, interval_seconds: int = 60):
        logger.info(f"🔄 Запуск мониторинга (интервал {interval_seconds} сек, режим {self.trading_mode.upper()})")
        iteration = 0

        while True:
            iteration += 1
            iteration_start = datetime.now()

            try:
                logger.info(f"🔄 ИТЕРАЦИЯ #{iteration} - {datetime.now().strftime('%H:%M:%S')}")

                all_symbols = await data_provider.get_all_symbols()
                if not all_symbols:
                    await asyncio.sleep(interval_seconds)
                    continue

                symbols_to_analyze = await self.orchestrator._get_liquid_symbols(all_symbols)
                logger.info(f"🔍 Анализ {len(symbols_to_analyze)} монет...")

                result = await self.orchestrator.analyze_symbols_batch(symbols_to_analyze)

                signals_found = sum(1 for a in result.values() if a and a.should_trade)
                if signals_found > 0:
                    logger.info(f"🎯 НАЙДЕНО {signals_found} СИГНАЛОВ!")
                else:
                    logger.info(f"💤 Сигналы не найдены")

                if self.position_manager:
                    stats = await self.position_manager.paper_account.get_statistics()
                    logger.info(f"💰 PAPER: Баланс {stats['balance']:.2f} | Позиций {stats['open_positions']}")

                iteration_duration = (datetime.now() - iteration_start).total_seconds()
                wait_time = max(1, interval_seconds - iteration_duration)
                await asyncio.sleep(wait_time)

            except asyncio.CancelledError:
                logger.info("🛑 Мониторинг остановлен")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка: {e}")
                await asyncio.sleep(interval_seconds)

    async def cleanup(self):
        logger.info("🧹 Очистка...")
        if self.position_manager:
            await self.position_manager.cleanup()
        if self.orchestrator:
            await self.orchestrator.cleanup()
        await event_bus.stop()
        await data_provider.close()


async def main():
    service = SignalGeneratorService()
    try:
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК БОТА")
        logger.info("=" * 60)
        logger.info(f"🎯 Режим: {service.trading_mode.upper()}")
        logger.info(f"📁 Логи: {LOG_DIR / 'signal_generator.log'}")
        logger.info("=" * 60)

        if not await service.initialize():
            return

        interval_seconds = service.config.get('analysis', {}).get('monitoring_interval_seconds', 60)
        await service.continuous_monitoring(interval_seconds)

    except KeyboardInterrupt:
        logger.info("🛑 Остановлено пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await service.cleanup()


if __name__ == "__main__":
    asyncio.run(main())