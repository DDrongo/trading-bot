#!/usr/bin/env python3
"""
🚀 ГЛАВНЫЙ ЗАПУСКАЕМЫЙ ФАЙЛ ДЛЯ АНАЛИЗАТОРА СИГНАЛОВ
ЦИКЛИЧЕСКИЙ МОНИТОРИНГ КАЖДЫЕ 60 СЕКУНД!
"""

import asyncio
import logging
import yaml
import sys
from typing import List, Dict, Any
from datetime import datetime

# ✅ ИСПРАВЛЕННЫЕ ИМПОРТЫ
from analyzer.core.api_client_bybit import BybitAPIClient
from analyzer.core.orchestrator import AnalysisOrchestrator
# ✅ ДОБАВЛЕННЫЕ ИМПОРТЫ ДЛЯ EVENTBUS
from analyzer.core.event_bus import event_bus, EventType, Event

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/signal_generator.log')
    ]
)
logger = logging.getLogger('signal_generator')


class SignalGeneratorService:
    """Главный сервис генерации сигналов"""

    def __init__(self, config_path: str = 'config/config.yaml'):
        self.config_path = config_path
        self.config = self._load_config()
        self.api_client = None
        self.orchestrator = None

    def _load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"✅ Конфигурация загружена из {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфига: {e}")
            return {}

    # ✅ ТЕСТОВЫЙ ОБРАБОТЧИК СОБЫТИЙ С ДИАГНОСТИКОЙ
    async def _on_signal_generated(self, event: Event):
        """Тестовый обработчик события генерации сигнала"""
        try:
            data = event.data
            signal_id = data.get("signal_id")

            # ✅ ДИАГНОСТИКА В ОБРАБОТЧИКЕ
            logger.info("=" * 60)
            logger.info("🔍 [ОБРАБОТЧИК EVENT BUS] ПОЛУЧЕНЫ ДАННЫЕ:")
            logger.info(f"   Тип события: {event.event_type.value}")
            logger.info(f"   Источник: {event.source}")
            logger.info(f"   Время: {event.timestamp}")
            logger.info(f"   Все данные: {data}")
            logger.info(f"   signal_id из данных: {signal_id}, тип: {type(signal_id)}")

            if signal_id is None:
                logger.warning("⚠️ [ОБРАБОТЧИК] ВНИМАНИЕ: Получен сигнал без ID!")
                signal_id_display = "NO_ID"
            else:
                signal_id_display = signal_id
                logger.info(f"✅ [ОБРАБОТЧИК] ID успешно получен: {signal_id}")

            logger.info(f"🎯 [EVENT BUS] Получен новый сигнал!")
            logger.info(f"   ID: {signal_id_display}")
            logger.info(f"   {data.get('symbol')}: {data.get('signal_type')}")
            logger.info(f"   Entry: {data.get('entry_price'):.4f}")
            logger.info(f"   Confidence: {data.get('confidence'):.2f}")
            logger.info(f"   R/R: {data.get('risk_reward_ratio'):.2f}:1")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ Ошибка в обработчике событий: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

    async def initialize(self) -> bool:
        """Инициализация всех компонентов"""
        try:
            logger.info("🚀 Инициализация SignalGeneratorService...")

            # ✅ 1. ЗАПУСК EVENTBUS
            logger.info("🔌 Запуск EventBus...")
            await event_bus.start()

            # Проверяем статус
            stats = event_bus.get_stats()
            logger.info(f"📊 EventBus статистика: {stats}")
            logger.info("✅ EventBus запущен")

            # ✅ 2. ПОДПИСКА НА СОБЫТИЯ (ДЛЯ ТЕСТА)
            event_bus.subscribe(EventType.TRADING_SIGNAL_GENERATED, self._on_signal_generated)
            logger.info("✅ Выполнена подписка на события сигналов")

            # 3. API клиент
            self.api_client = BybitAPIClient(self.config)
            api_init = await self.api_client.initialize()
            if not api_init:
                logger.error("❌ Не удалось инициализировать API клиент")
                return False

            # 4. Репозиторий сигналов
            from analyzer.core.signal_repository import signal_repository
            repo_init = await signal_repository.initialize()
            if not repo_init:
                logger.warning("⚠️ Не удалось инициализировать репозиторий сигналов, сигналы не будут сохраняться")

            # 5. Оркестратор анализа
            self.orchestrator = AnalysisOrchestrator(self.api_client, self.config)
            orchestrator_init = await self.orchestrator.initialize()
            if not orchestrator_init:
                logger.error("❌ Не удалось инициализировать оркестратор")
                return False

            logger.info("✅ SignalGeneratorService готов к работе")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def analyze_single_symbol(self, symbol: str) -> Dict[str, Any]:
        """Анализ одного символа"""
        try:
            logger.info(f"🔍 Анализ символа: {symbol}")

            analysis = await self.orchestrator.analyze_single_symbol(symbol)

            if not analysis:
                return {"symbol": symbol, "error": "Анализ не удался"}

            result = {
                "symbol": symbol,
                "timestamp": datetime.now().isoformat(),
                "should_trade": analysis.should_trade,
                "confidence": analysis.overall_confidence,
                "signal": analysis.screen3.signal_type if analysis.screen3 else None,
                "entry": analysis.screen3.entry_price if analysis.screen3 else None,
                "stop_loss": analysis.screen3.stop_loss if analysis.screen3 else None,
                "take_profit": analysis.screen3.take_profit if analysis.screen3 else None,
                "risk_reward": analysis.screen3.indicators.get('risk_reward_ratio', 0) if analysis.screen3 else 0
            }

            logger.info(f"📊 Результат анализа {symbol}: {'✅ СИГНАЛ' if result['should_trade'] else '❌ НЕТ СИГНАЛА'}")
            return result

        except Exception as e:
            logger.error(f"❌ Ошибка анализа {symbol}: {e}")
            return {"symbol": symbol, "error": str(e)}

    async def analyze_symbols_batch(self, symbols: List[str]) -> Dict[str, Any]:
        """Анализ пачки символов"""
        try:
            logger.info(f"📦 Анализ пачки из {len(symbols)} символов")

            results = await self.orchestrator.analyze_symbols_batch(symbols)

            signals = []
            for symbol, analysis in results.items():
                if analysis and analysis.should_trade:
                    signals.append({
                        "symbol": symbol,
                        "signal": analysis.screen3.signal_type,
                        "entry": analysis.screen3.entry_price,
                        "stop_loss": analysis.screen3.stop_loss,
                        "take_profit": analysis.screen3.take_profit,
                        "confidence": analysis.overall_confidence,
                        "risk_reward": analysis.screen3.indicators.get('risk_reward_ratio', 0)
                    })

            return {
                "total_analyzed": len(results),
                "signals_found": len(signals),
                "signals": signals,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Ошибка анализа пачки: {e}")
            return {"error": str(e), "total_analyzed": 0, "signals": []}

    async def continuous_monitoring(self, interval_seconds: int = 60):
        """
        ЦИКЛИЧЕСКИЙ МОНИТОРИНГ КАЖДЫЕ 60 СЕКУНД!
        Автоматически получает все монеты с биржи и анализирует их
        """
        logger.info(f"🔄 Запуск циклического мониторинга каждые {interval_seconds} секунд")

        iteration = 0

        while True:
            iteration += 1
            iteration_start = datetime.now()

            try:
                logger.info(f"\n{'=' * 60}")
                logger.info(f"🔄 ИТЕРАЦИЯ #{iteration} - {datetime.now().strftime('%H:%M:%S')}")
                logger.info(f"{'=' * 60}")

                # 🔥 АВТОМАТИЧЕСКИ ПОЛУЧАЕМ ВСЕ МОНЕТЫ С БИРЖИ
                logger.info("📡 Получение списка всех торговых пар...")
                all_symbols = await self.api_client.get_all_symbols()

                if not all_symbols:
                    logger.warning("❌ Не удалось получить список символов с биржи")
                    await asyncio.sleep(interval_seconds)
                    continue

                # Фильтруем только USDT пары
                usdt_symbols = [s for s in all_symbols if s.endswith("USDT")]
                logger.info(f"✅ Получено {len(all_symbols)} символов, USDT пар: {len(usdt_symbols)}")

                # Берем топ-N монет для анализа (можно все)
                max_symbols_per_cycle = self.config.get('analysis', {}).get('max_symbols_per_cycle', 50)
                symbols_to_analyze = usdt_symbols[:max_symbols_per_cycle]

                logger.info(f"🔍 Анализ {len(symbols_to_analyze)} монет...")

                # Анализируем пачку
                result = await self.analyze_symbols_batch(symbols_to_analyze)

                # Выводим результаты
                signals_found = result.get('signals_found', 0)
                total_analyzed = result.get('total_analyzed', 0)

                if signals_found > 0:
                    logger.info(f"🎯 НАЙДЕНО {signals_found} СИГНАЛОВ!")
                    for signal in result['signals']:
                        rr = signal['risk_reward']
                        risk_pct = abs(signal['entry'] - signal['stop_loss']) / signal['entry'] * 100
                        logger.info(f"   📈 {signal['symbol']}: {signal['signal']} @ {signal['entry']:.4f}")
                        logger.info(
                            f"      SL: {signal['stop_loss']:.4f} (-{risk_pct:.2f}%) | TP: {signal['take_profit']:.4f} | R/R: {rr:.2f}:1")
                else:
                    logger.info("❌ Сигналы не найдены")

                # Логируем статистику итерации
                iteration_duration = (datetime.now() - iteration_start).total_seconds()
                logger.info(f"📊 Итог итерации #{iteration}: "
                            f"Проанализировано: {total_analyzed}/{len(symbols_to_analyze)}, "
                            f"Сигналов: {signals_found}, "
                            f"Время: {iteration_duration:.1f} сек")

                # Ожидание до следующей итерации
                wait_time = max(1, interval_seconds - iteration_duration)
                if wait_time > 0:
                    logger.info(f"⏳ Следующая итерация через {wait_time} секунд...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"⚠️ Итерация заняла больше времени ({iteration_duration:.1f} сек) чем интервал!")
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("🛑 Циклический мониторинг остановлен пользователем")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в циклическом мониторинге: {e}")
                await asyncio.sleep(interval_seconds)

    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("🧹 Очистка ресурсов...")

        # ✅ КОРРЕКТНАЯ ОСТАНОВКА EVENTBUS
        try:
            # Отписываемся от событий
            event_bus.unsubscribe(EventType.TRADING_SIGNAL_GENERATED, self._on_signal_generated)
            logger.info("✅ Отписка от событий выполнена")

            # Останавливаем EventBus
            await event_bus.stop()
            logger.info("✅ EventBus остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка при остановке EventBus: {e}")

        if self.orchestrator:
            await self.orchestrator.cleanup()
        if self.api_client:
            await self.api_client.close()
        logger.info("✅ Ресурсы очищены")


async def main():
    """Основная функция запуска"""
    service = SignalGeneratorService()

    try:
        # Инициализация
        logger.info("🚀 ЗАПУСК БОТА С ЦИКЛИЧЕСКИМ МОНИТОРИНГОМ")
        logger.info("🎯 НАСТРОЙКИ: R/R 3:1+, SL 1%, TP 1.5%+, ЦИКЛ 60 сек")
        logger.info("🔌 EventBus интегрирован для межмодульной коммуникации")

        if not await service.initialize():
            logger.error("❌ Не удалось инициализировать сервис")
            return

        # 🔥 ЗАПУСК ЦИКЛИЧЕСКОГО МОНИТОРИНГА КАЖДЫЕ 60 СЕКУНД
        interval_seconds = service.config.get('analysis', {}).get('monitoring_interval_seconds', 60)

        logger.info(f"🔄 Запуск циклического мониторинга каждые {interval_seconds} секунд")
        logger.info("📡 Бот будет автоматически получать ВСЕ монеты с биржи")
        logger.info("🎯 Сигналы будут с R/R 3:1+ и улучшенными Stop Loss")

        await service.continuous_monitoring(interval_seconds=interval_seconds)

    except KeyboardInterrupt:
        logger.info("🛑 Остановлено пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
    finally:
        await service.cleanup()


if __name__ == "__main__":
    asyncio.run(main())