#!/usr/bin/env python3
# analyzer/main.py (ИСПРАВЛЕННЫЙ - единая папка logs)
"""
🚀 ГЛАВНЫЙ ЗАПУСКАЕМЫЙ ФАЙЛ ДЛЯ АНАЛИЗАТОРА СИГНАЛОВ
ФАЗА 1.3.6.1: Финальная архитектура
- WATCH сигналы (монеты в зоне интереса)
- M15 сигналы (рыночные ордера, 3ч, R/R ≥ 3:1)
- WebSocket для раннего входа
- Только MARKET ордера
"""

import asyncio
import logging
import yaml
import sys
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

from analyzer.core.api_client_bybit import BybitAPIClient
from analyzer.core.orchestrator import AnalysisOrchestrator
from analyzer.core.event_bus import event_bus, EventType, Event
from analyzer.core.position_manager import PositionManager

# ✅ ИСПРАВЛЕНО: единая папка logs
PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / 'logs' / 'bot'
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / 'signal_generator.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('signal_generator')


class SignalGeneratorService:
    """Главный сервис генерации сигналов (Фаза 1.3.6.1)"""

    def __init__(self, config_path: str = 'analyzer/config/config.yaml'):
        self.config_path = config_path
        self.config = self._load_config()
        self.api_client = None
        self.orchestrator = None
        self.position_manager = None

        self.market_type = self.config.get('market_type', 'linear')
        logger.info(f"🎯 Рынок: {self.market_type.upper()} ({'FUTURES' if self.market_type == 'linear' else 'SPOT'})")

    def _load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации"""
        try:
            config_full_path = PROJECT_ROOT / self.config_path
            with open(config_full_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"✅ Конфигурация загружена из {config_full_path}")
            return config
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфига: {e}")
            return {}

    async def _on_signal_generated(self, event: Event):
        """Обработчик события генерации сигнала"""
        try:
            data = event.data
            signal_id = data.get("signal_id")
            signal_subtype = data.get("signal_subtype", "M15")

            logger.info("=" * 60)
            logger.info(f"🔍 ПОЛУЧЕН СИГНАЛ: ID={signal_id}, тип={signal_subtype}")
            logger.info(f"   {data.get('symbol')}: {data.get('signal_type')} @ {data.get('entry_price'):.4f}")
            logger.info(f"   Confidence: {data.get('confidence'):.2f}, R/R: {data.get('risk_reward_ratio', 0):.2f}:1")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ Ошибка в обработчике событий: {e}")

    async def initialize(self) -> bool:
        """Инициализация всех компонентов"""
        try:
            logger.info("🚀 Инициализация SignalGeneratorService (Фаза 1.3.6.1)...")

            await event_bus.start()
            logger.info("✅ EventBus запущен")

            event_bus.subscribe(EventType.TRADING_SIGNAL_GENERATED, self._on_signal_generated)
            logger.info("✅ Подписка на события сигналов выполнена")

            self.api_client = BybitAPIClient(self.config)
            api_init = await self.api_client.initialize()
            if not api_init:
                logger.error("❌ Не удалось инициализировать API клиент")
                return False

            from analyzer.core.signal_repository import signal_repository
            repo_init = await signal_repository.initialize()
            if not repo_init:
                logger.warning("⚠️ Не удалось инициализировать репозиторий сигналов")

            self.orchestrator = AnalysisOrchestrator(self.api_client, self.config)
            orchestrator_init = await self.orchestrator.initialize()
            if not orchestrator_init:
                logger.error("❌ Не удалось инициализировать оркестратор")
                return False

            logger.info("🎯 Инициализация Position Manager (только MARKET ордера)...")
            self.position_manager = PositionManager(self.config, self.api_client)
            pm_init = await self.position_manager.initialize()
            if not pm_init:
                logger.warning("⚠️ Position Manager не инициализирован")
            else:
                logger.info("✅ Position Manager готов к работе")

            logger.info("✅ SignalGeneratorService готов к работе")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return False

    async def analyze_single_symbol(self, symbol: str) -> Dict[str, Any]:
        """Анализ одного символа"""
        try:
            logger.info(f"🔍 Анализ символа: {symbol}")

            analysis = await self.orchestrator.analyze_single_symbol(symbol)

            if not analysis:
                return {"symbol": symbol, "error": "Анализ не удался"}

            return {
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
        """Циклический мониторинг каждые N секунд"""
        logger.info(f"🔄 Запуск циклического мониторинга (Фаза 1.3.6.1)")
        logger.info(f"   Интервал: {interval_seconds} сек")
        logger.info(f"   Типы сигналов: WATCH (зона интереса) и M15 (рыночный ордер)")
        logger.info(f"   Логи сохраняются в: {LOG_DIR / 'signal_generator.log'}")

        iteration = 0

        while True:
            iteration += 1
            iteration_start = datetime.now()

            try:
                logger.info(f"\n{'=' * 60}")
                logger.info(f"🔄 ИТЕРАЦИЯ #{iteration} - {datetime.now().strftime('%H:%M:%S')}")
                logger.info(f"{'=' * 60}")

                logger.info("📡 Получение списка всех торговых пар...")
                all_symbols = await self.api_client.get_all_symbols()

                if not all_symbols:
                    logger.warning("❌ Не удалось получить список символов")
                    await asyncio.sleep(interval_seconds)
                    continue

                usdt_symbols = [s for s in all_symbols if s.endswith("USDT")]
                logger.info(f"✅ USDT пар: {len(usdt_symbols)}")

                max_symbols_per_cycle = self.config.get('analysis', {}).get('max_symbols_per_cycle', 50)
                symbols_to_analyze = usdt_symbols[:max_symbols_per_cycle]

                logger.info(f"🔍 Анализ {len(symbols_to_analyze)} монет...")

                result = await self.analyze_symbols_batch(symbols_to_analyze)

                signals_found = result.get('signals_found', 0)
                total_analyzed = result.get('total_analyzed', 0)

                if signals_found > 0:
                    logger.info(f"🎯 НАЙДЕНО {signals_found} M15 СИГНАЛОВ!")
                    for signal in result['signals']:
                        rr = signal['risk_reward']
                        logger.info(f"   📈 {signal['symbol']}: {signal['signal']} @ {signal['entry']:.4f}")
                        logger.info(f"      SL: {signal['stop_loss']:.4f} | TP: {signal['take_profit']:.4f} | R/R: {rr:.2f}:1")
                else:
                    logger.info("❌ M15 сигналы не найдены")

                iteration_duration = (datetime.now() - iteration_start).total_seconds()
                logger.info(f"📊 Итог итерации #{iteration}: "
                            f"Проанализировано: {total_analyzed}, "
                            f"M15 сигналов: {signals_found}, "
                            f"Время: {iteration_duration:.1f} сек")

                if self.position_manager:
                    stats = await self.position_manager.paper_account.get_statistics()
                    logger.info(f"💰 PAPER СЧЁТ: Баланс: {stats['balance']:.2f} USDT, "
                                f"Открыто: {stats['open_positions']}, "
                                f"Всего сделок: {stats['total_trades']}, "
                                f"Win Rate: {stats['win_rate']:.1f}%")

                wait_time = max(1, interval_seconds - iteration_duration)
                if wait_time > 0:
                    logger.info(f"⏳ Следующая итерация через {wait_time} сек...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"⚠️ Итерация заняла {iteration_duration:.1f} сек!")
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("🛑 Циклический мониторинг остановлен")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в мониторинге: {e}")
                await asyncio.sleep(interval_seconds)

    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("🧹 Очистка ресурсов...")

        if self.position_manager:
            await self.position_manager.cleanup()

        try:
            event_bus.unsubscribe(EventType.TRADING_SIGNAL_GENERATED, self._on_signal_generated)
            await event_bus.stop()
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
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК БОТА (ФАЗА 1.3.6.1)")
        logger.info("=" * 60)
        logger.info("🎯 НАСТРОЙКИ:")
        logger.info("   - Типы сигналов: WATCH (зона интереса) и M15 (рыночный ордер)")
        logger.info("   - M15: R/R ≥ 3:1, MARKET ордер, 3 часа")
        logger.info("   - WebSocket для реального времени")
        logger.info("   - Ранний вход при формировании паттерна")
        logger.info("   - FUTURES рынок (USDT бессрочные)")
        logger.info(f"   - Логи: {LOG_DIR / 'signal_generator.log'}")
        logger.info("=" * 60)

        if not await service.initialize():
            logger.error("❌ Не удалось инициализировать сервис")
            return

        interval_seconds = service.config.get('analysis', {}).get('monitoring_interval_seconds', 60)
        await service.continuous_monitoring(interval_seconds=interval_seconds)

    except KeyboardInterrupt:
        logger.info("🛑 Остановлено пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        await service.cleanup()


if __name__ == "__main__":
    asyncio.run(main())