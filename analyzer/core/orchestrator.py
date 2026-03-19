# core/orchestrator.py
"""
🎯 ОРКЕСТРАТОР - главный координатор всей системы анализа
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

# ИСПРАВЛЕННЫЕ ИМПОРТЫ
from .prefilter_liquidity import LiquidityPrefilter, PrefilterResult
from .three_screen_analyzer import ThreeScreenAnalyzer
from .event_bus import EventType, event_bus

# Импортируем ThreeScreenAnalysis из data_classes
from .data_classes import ThreeScreenAnalysis

logger = logging.getLogger('orchestrator')


@dataclass
class AnalysisSession:
    """Сессия анализа"""
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_symbols: int = 0
    analyzed_symbols: int = 0
    signals_found: int = 0
    prefilter_result: Optional[PrefilterResult] = None
    analysis_results: Dict[str, ThreeScreenAnalysis] = field(default_factory=dict)
    status: str = "running"  # running, completed, failed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_symbols": self.total_symbols,
            "analyzed_symbols": self.analyzed_symbols,
            "signals_found": self.signals_found,
            "status": self.status,
            "prefilter_stats": self.prefilter_result.to_dict() if self.prefilter_result else None,
            "symbols_analyzed": list(self.analysis_results.keys())
        }


class AnalysisOrchestrator:
    """
    Главный координатор для всей системы анализа
    Управляет процессом: префильтр → трёхэкранный анализ → результаты
    """

    def __init__(self, api_client, config=None):
        self.api = api_client
        self.config = config or {}  # Гарантируем, что config всегда словарь

        # Получаем параметры оркестрации из конфига
        analysis_config = self.config.get('analysis', {})
        self.orchestration_config = analysis_config.get('orchestration', {})
        self.caching_config = analysis_config.get('caching', {})

        # Инициализируем модули
        self.prefilter = LiquidityPrefilter(api_client, self.config)
        self.three_screen_analyzer = ThreeScreenAnalyzer(api_client, self.config)

        # Сессии анализа
        self._sessions: Dict[str, AnalysisSession] = {}
        self._current_session: Optional[AnalysisSession] = None

        # Статистика
        self._total_analyses = 0
        self._total_signals = 0

        logger.info("✅ AnalysisOrchestrator создан")

    async def initialize(self) -> bool:
        """Инициализация всех модулей"""
        logger.info("🚀 Инициализация оркестратора и модулей")

        try:
            # Инициализируем анализаторы
            three_screen_init = await self.three_screen_analyzer.initialize()

            if not three_screen_init:
                logger.error("❌ Не удалось инициализировать ThreeScreenAnalyzer")
                return False

            logger.info("✅ Все модули инициализированы")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации оркестратора: {e}")
            return False

    def create_session(self, symbols: List[str]) -> AnalysisSession:
        """Создание новой сессии анализа"""
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Получаем лимит логов из конфига
        log_symbols_limit = self.orchestration_config.get('log_symbols_limit', 10)

        session = AnalysisSession(
            session_id=session_id,
            start_time=datetime.now(),
            total_symbols=len(symbols)
        )

        self._sessions[session_id] = session
        self._current_session = session

        logger.info(f"📁 Создана сессия {session_id} для {len(symbols)} символов")

        # Публикуем событие
        asyncio.create_task(event_bus.publish(EventType.SESSION_STARTED, {
            "session_id": session_id,
            "symbol_count": len(symbols),
            "symbols": symbols[:log_symbols_limit]  # Ограничение из конфига
        }, source="orchestrator"))

        return session

    async def analyze_symbols_batch(self, symbols: List[str],
                                    max_concurrent: int = None) -> Dict[str, ThreeScreenAnalysis]:
        """
        Анализ пачки символов с префильтром

        Args:
            symbols: Список символов для анализа
            max_concurrent: Максимальное количество одновременных анализов (None = из конфига)

        Returns:
            Словарь с результатами анализа {symbol: ThreeScreenAnalysis}
        """
        logger.info(f"🚀 Начинаем анализ пачки из {len(symbols)} символов")

        # Используем параметр из конфига если не задан явно
        if max_concurrent is None:
            max_concurrent = self.orchestration_config.get('max_concurrent_analysis', 5)

        # Создаем сессию
        session = self.create_session(symbols)

        try:
            # ШАГ 1: ПРЕФИЛЬТРАЦИЯ ПО ЛИКВИДНОСТИ
            logger.info("🔍 ШАГ 1: Префильтрация по ликвидности...")
            prefilter_result = await self.prefilter.filter_symbols(symbols)
            session.prefilter_result = prefilter_result

            if not prefilter_result.filtered_symbols:
                logger.warning("❌ Префильтр не пропустил ни одного символа")
                session.status = "completed"
                session.end_time = datetime.now()
                return {}

            filtered_symbols = prefilter_result.filtered_symbols
            logger.info(f"✅ Префильтр: {len(symbols)} → {len(filtered_symbols)} символов")

            # ШАГ 2: ТРЁХЭКРАННЫЙ АНАЛИЗ ОТФИЛЬТРОВАННЫХ СИМВОЛОВ
            logger.info(f"📊 ШАГ 2: Запускаем трёхэкранный анализ для {len(filtered_symbols)} символов...")

            # Создаем семафор для ограничения параллельных запросов
            semaphore = asyncio.Semaphore(max_concurrent)
            results = {}

            async def analyze_with_semaphore(symbol: str):
                async with semaphore:
                    return await self._analyze_single_symbol(symbol, session)

            # Запускаем анализ всех символов параллельно с ограничением
            tasks = [analyze_with_semaphore(symbol) for symbol in filtered_symbols]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Собираем результаты
            for i, result in enumerate(batch_results):
                symbol = filtered_symbols[i]

                if isinstance(result, Exception):
                    logger.error(f"❌ Ошибка анализа {symbol}: {result}")
                    continue

                if result:
                    results[symbol] = result
                    session.analysis_results[symbol] = result

                    if result.should_trade:
                        session.signals_found += 1

            # Обновляем статистику сессии
            session.analyzed_symbols = len(results)
            session.status = "completed"
            session.end_time = datetime.now()

            # Обновляем глобальную статистику
            self._total_analyses += len(results)
            self._total_signals += session.signals_found

            # Логируем итоги
            logger.info(f"🎯 АНАЛИЗ ЗАВЕРШЕН: "
                        f"Проанализировано: {session.analyzed_symbols}/{session.total_symbols} "
                        f"Найдено сигналов: {session.signals_found} "
                        f"Время: {(session.end_time - session.start_time).total_seconds():.1f} сек")

            # Публикуем событие завершения
            asyncio.create_task(event_bus.publish(EventType.SESSION_COMPLETED, {
                "session_id": session.session_id,
                "total_symbols": session.total_symbols,
                "analyzed_symbols": session.analyzed_symbols,
                "signals_found": session.signals_found,
                "execution_time_seconds": (session.end_time - session.start_time).total_seconds()
            }, source="orchestrator"))

            return results

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при анализе пачки: {e}")
            session.status = "failed"
            session.end_time = datetime.now()

            # Публикуем событие ошибки
            asyncio.create_task(event_bus.publish(EventType.ERROR_OCCURRED, {
                "error": str(e),
                "session_id": session.session_id,
                "function": "analyze_symbols_batch"
            }, source="orchestrator"))

            return {}

    async def _analyze_single_symbol(self, symbol: str, session: AnalysisSession) -> Optional[ThreeScreenAnalysis]:
        """Анализ одного символа с логированием прогресса"""
        logger.debug(f"🔍 Анализ символа {symbol}")

        try:
            # Полный трёхэкранный анализ
            analysis = await self.three_screen_analyzer.analyze_symbol(symbol)

            if not analysis:
                logger.debug(f"❌ {symbol}: анализ не удался")
                return None

            # Логируем результат
            if analysis.should_trade:
                # ПРОВЕРЯЕМ КАЧЕСТВО СИГНАЛА ПЕРЕД ЛОГИРОВАНИЕМ
                if hasattr(analysis.screen3, 'quality_check'):
                    quality_result = analysis.screen3.quality_check()
                    if not quality_result.get('passed', True):
                        # Сигнал плохой - не логируем как найденный
                        logger.debug(
                            f"⚠️ {symbol}: сигнал отфильтрован - {quality_result.get('reason', 'плохое качество')}")
                        # Помечаем что не торгуем
                        analysis.should_trade = False
                        return analysis

                # Только хорошие сигналы логируем как найденные
                logger.info(f"✅ {symbol}: СИГНАЛ НАЙДЕН! "
                            f"{analysis.screen3.signal_type} @ {analysis.screen3.entry_price:.2f} "
                            f"(R/R: {analysis.screen3.indicators.get('risk_reward_ratio', 0):.2f}:1)")

                # Публикуем событие сигнала
                asyncio.create_task(event_bus.publish(EventType.TRADING_SIGNAL_GENERATED, {
                    "symbol": symbol,
                    "signal_type": analysis.screen3.signal_type,
                    "entry_price": analysis.screen3.entry_price,
                    "stop_loss": analysis.screen3.stop_loss,
                    "take_profit": analysis.screen3.take_profit,
                    "confidence": analysis.overall_confidence,
                    "analysis_duration": 0
                }, source="orchestrator"))
            else:
                logger.debug(f"❌ {symbol}: сигнал не найден (причина: не прошел экраны)")

            return analysis

        except Exception as e:
            logger.error(f"❌ Ошибка анализа символа {symbol}: {e}")
            return None

    async def analyze_single_symbol(self, symbol: str) -> Optional[ThreeScreenAnalysis]:
        """Анализ одного символа (без префильтра, с быстрой проверкой ликвидности)"""
        logger.info(f"🔍 Анализ одиночного символа: {symbol}")

        # Быстрая проверка ликвидности
        liquidity_ok = await self.prefilter.quick_check(symbol)
        if not liquidity_ok:
            logger.warning(f"❌ {symbol} не прошел проверку ликвидности")
            return None

        # Полный анализ
        return await self._analyze_single_symbol(symbol, None)

    def get_session(self, session_id: str) -> Optional[AnalysisSession]:
        """Получение информации о сессии"""
        return self._sessions.get(session_id)

    def get_all_sessions(self) -> List[AnalysisSession]:
        """Получение всех сессий"""
        return list(self._sessions.values())

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики оркестратора"""
        return {
            "total_sessions": len(self._sessions),
            "total_analyses": self._total_analyses,
            "total_signals": self._total_signals,
            "success_rate": (self._total_signals / self._total_analyses * 100
                             if self._total_analyses > 0 else 0),
            "current_session": self._current_session.session_id if self._current_session else None,
            "prefilter_cache_stats": self.prefilter.get_cache_stats()
        }

    async def run_continuous_analysis(self, symbols: List[str],
                                      interval_minutes: int = None,
                                      max_concurrent: int = None):
        """
        Непрерывный анализ символов с заданным интервалом

        Args:
            symbols: Список символов для мониторинга
            interval_minutes: Интервал между анализами в минутах (None = из конфига)
            max_concurrent: Максимальное количество одновременных анализов (None = из конфига)
        """
        # Получаем параметры из конфига если не заданы явно
        if interval_minutes is None:
            interval_minutes = self.orchestration_config.get('continuous_analysis_interval', 15)

        if max_concurrent is None:
            max_concurrent = self.orchestration_config.get('continuous_max_concurrent', 3)

        min_wait_seconds = self.caching_config.get('min_wait_seconds', 1)

        logger.info(f"🔄 Запуск непрерывного анализа {len(symbols)} символов "
                    f"каждые {interval_minutes} минут")

        try:
            while True:
                iteration_start = datetime.now()
                iteration_id = iteration_start.strftime("%Y%m%d_%H%M")

                logger.info(f"🔄 Итерация {iteration_id}: начинаем анализ")

                # Запускаем анализ пачки
                results = await self.analyze_symbols_batch(symbols, max_concurrent)

                # Логируем результаты итерации
                signals = [s for s, a in results.items() if a and a.should_trade]
                logger.info(f"🔄 Итерация {iteration_id} завершена: "
                            f"найдено {len(signals)} сигналов: {signals}")

                # Ожидаем до следующей итерации
                iteration_duration = (datetime.now() - iteration_start).total_seconds()
                wait_time = max(0, interval_minutes * 60 - iteration_duration)

                if wait_time > 0:
                    logger.info(f"⏳ Следующая итерация через {wait_time / 60:.1f} минут")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"⚠️ Итерация заняла больше времени чем интервал! "
                                   f"Запускаем следующую немедленно")
                    await asyncio.sleep(min_wait_seconds)  # Минимальная пауза из конфига

        except asyncio.CancelledError:
            logger.info("🛑 Непрерывный анализ остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка в непрерывном анализе: {e}")
            raise

    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("🧹 Очистка ресурсов оркестратора")

        # Очищаем кэш префильтра
        self.prefilter._checked_symbols.clear()

        # Сбрасываем текущую сессию
        self._current_session = None

        logger.info("✅ Ресурсы очищены")


# Экспорт для импорта в другие модули
__all__ = ['AnalysisOrchestrator', 'AnalysisSession']