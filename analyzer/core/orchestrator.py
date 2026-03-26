# analyzer/core/orchestrator.py (ПОЛНОСТЬЮ)
"""
🎯 ОРКЕСТРАТОР - главный координатор всей системы анализа
ФАЗА 1.3.6:
- Интеграция WebSocket для раннего входа
- Поддержка WATCH статуса и M15 сигналов
- Упрощена логика (только WATCH и M15)

HOTFIX 1.3.6.2:
- Добавлена проверка has_active_m15 перед созданием M15 сигнала
- Улучшена валидация символов
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

from .prefilter_liquidity import LiquidityPrefilter, PrefilterResult
from .three_screen_analyzer import ThreeScreenAnalyzer
from .event_bus import EventType, event_bus
from .data_classes import ThreeScreenAnalysis
from .websocket_client import BybitWebSocketClient
from analyzer.core.signal_repository import signal_repository

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
    watch_signals: int = 0
    prefilter_result: Optional[PrefilterResult] = None
    analysis_results: Dict[str, ThreeScreenAnalysis] = field(default_factory=dict)
    status: str = "running"  # running, completed, failed
    duplicates_skipped: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_symbols": self.total_symbols,
            "analyzed_symbols": self.analyzed_symbols,
            "signals_found": self.signals_found,
            "watch_signals": self.watch_signals,
            "status": self.status,
            "prefilter_stats": self.prefilter_result.to_dict() if self.prefilter_result else None,
            "symbols_analyzed": list(self.analysis_results.keys()),
            "duplicates_skipped": self.duplicates_skipped
        }


class AnalysisOrchestrator:
    """
    Главный координатор для всей системы анализа
    Управляет процессом: префильтр → проверка дубликатов → трёхэкранный анализ → WATCH/M15
    ФАЗА 1.3.6: WebSocket интеграция, только WATCH и M15
    """

    def __init__(self, api_client, config=None):
        self.api = api_client
        self.config = config or {}

        # Получаем параметры оркестрации из конфига
        analysis_config = self.config.get('analysis', {})
        self.orchestration_config = analysis_config.get('orchestration', {})
        self.caching_config = analysis_config.get('caching', {})

        # Настройки для проверки дубликатов
        self.signal_types_config = analysis_config.get('signal_types', {})
        self.watch_config = self.signal_types_config.get('watch', {})
        self.m15_config = self.signal_types_config.get('m15', {})

        # Время жизни для проверки дубликатов (часы)
        self.duplicate_check_hours = {
            'WATCH': self.watch_config.get('expiration_hours', 3),
            'M15': self.m15_config.get('expiration_hours', 3)
        }

        # Инициализируем модули
        self.prefilter = LiquidityPrefilter(api_client, self.config)
        self.three_screen_analyzer = ThreeScreenAnalyzer(api_client, self.config)

        # WebSocket
        self.websocket: Optional[BybitWebSocketClient] = None

        # Сессии анализа
        self._sessions: Dict[str, AnalysisSession] = {}
        self._current_session: Optional[AnalysisSession] = None

        # Статистика
        self._total_analyses = 0
        self._total_signals = 0
        self._total_watch = 0
        self._duplicates_skipped = 0
        self._rejected_signals = 0

        logger.info("✅ AnalysisOrchestrator создан (Фаза 1.3.6)")
        logger.info(f"   Проверка дубликатов: WATCH={self.duplicate_check_hours['WATCH']}ч, M15={self.duplicate_check_hours['M15']}ч")

    async def initialize(self) -> bool:
        """Инициализация всех модулей"""
        logger.info("🚀 Инициализация оркестратора и модулей")

        try:
            # Инициализируем анализаторы
            three_screen_init = await self.three_screen_analyzer.initialize()

            if not three_screen_init:
                logger.error("❌ Не удалось инициализировать ThreeScreenAnalyzer")
                return False

            # ✅ НОВОЕ: Инициализация WebSocket
            logger.info("🔌 Инициализация WebSocket клиента...")
            self.websocket = BybitWebSocketClient()
            self.websocket.on_price_update(self._on_price_update)
            asyncio.create_task(self.websocket.connect())
            logger.info("✅ WebSocket клиент запущен")

            logger.info("✅ Все модули инициализированы")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации оркестратора: {e}")
            return False

    def create_session(self, symbols: List[str]) -> AnalysisSession:
        """Создание новой сессии анализа"""
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        log_symbols_limit = self.orchestration_config.get('log_symbols_limit', 10)

        session = AnalysisSession(
            session_id=session_id,
            start_time=datetime.now(),
            total_symbols=len(symbols)
        )

        self._sessions[session_id] = session
        self._current_session = session

        logger.info(f"📁 Создана сессия {session_id} для {len(symbols)} символов")

        asyncio.create_task(event_bus.publish(EventType.SESSION_STARTED, {
            "session_id": session_id,
            "symbol_count": len(symbols),
            "symbols": symbols[:log_symbols_limit]
        }, source="orchestrator"))

        return session

    async def _check_duplicate_before_analysis(
            self,
            symbol: str,
            signal_subtype: str
    ) -> bool:
        """
        Проверка дубликата ДО анализа

        Args:
            symbol: Символ монеты
            signal_subtype: Тип сигнала (WATCH/M15)

        Returns:
            True если есть активный дубликат, False если можно анализировать
        """
        try:
            expiration_hours = self.duplicate_check_hours.get(signal_subtype, 3)

            is_duplicate = await signal_repository.check_duplicate_signal(
                symbol, signal_subtype, expiration_hours
            )

            if is_duplicate:
                logger.debug(f"⏭️ Пропускаем {symbol} ({signal_subtype}) - есть активный дубликат")
                return True

            return False

        except Exception as e:
            logger.error(f"❌ Ошибка проверки дубликата для {symbol}: {e}")
            return False

    async def analyze_symbols_batch(self, symbols: List[str],
                                    max_concurrent: int = None) -> Dict[str, ThreeScreenAnalysis]:
        """
        Анализ пачки символов с префильтром и проверкой дубликатов ДО анализа
        Генерирует WATCH сигналы для монет, прошедших Screen2 (4-5 условий)

        Args:
            symbols: Список символов для анализа
            max_concurrent: Максимальное количество одновременных анализов (None = из конфига)

        Returns:
            Словарь с результатами анализа {symbol: ThreeScreenAnalysis}
        """
        logger.info(f"🚀 Начинаем анализ пачки из {len(symbols)} символов")

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

            # ШАГ 1.5: Проверка дубликатов ДО анализа (только для WATCH)
            logger.info("🔍 ШАГ 1.5: Проверка активных WATCH дубликатов...")

            symbols_to_analyze = []
            duplicate_skipped_symbols = []

            for symbol in filtered_symbols:
                if await self._check_duplicate_before_analysis(symbol, 'WATCH'):
                    duplicate_skipped_symbols.append(symbol)
                else:
                    symbols_to_analyze.append(symbol)

            session.duplicates_skipped = len(duplicate_skipped_symbols)
            self._duplicates_skipped += len(duplicate_skipped_symbols)

            logger.info(f"✅ Проверка дубликатов: {len(filtered_symbols)} → {len(symbols_to_analyze)} символов "
                        f"(пропущено WATCH дубликатов: {len(duplicate_skipped_symbols)})")

            if not symbols_to_analyze:
                logger.info("❌ Нет символов для анализа после проверки дубликатов")
                session.status = "completed"
                session.end_time = datetime.now()
                return {}

            # ШАГ 2: ТРЁХЭКРАННЫЙ АНАЛИЗ ОТФИЛЬТРОВАННЫХ СИМВОЛОВ
            logger.info(f"📊 ШАГ 2: Запускаем трёхэкранный анализ для {len(symbols_to_analyze)} символов...")

            semaphore = asyncio.Semaphore(max_concurrent)
            results = {}

            async def analyze_with_semaphore(symbol: str):
                async with semaphore:
                    return await self._analyze_single_symbol(symbol, session)

            tasks = [analyze_with_semaphore(symbol) for symbol in symbols_to_analyze]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Собираем результаты
            for i, result in enumerate(batch_results):
                symbol = symbols_to_analyze[i]

                if isinstance(result, Exception):
                    logger.error(f"❌ Ошибка анализа {symbol}: {result}")
                    continue

                if result:
                    results[symbol] = result
                    session.analysis_results[symbol] = result

                    # Для WATCH сигналов (screen2 прошёл)
                    if result.screen2 and result.screen2.passed:
                        # Проверяем, нужно ли создавать WATCH
                        screen2_score = getattr(result.screen2, 'screen2_score', 0)
                        if screen2_score >= 4:
                            # Сохраняем WATCH сигнал
                            signal_id = await signal_repository.save_watch_signal(
                                symbol=symbol,
                                direction="BUY" if result.screen1.trend_direction == "BULL" else "SELL",
                                zone_low=result.screen2.zone_low,
                                zone_high=result.screen2.zone_high,
                                screen2_score=screen2_score,
                                expected_pattern=result.screen2.expected_pattern,
                                expiration_hours=self.watch_config.get('expiration_hours', 3)
                            )
                            if signal_id:
                                session.watch_signals += 1
                                self._total_watch += 1
                                logger.info(f"👀 WATCH сигнал для {symbol} сохранён (score={screen2_score})")

                                # Добавляем символ в WebSocket подписку
                                if self.websocket:
                                    self.websocket.add_symbols([symbol])

                    # Для M15 сигналов (screen3 прошёл и есть паттерн)
                    if result.should_trade and result.screen3 and result.screen3.passed:
                        session.signals_found += 1
                        self._total_signals += 1
                        logger.info(f"✅ M15 сигнал для {symbol} найден")

            # Обновляем статистику сессии
            session.analyzed_symbols = len(results)
            session.status = "completed"
            session.end_time = datetime.now()

            # Обновляем глобальную статистику
            self._total_analyses += len(results)

            # Логируем итоги
            logger.info(f"🎯 АНАЛИЗ ЗАВЕРШЕН: "
                        f"Проанализировано: {session.analyzed_symbols}/{len(symbols_to_analyze)} "
                        f"WATCH сигналов: {session.watch_signals} "
                        f"M15 сигналов: {session.signals_found} "
                        f"Пропущено дубликатов: {session.duplicates_skipped} "
                        f"Время: {(session.end_time - session.start_time).total_seconds():.1f} сек")

            asyncio.create_task(event_bus.publish(EventType.SESSION_COMPLETED, {
                "session_id": session.session_id,
                "total_symbols": session.total_symbols,
                "analyzed_symbols": session.analyzed_symbols,
                "watch_signals": session.watch_signals,
                "signals_found": session.signals_found,
                "duplicates_skipped": session.duplicates_skipped,
                "execution_time_seconds": (session.end_time - session.start_time).total_seconds()
            }, source="orchestrator"))

            return results

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при анализе пачки: {e}")
            session.status = "failed"
            session.end_time = datetime.now()

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
            if analysis.should_trade and analysis.screen3 and analysis.screen3.passed:
                rr = analysis.screen3.indicators.get('risk_reward_ratio', 0)
                logger.info(f"✅ {symbol}: M15 СИГНАЛ НАЙДЕН! "
                            f"{analysis.screen3.signal_type} @ {analysis.screen3.entry_price:.2f} "
                            f"(R/R: {rr:.2f}:1, паттерн: {analysis.screen3.trigger_pattern})")
            elif analysis.screen2 and analysis.screen2.passed:
                score = getattr(analysis.screen2, 'screen2_score', 0)
                logger.info(f"👀 {symbol}: WATCH СИГНАЛ (score={score}/5, зона: {analysis.screen2.zone_low:.4f}-{analysis.screen2.zone_high:.4f})")
            else:
                if analysis.screen3 and analysis.screen3.rejection_reason:
                    logger.debug(f"❌ {symbol}: сигнал отклонён - {analysis.screen3.rejection_reason}")
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

    async def _on_price_update(self, symbol: str, price: float):
        """
        Обработка обновления цены от WebSocket

        ✅ НОВОЕ для Фазы 1.3.6
        ✅ HOTFIX 1.3.6.2: добавлена проверка has_active_m15
        """
        try:
            # ✅ ПРОВЕРКА: есть ли уже активный M15 сигнал для этого символа?
            if await signal_repository.has_active_m15(symbol):
                logger.debug(f"⏭️ {symbol}: уже есть активный M15 сигнал, пропускаем")
                return

            # Проверяем, есть ли монета в WATCH
            watch_symbols = await signal_repository.get_watch_symbols()

            if symbol in watch_symbols:
                logger.debug(f"📊 WATCH монета {symbol}: цена {price:.6f}")

                # Проверяем паттерны на текущей цене
                await self._check_patterns_for_watch(symbol, price)

        except Exception as e:
            logger.error(f"❌ Ошибка обработки цены {symbol}: {e}")

    async def _check_patterns_for_watch(self, symbol: str, current_price: float):
        """
        Проверка паттернов для WATCH монеты

        ✅ НОВОЕ для Фазы 1.3.6
        ✅ HOTFIX 1.3.6.2: повторная проверка has_active_m15 перед созданием сигнала
        """
        try:
            # Получаем WATCH сигнал из БД
            watch_signal = await signal_repository.get_watch_signal(symbol)
            if not watch_signal:
                return

            # Проверяем, достигнута ли зона
            zone_low = watch_signal.get('zone_low', 0)
            zone_high = watch_signal.get('zone_high', 0)

            if zone_low <= current_price <= zone_high:
                logger.info(f"🎯 {symbol}: цена {current_price:.6f} вошла в зону {zone_low:.6f}-{zone_high:.6f}")

                # ✅ ПОВТОРНАЯ ПРОВЕРКА: убеждаемся, что за это время не появился M15 сигнал
                if await signal_repository.has_active_m15(symbol):
                    logger.info(f"⏭️ {symbol}: M15 сигнал уже создан (гонка условий), пропускаем")
                    return

                # Получаем свечи для анализа паттернов
                klines_data = await self._get_klines_for_watch(symbol)
                if not klines_data:
                    logger.warning(f"⚠️ Не удалось получить свечи для {symbol}")
                    return

                # Проверяем паттерны
                patterns = self.three_screen_analyzer.screen3_analyzer._find_chart_patterns_m15(
                    klines_data.get('15m', []),
                    watch_signal.get('trend_direction', 'BULL')
                )

                if patterns:
                    # Паттерн найден - генерируем M15 сигнал
                    pattern = patterns[0]
                    logger.info(f"✅ {symbol}: обнаружен паттерн {pattern.get('type')}")

                    # Рассчитываем ATR
                    m15_klines = klines_data.get('15m', [])
                    if len(m15_klines) < 20:
                        logger.warning(f"⚠️ Недостаточно M15 данных для {symbol}")
                        return

                    highs = [float(k[2]) for k in m15_klines]
                    lows = [float(k[3]) for k in m15_klines]
                    closes = [float(k[4]) for k in m15_klines]

                    atr = self.three_screen_analyzer.screen3_analyzer._calculate_atr(
                        highs, lows, closes, entry_price=current_price
                    )

                    # Определяем направление
                    direction = watch_signal.get('direction', 'BUY')
                    signal_type = "BUY" if direction == 'BUY' else "SELL"

                    # Рассчитываем SL
                    stop_loss = self.three_screen_analyzer.screen3_analyzer._calculate_stop_loss(
                        entry_price=current_price,
                        signal_type=signal_type,
                        atr=atr
                    )

                    if stop_loss is None:
                        logger.warning(f"⚠️ Не удалось рассчитать SL для {symbol}")
                        return

                    # Рассчитываем TP (R/R ≥ 3:1)
                    risk = abs(current_price - stop_loss)
                    reward = risk * 3.0
                    if signal_type == "BUY":
                        take_profit = current_price + reward
                    else:
                        take_profit = current_price - reward

                    take_profit = self.three_screen_analyzer.screen3_analyzer._round_price(take_profit)
                    stop_loss = self.three_screen_analyzer.screen3_analyzer._round_price(stop_loss)

                    # ✅ ФИНАЛЬНАЯ ПРОВЕРКА: ещё раз убеждаемся, что нет активного M15
                    if await signal_repository.has_active_m15(symbol):
                        logger.info(f"⏭️ {symbol}: M15 сигнал уже создан (финальная проверка), пропускаем")
                        return

                    # Обновляем WATCH → ACTIVE
                    signal_id = await signal_repository.update_watch_to_active(
                        symbol=symbol,
                        entry_price=current_price,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        trigger_pattern=pattern.get('type', 'UNKNOWN'),
                        expiration_hours=self.m15_config.get('expiration_hours', 3)
                    )

                    if signal_id:
                        logger.info(f"✅ {symbol}: WATCH → ACTIVE (ID={signal_id})")

                        # Публикуем событие для открытия позиции
                        await event_bus.publish(
                            EventType.TRADING_SIGNAL_GENERATED,
                            {
                                'signal_id': signal_id,
                                'symbol': symbol,
                                'signal_type': signal_type,
                                'entry_price': current_price,
                                'stop_loss': stop_loss,
                                'take_profit': take_profit,
                                'signal_subtype': 'M15',
                                'order_type': 'MARKET',
                                'expiration_time': (datetime.now() + timedelta(hours=3)).isoformat(),
                                'confidence': pattern.get('confidence', 0.7)
                            },
                            'orchestrator'
                        )

        except Exception as e:
            logger.error(f"❌ Ошибка проверки паттернов для {symbol}: {e}")

    async def _get_klines_for_watch(self, symbol: str) -> Dict[str, List]:
        """
        Получение свечей для WATCH монеты
        """
        try:
            # Получаем последние свечи для M15 и M5
            m15_klines = await self.api.get_klines(symbol, "15m", limit=50)
            m5_klines = await self.api.get_klines(symbol, "5m", limit=50)

            return {
                '15m': m15_klines,
                '5m': m5_klines
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения свечей для {symbol}: {e}")
            return {}

    def get_session(self, session_id: str) -> Optional[AnalysisSession]:
        """Получение информации о сессии"""
        return self._sessions.get(session_id)

    def get_all_sessions(self) -> List[AnalysisSession]:
        """Получение всех сессий"""
        return list(self._sessions.values())

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики оркестратора"""
        ws_stats = self.websocket.get_stats() if self.websocket else {}
        return {
            "total_sessions": len(self._sessions),
            "total_analyses": self._total_analyses,
            "total_watch": self._total_watch,
            "total_signals": self._total_signals,
            "duplicates_skipped": self._duplicates_skipped,
            "rejected_signals": self._rejected_signals,
            "success_rate": (self._total_signals / self._total_analyses * 100
                             if self._total_analyses > 0 else 0),
            "current_session": self._current_session.session_id if self._current_session else None,
            "prefilter_cache_stats": self.prefilter.get_cache_stats(),
            "websocket_stats": ws_stats
        }

    async def run_continuous_analysis(self, symbols: List[str],
                                      interval_minutes: int = None,
                                      max_concurrent: int = None):
        """
        Непрерывный анализ символов с заданным интервалом
        """
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

                results = await self.analyze_symbols_batch(symbols, max_concurrent)

                signals = [s for s, a in results.items() if a and a.should_trade]
                watch = [s for s, a in results.items() if a and a.screen2 and a.screen2.passed and getattr(a.screen2, 'screen2_score', 0) >= 4]

                logger.info(f"🔄 Итерация {iteration_id} завершена: "
                            f"WATCH: {len(watch)}, M15: {len(signals)}")

                iteration_duration = (datetime.now() - iteration_start).total_seconds()
                wait_time = max(0, interval_minutes * 60 - iteration_duration)

                if wait_time > 0:
                    logger.info(f"⏳ Следующая итерация через {wait_time / 60:.1f} минут")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"⚠️ Итерация заняла больше времени чем интервал! "
                                   f"Запускаем следующую немедленно")
                    await asyncio.sleep(min_wait_seconds)

        except asyncio.CancelledError:
            logger.info("🛑 Непрерывный анализ остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка в непрерывном анализе: {e}")
            raise

    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("🧹 Очистка ресурсов оркестратора")

        # Закрываем WebSocket
        if self.websocket:
            await self.websocket.close()

        self.prefilter._checked_symbols.clear()
        self._current_session = None
        self._duplicates_skipped = 0
        self._rejected_signals = 0

        logger.info("✅ Ресурсы очищены")


__all__ = ['AnalysisOrchestrator', 'AnalysisSession']