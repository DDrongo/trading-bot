# analyzer/core/orchestrator.py (ПОЛНОСТЬЮ - ФАЗА 1.5.2)
"""
🎯 ОРКЕСТРАТОР - главный координатор всей системы анализа

ФАЗА 1.3.8:
- Исправлена обработка исключений
- Добавлена точка входа
- ИСПРАВЛЕНО: WATCH резервирование (передача параметров)
- ИСПРАВЛЕНО: единое время через time_utils

ФАЗА 1.5.0:
- Добавлена поддержка режима Light (trading_mode: light)
- Ветвление между LightTrader и ThreeScreenAnalyzer

ФАЗА 1.5.1:
- Добавлено кэширование списка ликвидных монет (TTL = 1 час)

ФАЗА 1.4.0:
- Восстановление Pro режима
- Добавлено восстановление WATCH сигналов при старте

ФАЗА 1.5.2:
- 🆕 Автоматический сбор исторических уровней для ликвидных монет
- 🆕 Проверка наличия уровней и блокирующая загрузка при отсутствии
- 🆕 WATCH-сигналы сохраняются с обучающим комментарием
- 🆕 Метод _generate_watch_comment для генерации комментариев
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

from .prefilter_liquidity import LiquidityPrefilter, PrefilterResult
from .three_screen_analyzer import ThreeScreenAnalyzer
from .event_bus import EventType, event_bus
from .data_classes import ThreeScreenAnalysis
from .websocket_client import BybitWebSocketClient
from analyzer.core.signal_repository import signal_repository
from analyzer.core.data_provider import data_provider
from analyzer.core.time_utils import now, utc_now, format_local, to_local

logger = logging.getLogger('orchestrator')


@dataclass
class AnalysisSession:
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_symbols: int = 0
    analyzed_symbols: int = 0
    signals_found: int = 0
    watch_signals: int = 0
    prefilter_result: Optional[PrefilterResult] = None
    analysis_results: Dict[str, ThreeScreenAnalysis] = field(default_factory=dict)
    status: str = "running"
    duplicates_skipped: int = 0
    levels_collected: int = 0

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
            "duplicates_skipped": self.duplicates_skipped,
            "levels_collected": self.levels_collected
        }


class AnalysisOrchestrator:

    def __init__(self, config=None, data_provider_instance=None):
        self.config = config or {}
        self.data_provider = data_provider_instance or data_provider

        analysis_config = self.config.get('analysis', {})
        self.orchestration_config = analysis_config.get('orchestration', {})
        self.caching_config = analysis_config.get('caching', {})

        self.signal_types_config = analysis_config.get('signal_types', {})
        self.watch_config = self.signal_types_config.get('watch', {})
        self.m15_config = self.signal_types_config.get('m15', {})

        self.duplicate_check_hours = {
            'WATCH': self.watch_config.get('expiration_hours', 3),
            'M15': self.m15_config.get('expiration_hours', 3)
        }

        self.trading_mode = self.config.get('trading_mode', 'pro')
        logger.info(f"🎯 Режим торговли: {self.trading_mode.upper()}")

        self.prefilter = LiquidityPrefilter(self.data_provider, self.config)
        self.three_screen_analyzer = ThreeScreenAnalyzer(self.config, self.data_provider)

        if self.trading_mode == 'light':
            from .light_trader import LightTrader
            self.light_trader = LightTrader(self.config, self.data_provider)
            logger.info("✅ LightTrader инициализирован")
        else:
            self.light_trader = None

        self.websocket: Optional[BybitWebSocketClient] = None

        self._sessions: Dict[str, AnalysisSession] = {}
        self._current_session: Optional[AnalysisSession] = None

        self._total_analyses = 0
        self._total_signals = 0
        self._total_watch = 0
        self._duplicates_skipped = 0
        self._rejected_signals = 0
        self._levels_collected_total = 0

        self._liquid_symbols_cache: List[str] = []
        self._liquid_symbols_cache_time: Optional[datetime] = None
        self._liquid_symbols_cache_ttl = 3600

        self._levels_checked_cache: Dict[str, bool] = {}

        logger.info("✅ AnalysisOrchestrator создан (Фаза 1.5.2)")
        logger.info(
            f"   Проверка дубликатов: WATCH={self.duplicate_check_hours['WATCH']}ч, M15={self.duplicate_check_hours['M15']}ч")
        logger.info(f"   Режим торговли: {self.trading_mode.upper()}")
        logger.info(f"   Кэш ликвидных монет: TTL={self._liquid_symbols_cache_ttl} сек")
        logger.info(f"   🆕 Автосбор исторических уровней: ВКЛЮЧЕН")
        logger.info(f"   🆕 WATCH комментарии: ВКЛЮЧЕНЫ")

    # ========== АВТОМАТИЧЕСКИЙ СБОР ИСТОРИЧЕСКИХ УРОВНЕЙ ==========

    async def _ensure_historical_levels(self, symbol: str, wait: bool = True) -> bool:
        try:
            from analyzer.core.historical_levels import historical_levels, LevelStrength

            levels = await historical_levels.get_historical_levels(symbol, LevelStrength.STRONG)

            if levels:
                logger.debug(f"✅ {symbol}: исторические уровни уже есть ({len(levels)} шт.)")
                return True

            logger.info(f"🔄 {symbol}: исторических уровней нет, запускаем сбор (БЛОКИРУЮЩИЙ)...")
            logger.info(f"📊 [{symbol}] Это займёт ~30-60 секунд...")

            results = await historical_levels.collect_and_save_all([symbol])
            saved = results.get(symbol, 0)

            if saved > 0:
                logger.info(f"✅ {symbol}: собрано {saved} исторических уровней, продолжаем анализ")
                return True
            else:
                logger.warning(f"⚠️ {symbol}: не удалось собрать уровни, продолжаем с H4")
                return False

        except ImportError:
            logger.debug(f"⚠️ historical_levels не импортирован")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка сбора уровней {symbol}: {e}")
            return False

    async def initialize(self) -> bool:
        logger.info("🚀 Инициализация оркестратора")

        try:
            three_screen_init = await self.three_screen_analyzer.initialize()
            if not three_screen_init:
                logger.error("❌ Не удалось инициализировать ThreeScreenAnalyzer")
                return False

            try:
                from analyzer.core.historical_levels import historical_levels
                await historical_levels.initialize()
                logger.info("✅ HistoricalLevelsCollector инициализирован")
            except ImportError:
                logger.warning("⚠️ historical_levels не найден, автосбор отключен")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось инициализировать historical_levels: {e}")

            logger.info("🔌 Инициализация WebSocket клиента...")
            self.websocket = BybitWebSocketClient()
            self.websocket.on_price_update(self._on_price_update)
            asyncio.create_task(self.websocket.connect())
            logger.info("✅ WebSocket клиент запущен")

            await self._restore_watch_signals()

            logger.info("✅ Все модули инициализированы")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return False

    async def _restore_watch_signals(self):
        try:
            watch_signals = await signal_repository.get_watch_signals_with_reserve()

            if not watch_signals:
                logger.info("📭 Нет WATCH сигналов для восстановления")
                return

            symbols = list(set([w['symbol'] for w in watch_signals]))
            if self.websocket:
                self.websocket.add_symbols(symbols)
                logger.info(f"🔄 Восстановлена подписка на {len(symbols)} WATCH символов: {symbols[:5]}...")

            logger.info(f"✅ Восстановлено {len(watch_signals)} WATCH сигналов")

        except Exception as e:
            logger.error(f"❌ Ошибка восстановления WATCH сигналов: {e}")

    async def _get_liquid_symbols(self, all_symbols: List[str]) -> List[str]:
        current_time = datetime.now()

        if (self._liquid_symbols_cache_time and
                (current_time - self._liquid_symbols_cache_time).total_seconds() < self._liquid_symbols_cache_ttl):
            logger.debug(f"♻️ Используем кэш ликвидных символов: {len(self._liquid_symbols_cache)} шт")
            return self._liquid_symbols_cache.copy()

        logger.info("🔄 Обновление списка ликвидных символов...")

        usdt_symbols = [s for s in all_symbols if s.endswith("USDT")]
        max_symbols_per_cycle = self.config.get('analysis', {}).get('max_symbols_per_cycle', 50)
        symbols_to_check = usdt_symbols[:max_symbols_per_cycle]

        prefilter_result = await self.prefilter.filter_symbols(symbols_to_check)

        self._liquid_symbols_cache = prefilter_result.filtered_symbols
        self._liquid_symbols_cache_time = current_time

        logger.info(
            f"✅ Ликвидных символов: {len(self._liquid_symbols_cache)} (обновлено, TTL={self._liquid_symbols_cache_ttl} сек)")

        return self._liquid_symbols_cache.copy()

    def create_session(self, symbols: List[str]) -> AnalysisSession:
        session_id = f"session_{now().strftime('%Y%m%d_%H%M%S')}"
        log_symbols_limit = self.orchestration_config.get('log_symbols_limit', 10)

        session = AnalysisSession(
            session_id=session_id,
            start_time=now(),
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

    async def _check_duplicate_before_analysis(self, symbol: str, signal_subtype: str) -> bool:
        try:
            expiration_hours = self.duplicate_check_hours.get(signal_subtype, 3)
            is_duplicate = await signal_repository.check_duplicate_signal(symbol, signal_subtype, expiration_hours)
            if is_duplicate:
                logger.info(f"⏭️ Пропускаем {symbol} ({signal_subtype}) - есть активный дубликат")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка проверки дубликата для {symbol}: {e}")
            return False

    async def analyze_symbols_batch(self, symbols: List[str], max_concurrent: int = None) -> Dict[
        str, ThreeScreenAnalysis]:
        logger.info(f"🚀 Начинаем анализ пачки из {len(symbols)} символов (режим: {self.trading_mode.upper()})")

        if max_concurrent is None:
            max_concurrent = self.orchestration_config.get('max_concurrent_analysis', 5)

        session = self.create_session(symbols)

        try:
            if self.trading_mode == 'light':
                filtered_symbols = symbols
                logger.info(f"📊 Light режим: анализируем {len(filtered_symbols)} ликвидных символов")
            else:
                logger.info("🔍 ШАГ 1: Префильтрация по ликвидности...")
                prefilter_result = await self.prefilter.filter_symbols(symbols)
                session.prefilter_result = prefilter_result

                if not prefilter_result.filtered_symbols:
                    logger.warning("❌ Префильтр не пропустил ни одного символа")
                    session.status = "completed"
                    session.end_time = now()
                    return {}

                filtered_symbols = prefilter_result.filtered_symbols
                logger.info(f"✅ Префильтр: {len(symbols)} → {len(filtered_symbols)} символов")

            logger.info("🔍 ШАГ 1.5: Проверка исторических уровней...")
            levels_checked = 0
            levels_missing = 0

            for symbol in filtered_symbols:
                has_levels = await self._ensure_historical_levels(symbol)
                levels_checked += 1
                if not has_levels:
                    levels_missing += 1

            session.levels_collected = levels_missing
            logger.info(f"✅ Проверка уровней: {levels_checked} символов, у {levels_missing} запущен сбор")

            if self.trading_mode == 'light':
                symbols_to_analyze = filtered_symbols
                duplicate_skipped_symbols = []
                logger.debug(f"📊 Light режим: анализируем все {len(symbols_to_analyze)} символов")
            else:
                logger.info("🔍 ШАГ 2: Проверка активных WATCH дубликатов...")
                symbols_to_analyze = []
                duplicate_skipped_symbols = []

                for symbol in filtered_symbols:
                    if await self._check_duplicate_before_analysis(symbol, 'WATCH'):
                        duplicate_skipped_symbols.append(symbol)
                    else:
                        symbols_to_analyze.append(symbol)

                session.duplicates_skipped = len(duplicate_skipped_symbols)
                self._duplicates_skipped += len(duplicate_skipped_symbols)
                logger.info(f"✅ Проверка дубликатов: {len(filtered_symbols)} → {len(symbols_to_analyze)} символов")

            if not symbols_to_analyze:
                logger.info("❌ Нет символов для анализа после проверки дубликатов")
                session.status = "completed"
                session.end_time = now()
                return {}

            logger.info(
                f"📊 ШАГ 3: Запускаем анализ для {len(symbols_to_analyze)} символов (режим: {self.trading_mode.upper()})...")

            semaphore = asyncio.Semaphore(max_concurrent)
            results = {}

            async def analyze_with_semaphore(symbol: str):
                async with semaphore:
                    return await self._analyze_single_symbol(symbol, session)

            tasks = [analyze_with_semaphore(symbol) for symbol in symbols_to_analyze]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(batch_results):
                symbol = symbols_to_analyze[i]

                if isinstance(result, Exception):
                    logger.error(f"❌ Ошибка анализа {symbol}: {result}")
                    continue

                if result is None:
                    continue

                if not isinstance(result, ThreeScreenAnalysis):
                    logger.error(f"❌ Неожиданный тип результата для {symbol}: {type(result)}")
                    continue

                results[symbol] = result
                session.analysis_results[symbol] = result

                if self.trading_mode != 'light':
                    if result.screen2 and result.screen2.passed:
                        screen2_score = getattr(result.screen2, 'screen2_score', 0)
                        if screen2_score >= 3:
                            current_price = result.screen3.entry_price if result.screen3 else 0
                            if current_price == 0:
                                current_price = await self.data_provider.get_current_price(symbol)

                            pos_config = self.config.get('position_management', {})
                            risk_per_trade_pct = pos_config.get('position_sizing', {}).get('risk_per_trade_pct', 2.0)
                            leverage = self.config.get('paper_trading', {}).get('leverage', 10)
                            balance = 10000.0

                            margin_target = balance * (risk_per_trade_pct / 100.0)
                            position_value = margin_target * leverage
                            position_size = position_value / current_price if current_price > 0 else 0.001
                            position_size = max(0.001, min(1000000.0, position_size))

                            learning_comment = self.three_screen_analyzer._generate_full_analysis_description(
                                symbol=symbol,
                                screen1=result.screen1,
                                screen2=result.screen2,
                                screen3=result.screen3 if result.screen3 else None
                            )

                            # ========== ПОЛУЧАЕМ ДАННЫЕ ДЛЯ SNIPER ==========
                            entry_type = getattr(result.screen2, 'entry_type', 'LEGACY')

                            # Данные о выбранном FVG
                            selected_fvg = getattr(result.screen2, 'selected_fvg', None)
                            fvg_type = selected_fvg.get('type', '') if selected_fvg else ''
                            fvg_formed_at = selected_fvg.get('formed_at', None)
                            if fvg_formed_at:
                                if hasattr(fvg_formed_at, 'isoformat'):
                                    fvg_formed_at = fvg_formed_at.isoformat()
                                else:
                                    fvg_formed_at = str(fvg_formed_at)
                            fvg_age = selected_fvg.get('age', 0) if selected_fvg else 0
                            fvg_strength = selected_fvg.get('strength', '') if selected_fvg else ''

                            # Данные о выбранном пуле
                            selected_pool = getattr(result.screen2, 'selected_liquidity_pool', None)
                            selected_pool_price = selected_pool.get('price', 0) if selected_pool else 0
                            selected_pool_touches = selected_pool.get('touches', 0) if selected_pool else 0

                            # Все пулы в JSON
                            liquidity_pools = getattr(result.screen2, 'liquidity_pools', [])
                            import json
                            liquidity_pools_json = json.dumps(liquidity_pools) if liquidity_pools else None

                            signal_id = await signal_repository.save_watch_signal(
                                symbol=symbol,
                                direction="BUY" if result.screen1.trend_direction == "BULL" else "SELL",
                                zone_low=result.screen2.zone_low,
                                zone_high=result.screen2.zone_high,
                                screen2_score=screen2_score,
                                expected_pattern=result.screen2.expected_pattern,
                                expiration_hours=self.watch_config.get('expiration_hours', 8),
                                position_size=position_size,
                                entry_price=current_price,
                                leverage=leverage,
                                current_price=current_price,
                                learning_comment=learning_comment,
                                entry_type=entry_type,
                                fvg_type=fvg_type,
                                fvg_formed_at=fvg_formed_at,
                                fvg_age=fvg_age,
                                fvg_strength=fvg_strength,
                                liquidity_pools=liquidity_pools_json,
                                selected_pool_price=selected_pool_price,
                                selected_pool_touches=selected_pool_touches
                            )
                            if signal_id:
                                session.watch_signals += 1
                                self._total_watch += 1
                                logger.info(f"👀 WATCH сигнал для {symbol} сохранён (score={screen2_score})")
                                logger.info(f"📚 Полный комментарий сохранён ({len(learning_comment)} символов)")

                                if self.websocket:
                                    self.websocket.add_symbols([symbol])

                if result.should_trade and result.screen3 and result.screen3.passed:
                    session.signals_found += 1
                    self._total_signals += 1
                    logger.info(f"✅ Сигнал для {symbol} найден")

            session.analyzed_symbols = len(results)
            session.status = "completed"
            session.end_time = now()
            self._total_analyses += len(results)

            logger.info(f"🎯 АНАЛИЗ ЗАВЕРШЕН: "
                        f"Проанализировано: {session.analyzed_symbols}/{len(symbols_to_analyze)} "
                        f"Уровней собрано: {session.levels_collected} "
                        f"WATCH сигналов: {session.watch_signals} "
                        f"Сигналов: {session.signals_found} "
                        f"Время: {(session.end_time - session.start_time).total_seconds():.1f} сек")

            asyncio.create_task(event_bus.publish(EventType.SESSION_COMPLETED, {
                "session_id": session.session_id,
                "total_symbols": session.total_symbols,
                "analyzed_symbols": session.analyzed_symbols,
                "watch_signals": session.watch_signals,
                "signals_found": session.signals_found,
                "duplicates_skipped": session.duplicates_skipped,
                "levels_collected": session.levels_collected,
                "execution_time_seconds": (session.end_time - session.start_time).total_seconds(),
                "trading_mode": self.trading_mode
            }, source="orchestrator"))

            return results

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при анализе пачки: {e}")
            session.status = "failed"
            session.end_time = now()

            asyncio.create_task(event_bus.publish(EventType.ERROR_OCCURRED, {
                "error": str(e),
                "session_id": session.session_id,
                "function": "analyze_symbols_batch"
            }, source="orchestrator"))

            return {}

    async def _analyze_single_symbol(self, symbol: str, session: AnalysisSession) -> Optional[ThreeScreenAnalysis]:
        logger.debug(f"🔍 Анализ символа {symbol} (режим: {self.trading_mode.upper()})")

        try:
            if await signal_repository.was_traded_recently(symbol, minutes=30):
                logger.info(f"⏭️ {symbol}: была сделка в последние 30 минут, пропускаем анализ")
                return None

            if await signal_repository.has_active_m15(symbol):
                logger.info(f"⏭️ {symbol}: уже есть активный M15 сигнал, пропускаем анализ")
                return None

            if self.trading_mode == 'light' and self.light_trader:
                analysis = await self.light_trader.analyze_symbol(symbol)
            else:
                analysis = await self.three_screen_analyzer.analyze_symbol(symbol)

            if not analysis:
                logger.debug(f"❌ {symbol}: анализ не удался")
                return None

            if analysis.should_trade and analysis.screen3 and analysis.screen3.passed:
                rr = analysis.screen3.indicators.get('risk_reward_ratio', 0)
                logger.info(f"✅ {symbol}: СИГНАЛ НАЙДЕН! "
                            f"{analysis.screen3.signal_type} @ {analysis.screen3.entry_price:.2f} "
                            f"(R/R: {rr:.2f}:1, паттерн: {analysis.screen3.trigger_pattern}, режим: {self.trading_mode.upper()})")
            elif analysis.screen2 and analysis.screen2.passed:
                score = getattr(analysis.screen2, 'screen2_score', 0)
                logger.info(
                    f"👀 {symbol}: WATCH СИГНАЛ (score={score}/5, зона: {analysis.screen2.zone_low:.4f}-{analysis.screen2.zone_high:.4f})")
            else:
                if analysis.screen3 and analysis.screen3.rejection_reason:
                    logger.debug(f"❌ {symbol}: сигнал отклонён - {analysis.screen3.rejection_reason}")

            return analysis

        except Exception as e:
            logger.error(f"❌ Ошибка анализа символа {symbol}: {e}")
            return None

    async def analyze_single_symbol(self, symbol: str) -> Optional[ThreeScreenAnalysis]:
        logger.info(f"🔍 Анализ одиночного символа: {symbol} (режим: {self.trading_mode.upper()})")

        liquidity_ok = await self.prefilter.quick_check(symbol)
        if not liquidity_ok:
            logger.warning(f"❌ {symbol} не прошел проверку ликвидности")
            return None

        await self._ensure_historical_levels(symbol)

        temp_session = AnalysisSession(
            session_id=f"single_{symbol}_{now().strftime('%Y%m%d_%H%M%S')}",
            start_time=now(),
            total_symbols=1
        )
        return await self._analyze_single_symbol(symbol, temp_session)

    async def _on_price_update(self, symbol: str, price: float):
        try:
            if await signal_repository.has_active_m15(symbol):
                return

            watch_symbols = await signal_repository.get_watch_symbols()
            if symbol in watch_symbols:
                await self._check_patterns_for_watch(symbol, price)

        except Exception as e:
            logger.error(f"❌ Ошибка обработки цены {symbol}: {e}")

    async def _check_patterns_for_watch(self, symbol: str, current_price: float):
        try:
            watch_signal = await signal_repository.get_watch_signal(symbol)
            if not watch_signal:
                return

            zone_low = watch_signal.get('zone_low', 0)
            zone_high = watch_signal.get('zone_high', 0)

            if zone_low <= current_price <= zone_high:
                logger.info(f"🎯 {symbol}: цена {current_price:.6f} вошла в зону")

                if await signal_repository.has_active_m15(symbol):
                    return

                klines_data = await self._get_klines_for_watch(symbol)
                if not klines_data:
                    return

                patterns = self.three_screen_analyzer.screen3_analyzer._find_chart_patterns_m15(
                    klines_data.get('15m', []),
                    watch_signal.get('trend_direction', 'BULL')
                )

                if patterns:
                    pattern = patterns[0]
                    logger.info(f"✅ {symbol}: обнаружен паттерн {pattern.get('type')}")

                    m15_klines = klines_data.get('15m', [])
                    if len(m15_klines) < 20:
                        return

                    highs = [float(k[2]) for k in m15_klines]
                    lows = [float(k[3]) for k in m15_klines]
                    closes = [float(k[4]) for k in m15_klines]

                    atr = self.three_screen_analyzer.screen3_analyzer._calculate_atr(
                        highs, lows, closes, entry_price=current_price
                    )

                    direction = watch_signal.get('direction', 'BUY')
                    signal_type = "BUY" if direction == 'BUY' else "SELL"

                    stop_loss = self.three_screen_analyzer.screen3_analyzer._calculate_stop_loss(
                        entry_price=current_price, signal_type=signal_type, atr=atr
                    )

                    if stop_loss is None:
                        return

                    risk = abs(current_price - stop_loss)
                    reward = risk * 3.0
                    if signal_type == "BUY":
                        take_profit = current_price + reward
                    else:
                        take_profit = current_price - reward

                    take_profit = self.three_screen_analyzer.screen3_analyzer._round_price(take_profit)
                    stop_loss = self.three_screen_analyzer.screen3_analyzer._round_price(stop_loss)

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
                                'expiration_time': (utc_now() + timedelta(hours=3)).isoformat(),
                                'confidence': pattern.get('confidence', 0.7)
                            },
                            'orchestrator'
                        )

        except Exception as e:
            logger.error(f"❌ Ошибка проверки паттернов для {symbol}: {e}")

    async def _get_klines_for_watch(self, symbol: str) -> Dict[str, List]:
        try:
            m15_klines = await self.data_provider.get_klines(symbol, "15m", limit=50)
            m5_klines = await self.data_provider.get_klines(symbol, "5m", limit=50)
            return {'15m': m15_klines, '5m': m5_klines}
        except Exception as e:
            logger.error(f"❌ Ошибка получения свечей для {symbol}: {e}")
            return {}

    def _generate_watch_comment(self, symbol: str, screen1, screen2, current_price: float) -> str:
        """Генерация обучающего комментария для WATCH-сигнала (ФАЗА 1.5.2)"""

        def fmt(p):
            if p is None or p == 0:
                return "-"
            if p < 0.01:
                return f"{p:.6f}"
            elif p < 0.1:
                return f"{p:.5f}"
            elif p < 1:
                return f"{p:.4f}"
            elif p < 10:
                return f"{p:.3f}"
            elif p < 100:
                return f"{p:.2f}"
            else:
                return f"{p:.2f}"

        zone_low = screen2.zone_low
        zone_high = screen2.zone_high

        if current_price > zone_high:
            diff_pct = (current_price - zone_high) / zone_high * 100
            position = f"▲ ВЫШЕ зоны на {diff_pct:.1f}%"
            position_status = "⏳ Ждём снижения цены в зону"
        elif current_price < zone_low:
            diff_pct = (zone_low - current_price) / zone_low * 100
            position = f"▼ НИЖЕ зоны на {diff_pct:.1f}%"
            position_status = "⏳ Ждём роста цены в зону"
        else:
            position = "● В ЗОНЕ"
            position_status = "✅ Цена в зоне! Ожидаем паттерн на M15"

        d1_trend = screen1.trend_direction
        d1_adx = screen1.indicators.get('adx', 0)
        d1_confidence = screen1.confidence_score

        if d1_adx > 25:
            strength = "СИЛЬНЫЙ"
        elif d1_adx > 20:
            strength = "УМЕРЕННЫЙ"
        else:
            strength = "СЛАБЫЙ"

        hist_used = getattr(screen2, 'historical_levels_used', 0)
        if hist_used > 0:
            source_desc = f"ИСТОРИЧЕСКИЙ ({hist_used} уровней W1/D1)"
        else:
            source_desc = "H4 (локальный)"

        comment = f"""
═══════════════════════════════════════════════════════════════
📊 WATCH АНАЛИЗ {symbol}
═══════════════════════════════════════════════════════════════

🎯 D1 ТРЕНД
───────────────────────────────────────────────────────────────
  Направление:     {d1_trend}
  Сила:            {strength} (ADX = {d1_adx:.1f})
  Уверенность:     {d1_confidence:.1%}

🎯 ЗОНА ВХОДА (Screen2)
───────────────────────────────────────────────────────────────
  Нижняя граница:  {fmt(zone_low)}
  Верхняя граница: {fmt(zone_high)}
  Score:           {screen2.screen2_score}/8
  Источник:        {source_desc}
  Ожидаемый паттерн: {screen2.expected_pattern}

📍 ТЕКУЩАЯ ПОЗИЦИЯ
───────────────────────────────────────────────────────────────
  Текущая цена:    {fmt(current_price)}
  Позиция:         {position}
  Статус:          {position_status}

⏱ ВРЕМЕННАЯ ШКАЛА
───────────────────────────────────────────────────────────────
  Сигнал создан:   {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}
  Истекает через:  3 часа

  💡 При входе цены в зону и появлении паттерна на M15
     будет автоматически открыта позиция.

═══════════════════════════════════════════════════════════════
"""
        return comment

    def get_session(self, session_id: str) -> Optional[AnalysisSession]:
        return self._sessions.get(session_id)

    def get_all_sessions(self) -> List[AnalysisSession]:
        return list(self._sessions.values())

    def get_stats(self) -> Dict[str, Any]:
        ws_stats = self.websocket.get_stats() if self.websocket else {}
        return {
            "trading_mode": self.trading_mode,
            "total_sessions": len(self._sessions),
            "total_analyses": self._total_analyses,
            "total_watch": self._total_watch,
            "total_signals": self._total_signals,
            "duplicates_skipped": self._duplicates_skipped,
            "rejected_signals": self._rejected_signals,
            "levels_collected_total": self._levels_collected_total,
            "success_rate": (self._total_signals / self._total_analyses * 100) if self._total_analyses > 0 else 0,
            "current_session": self._current_session.session_id if self._current_session else None,
            "prefilter_cache_stats": self.prefilter.get_cache_stats(),
            "websocket_stats": ws_stats,
            "liquid_symbols_cache_size": len(self._liquid_symbols_cache),
            "liquid_symbols_cache_ttl": self._liquid_symbols_cache_ttl,
            "levels_checked_cache_size": len(self._levels_checked_cache)
        }

    async def run_continuous_analysis(self, symbols: List[str] = None, interval_minutes: int = None,
                                      max_concurrent: int = None):
        if interval_minutes is None:
            interval_minutes = self.orchestration_config.get('continuous_analysis_interval', 15)
        if max_concurrent is None:
            max_concurrent = self.orchestration_config.get('continuous_max_concurrent', 3)

        min_wait_seconds = self.caching_config.get('min_wait_seconds', 1)

        logger.info(
            f"🔄 Запуск непрерывного анализа каждые {interval_minutes} минут (режим: {self.trading_mode.upper()})")
        logger.info(f"📚 Исторические уровни будут собираться автоматически для новых монет")

        try:
            while True:
                iteration_start = now()
                iteration_id = iteration_start.strftime("%Y%m%d_%H%M")

                logger.info(f"🔄 Итерация {iteration_id}: начинаем анализ")

                if symbols is None:
                    all_symbols = await self.data_provider.get_all_symbols()
                    symbols_to_analyze = await self._get_liquid_symbols(all_symbols)
                else:
                    symbols_to_analyze = symbols

                logger.info(f"🔍 Анализ {len(symbols_to_analyze)} монет...")

                results = await self.analyze_symbols_batch(symbols_to_analyze, max_concurrent)

                signals_found = sum(1 for a in results.values() if a and a.should_trade)
                if signals_found > 0:
                    logger.info(f"🎯 НАЙДЕНО {signals_found} СИГНАЛОВ!")
                else:
                    logger.info(f"💤 Сигналы не найдены")

                iteration_duration = (now() - iteration_start).total_seconds()
                wait_time = max(0, interval_minutes * 60 - iteration_duration)

                if wait_time > 0:
                    logger.info(f"⏳ Следующая итерация через {wait_time / 60:.1f} минут")
                    await asyncio.sleep(wait_time)
                else:
                    logger.warning(f"⚠️ Итерация заняла больше времени чем интервал!")
                    await asyncio.sleep(min_wait_seconds)

        except asyncio.CancelledError:
            logger.info("🛑 Непрерывный анализ остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка в непрерывном анализе: {e}")
            raise

    async def cleanup(self):
        logger.info("🧹 Очистка ресурсов оркестратора")
        if self.websocket:
            await self.websocket.close()
        self.prefilter.clear_cache()
        self._liquid_symbols_cache.clear()
        self._liquid_symbols_cache_time = None
        self._levels_checked_cache.clear()
        self._current_session = None
        self._duplicates_skipped = 0
        self._rejected_signals = 0
        logger.info("✅ Ресурсы очищены")


if __name__ == "__main__":
    import yaml
    from pathlib import Path

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    config_path = Path(__file__).parent.parent / "config/config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)


    async def main():
        orchestrator = AnalysisOrchestrator(config)
        if not await orchestrator.initialize():
            print("❌ Не удалось инициализировать оркестратор")
            return
        symbols = config.get('analysis', {}).get('symbols', ['BTCUSDT', 'ETHUSDT'])
        print(f"🚀 Запуск анализа для {len(symbols)} символов в режиме {orchestrator.trading_mode.upper()}...")
        results = await orchestrator.analyze_symbols_batch(symbols[:5])
        print(f"\n✅ Анализ завершён. Найдено сигналов: {len([r for r in results.values() if r.should_trade])}")
        await orchestrator.cleanup()


    asyncio.run(main())

__all__ = ['AnalysisOrchestrator', 'AnalysisSession']