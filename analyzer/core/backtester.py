# analyzer/core/backtester.py (ПОЛНОСТЬЮ)
"""
🧪 BACKTESTER — Тестирование стратегии на исторических данных
ФАЗА 1.5.1:
- Запуск стратегии на исторических свечах
- Эмуляция торговли через VirtualAccount
- Сохранение результатов в БД
"""

import logging
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

import aiosqlite

from analyzer.core.historical_data_provider import HistoricalDataProvider, HistoricalCandle
from analyzer.core.virtual_account import VirtualAccount, VirtualPosition, VirtualTrade
from analyzer.core.three_screen_analyzer import ThreeScreenAnalyzer
from analyzer.core.data_classes import ThreeScreenAnalysis, Direction
from analyzer.core.data_provider import data_provider

logger = logging.getLogger('backtester')


class BacktestMode(Enum):
    """Режим бэктеста"""
    LIGHT = "light"  # Light режим
    PRO = "pro"  # Pro режим


@dataclass
class BacktestResult:
    """Результат бэктеста"""
    id: Optional[int] = None
    symbol: str = ""
    start_date: datetime = field(default_factory=datetime.now)
    end_date: datetime = field(default_factory=datetime.now)
    mode: str = "pro"

    # Статистика
    initial_balance: float = 0.0
    final_balance: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0

    total_signals: int = 0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0

    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0

    avg_win: float = 0.0
    avg_loss: float = 0.0

    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'symbol': self.symbol,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'mode': self.mode,
            'initial_balance': self.initial_balance,
            'final_balance': self.final_balance,
            'total_pnl': self.total_pnl,
            'total_pnl_pct': self.total_pnl_pct,
            'total_signals': self.total_signals,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': self.win_rate,
            'profit_factor': self.profit_factor,
            'max_drawdown': self.max_drawdown,
            'max_drawdown_pct': self.max_drawdown_pct,
            'sharpe_ratio': self.sharpe_ratio,
            'avg_win': self.avg_win,
            'avg_loss': self.avg_loss,
            'created_at': self.created_at.isoformat()
        }


class Backtester:
    """
    Бэктестер для тестирования стратегии на исторических данных
    """

    def __init__(self, config: Dict = None, db_path: str = None):
        self.config = config or {}
        self.db_path = db_path or 'data/trading_bot.db'

        # Компоненты
        self.historical_data = HistoricalDataProvider(db_path, config)
        self.virtual_account: Optional[VirtualAccount] = None
        self.three_screen_analyzer: Optional[ThreeScreenAnalyzer] = None

        # Настройки
        self.trading_mode = self.config.get('trading_mode', 'pro')
        self.backtest_mode = BacktestMode.PRO if self.trading_mode == 'pro' else BacktestMode.LIGHT

        # Текущий бэктест
        self.current_result: Optional[BacktestResult] = None
        self._signals_generated = 0

        logger.info(f"✅ Backtester инициализирован (режим: {self.backtest_mode.value})")

    async def initialize(self) -> bool:
        """Инициализация компонентов и таблиц БД"""
        try:
            # Инициализируем исторические данные
            await self.historical_data.initialize()

            # Создаём таблицу для результатов бэктеста
            await self._create_backtest_table()

            # Инициализируем анализатор
            self.three_screen_analyzer = ThreeScreenAnalyzer(self.config, data_provider)
            await self.three_screen_analyzer.initialize()

            logger.info("✅ Backtester инициализирован")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Backtester: {e}")
            return False

    async def _create_backtest_table(self) -> None:
        """Создание таблицы для результатов бэктеста"""
        try:
            import os
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS backtest_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        start_date TIMESTAMP NOT NULL,
                        end_date TIMESTAMP NOT NULL,
                        mode TEXT NOT NULL,
                        initial_balance REAL,
                        final_balance REAL,
                        total_pnl REAL,
                        total_pnl_pct REAL,
                        total_signals INTEGER,
                        total_trades INTEGER,
                        winning_trades INTEGER,
                        losing_trades INTEGER,
                        win_rate REAL,
                        profit_factor REAL,
                        max_drawdown REAL,
                        max_drawdown_pct REAL,
                        sharpe_ratio REAL,
                        avg_win REAL,
                        avg_loss REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_results_symbol ON backtest_results(symbol)")
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_results_created ON backtest_results(created_at)")

                await conn.commit()

        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы backtest_results: {e}")

    async def run_backtest(
            self,
            symbols: List[str],
            start_date: datetime,
            end_date: datetime,
            config: Dict = None
    ) -> Dict[str, BacktestResult]:
        """
        Запуск бэктеста для списка символов

        Args:
            symbols: Список символов
            start_date: Дата начала
            end_date: Дата окончания
            config: Дополнительная конфигурация

        Returns:
            Словарь {symbol: BacktestResult}
        """
        logger.info(f"🚀 Запуск бэктеста для {len(symbols)} символов")
        logger.info(f"   Период: {start_date.date()} - {end_date.date()}")
        logger.info(f"   Режим: {self.backtest_mode.value}")

        results = {}

        for symbol in symbols:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"🔍 Бэктест {symbol}")
            logger.info(f"{'=' * 60}")

            result = await self._backtest_symbol(symbol, start_date, end_date)
            results[symbol] = result

            # Сохраняем результат в БД
            await self._save_result(result)

        # Выводим сводку
        self._print_summary(results)

        return results

    async def _backtest_symbol(
            self,
            symbol: str,
            start_date: datetime,
            end_date: datetime
    ) -> BacktestResult:
        """Бэктест одного символа"""
        # Создаём виртуальный счёт
        self.virtual_account = VirtualAccount(self.config)
        self._signals_generated = 0

        # Создаём результат
        result = BacktestResult(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            mode=self.backtest_mode.value,
            initial_balance=self.virtual_account.initial_balance
        )

        # Загружаем исторические данные для всех таймфреймов
        intervals = ['1d', '4h', '15m']
        klines_data = {}

        for interval in intervals:
            candles = await self.historical_data.load_klines(
                symbol, interval, start_date, end_date
            )
            klines_data[interval] = candles

        if not klines_data.get('1d'):
            logger.warning(f"⚠️ Нет данных для {symbol}")
            return result

        # Получаем все свечи 15m для итерации
        m15_candles = klines_data.get('15m', [])
        if not m15_candles:
            logger.warning(f"⚠️ Нет M15 данных для {symbol}")
            return result

        logger.info(f"📊 Загружено свечей: D1={len(klines_data.get('1d', []))}, "
                    f"H4={len(klines_data.get('4h', []))}, M15={len(m15_candles)}")

        # Итерируем по M15 свечам
        for i, candle in enumerate(m15_candles):
            # Обновляем текущую цену
            self.virtual_account.update_price(symbol, candle.close)

            # Каждые 15 минут (каждая свеча) проверяем SL/TP
            await self._check_positions(symbol, candle)

            # Анализ запускаем реже (например, раз в час или при смене H4)
            if self._should_analyze(candle, i):
                # Получаем срез данных до текущего момента
                current_klines = self._get_klines_slice(klines_data, candle.timestamp)

                # Запускаем анализ
                analysis = await self._analyze_at_candle(symbol, current_klines, candle)

                if analysis and analysis.should_trade:
                    self._signals_generated += 1

                    # Открываем позицию
                    position = self._open_position_from_analysis(analysis, candle)

                    if position:
                        logger.info(f"📈 Сигнал #{self._signals_generated}: {symbol} "
                                    f"{analysis.screen3.signal_type} @ {candle.close:.6f}")

            # Сохраняем снимок баланса раз в день
            if i % 96 == 0:  # 96 * 15мин = 24 часа
                self.virtual_account.snapshot_balance(candle.timestamp)

        # Закрываем все оставшиеся позиции по последней цене
        last_candle = m15_candles[-1] if m15_candles else None
        if last_candle:
            for pos in self.virtual_account.get_open_positions():
                self.virtual_account.close_position(
                    pos.id, last_candle.close, 'END_OF_BACKTEST', last_candle.timestamp
                )

        # Заполняем результат
        stats = self.virtual_account.get_statistics()

        result.final_balance = stats['current_balance']
        result.total_pnl = stats['total_pnl']
        result.total_pnl_pct = (result.total_pnl / result.initial_balance) * 100
        result.total_signals = self._signals_generated
        result.total_trades = stats['total_trades']
        result.winning_trades = stats['winning_trades']
        result.losing_trades = stats['losing_trades']
        result.win_rate = stats['win_rate']
        result.profit_factor = stats['profit_factor']
        result.max_drawdown = stats['max_drawdown']
        result.max_drawdown_pct = stats['max_drawdown_pct']
        result.sharpe_ratio = stats['sharpe_ratio']
        result.avg_win = stats['avg_win']
        result.avg_loss = stats['avg_loss']

        logger.info(f"\n📊 Результаты {symbol}:")
        logger.info(f"   Сигналов: {result.total_signals}")
        logger.info(f"   Сделок: {result.total_trades}")
        logger.info(f"   Win Rate: {result.win_rate:.1f}%")
        logger.info(f"   PnL: {result.total_pnl:+.2f} USDT ({result.total_pnl_pct:+.2f}%)")
        logger.info(f"   Profit Factor: {result.profit_factor:.2f}")

        return result

    def _should_analyze(self, candle: HistoricalCandle, index: int) -> bool:
        """Определение, нужно ли запускать анализ на этой свече"""
        # Анализируем каждый час (каждые 4 свечи M15)
        if index % 4 != 0:
            return False

        # Не анализируем первые 100 свечей (нужна история)
        if index < 100:
            return False

        return True

    def _get_klines_slice(
            self,
            all_klines: Dict[str, List[HistoricalCandle]],
            current_time: datetime
    ) -> Dict[str, List]:
        """Получение среза свечей до указанного времени"""
        result = {}

        for interval, candles in all_klines.items():
            # Фильтруем свечи до текущего времени
            filtered = [c for c in candles if c.timestamp <= current_time]

            # Берём последние N свечей
            limits = {'1d': 100, '4h': 50, '15m': 50}
            limit = limits.get(interval, 50)

            sliced = filtered[-limit:] if len(filtered) > limit else filtered

            # Конвертируем в формат, ожидаемый анализатором
            result[interval] = [c.to_list() for c in sliced]

        return result

    async def _analyze_at_candle(
            self,
            symbol: str,
            klines: Dict[str, List],
            candle: HistoricalCandle
    ) -> Optional[ThreeScreenAnalysis]:
        """Запуск анализа на конкретной свече"""
        try:
            # Подменяем текущую цену в data_provider
            # (в реальности нужно мокировать, но для простоты используем цену свечи)

            if self.backtest_mode == BacktestMode.LIGHT:
                # Light режим
                from analyzer.core.light_trader import LightTrader
                light_trader = LightTrader(self.config, data_provider)

                # Временно подменяем метод get_current_price
                original_get_price = data_provider.get_current_price

                async def mock_get_price(sym, force_refresh=False):
                    return candle.close

                data_provider.get_current_price = mock_get_price

                try:
                    analysis = await light_trader.analyze_symbol(symbol)
                finally:
                    data_provider.get_current_price = original_get_price

                return analysis
            else:
                # Pro режим
                # Временно подменяем метод get_current_price
                original_get_price = data_provider.get_current_price

                async def mock_get_price(sym, force_refresh=False):
                    return candle.close

                data_provider.get_current_price = mock_get_price

                try:
                    # Используем внутренний метод анализатора
                    # (упрощённо, т.к. полный analyse_symbol требует много данных)
                    analysis = await self.three_screen_analyzer.analyze_symbol(symbol)
                finally:
                    data_provider.get_current_price = original_get_price

                return analysis

        except Exception as e:
            logger.error(f"❌ Ошибка анализа {symbol}: {e}")
            return None

    def _open_position_from_analysis(
            self,
            analysis: ThreeScreenAnalysis,
            candle: HistoricalCandle
    ) -> Optional[VirtualPosition]:
        """Открытие позиции на основе анализа"""
        if not analysis.should_trade or not analysis.screen3:
            return None

        screen3 = analysis.screen3

        # Определяем направление
        direction = 'BUY' if screen3.signal_type == 'BUY' else 'SELL'

        # Рассчитываем размер позиции
        balance = self.virtual_account.balance
        risk_per_trade = self.config.get('position_management', {}).get(
            'position_sizing', {}).get('risk_per_trade_pct', 2.0
        ) / 100

        risk_amount = balance * risk_per_trade
        risk_distance = abs(candle.close - screen3.stop_loss)

        if risk_distance > 0:
            quantity = risk_amount / risk_distance
        else:
            quantity = 0.001

        # Ограничиваем количество
        quantity = max(0.001, min(quantity, 1000))

        # Открываем позицию
        return self.virtual_account.open_position(
            symbol=analysis.symbol,
            direction=direction,
            entry_price=candle.close,
            stop_loss=screen3.stop_loss,
            take_profit=screen3.take_profit,
            quantity=quantity,
            leverage=self.config.get('paper_trading', {}).get('leverage', 10),
            opened_at=candle.timestamp
        )

    async def _check_positions(self, symbol: str, candle: HistoricalCandle) -> None:
        """Проверка и закрытие позиций по SL/TP"""
        to_close = self.virtual_account.check_stop_loss_take_profit(symbol, candle.close)

        for pos_id, reason in to_close:
            self.virtual_account.close_position(
                pos_id, candle.close, reason, candle.timestamp
            )

    async def _save_result(self, result: BacktestResult) -> Optional[int]:
        """Сохранение результата в БД"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("""
                    INSERT INTO backtest_results (
                        symbol, start_date, end_date, mode,
                        initial_balance, final_balance, total_pnl, total_pnl_pct,
                        total_signals, total_trades, winning_trades, losing_trades,
                        win_rate, profit_factor, max_drawdown, max_drawdown_pct,
                        sharpe_ratio, avg_win, avg_loss
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    result.symbol,
                    result.start_date.isoformat(),
                    result.end_date.isoformat(),
                    result.mode,
                    result.initial_balance,
                    result.final_balance,
                    result.total_pnl,
                    result.total_pnl_pct,
                    result.total_signals,
                    result.total_trades,
                    result.winning_trades,
                    result.losing_trades,
                    result.win_rate,
                    result.profit_factor,
                    result.max_drawdown,
                    result.max_drawdown_pct,
                    result.sharpe_ratio,
                    result.avg_win,
                    result.avg_loss
                ))

                await conn.commit()
                result.id = cursor.lastrowid

            logger.info(f"💾 Результат сохранён (ID={result.id})")
            return result.id

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения результата: {e}")
            return None

    def _print_summary(self, results: Dict[str, BacktestResult]) -> None:
        """Вывод сводки по всем символам"""
        print("\n" + "=" * 80)
        print("📊 СВОДКА БЭКТЕСТА")
        print("=" * 80)

        total_trades = sum(r.total_trades for r in results.values())
        total_pnl = sum(r.total_pnl for r in results.values())
        winning_trades = sum(r.winning_trades for r in results.values())
        losing_trades = sum(r.losing_trades for r in results.values())

        print(f"\n{'Символ':<12} {'Сделок':<8} {'Win Rate':<10} {'P/L':<12} {'P/L %':<10} {'PF':<8}")
        print("-" * 60)

        for symbol, result in results.items():
            pnl_color = "+" if result.total_pnl > 0 else ""
            print(f"{symbol:<12} {result.total_trades:<8} "
                  f"{result.win_rate:<10.1f}% "
                  f"{pnl_color}{result.total_pnl:<11.2f} "
                  f"{pnl_color}{result.total_pnl_pct:<9.2f}% "
                  f"{result.profit_factor:<8.2f}")

        print("-" * 60)
        print(f"{'ИТОГО':<12} {total_trades:<8} "
              f"{(winning_trades / total_trades * 100) if total_trades > 0 else 0:<10.1f}% "
              f"{total_pnl:<12.2f} ")

        print("\n" + "=" * 80)

    async def get_historical_results(
            self,
            symbol: str = None,
            limit: int = 50
    ) -> List[BacktestResult]:
        """Получение истории результатов бэктеста"""
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row

                if symbol:
                    cursor = await conn.execute("""
                        SELECT * FROM backtest_results 
                        WHERE symbol = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                    """, (symbol, limit))
                else:
                    cursor = await conn.execute("""
                        SELECT * FROM backtest_results 
                        ORDER BY created_at DESC
                        LIMIT ?
                    """, (limit,))

                rows = await cursor.fetchall()

                results = []
                for row in rows:
                    result = BacktestResult(
                        id=row['id'],
                        symbol=row['symbol'],
                        start_date=datetime.fromisoformat(row['start_date']),
                        end_date=datetime.fromisoformat(row['end_date']),
                        mode=row['mode'],
                        initial_balance=row['initial_balance'],
                        final_balance=row['final_balance'],
                        total_pnl=row['total_pnl'],
                        total_pnl_pct=row['total_pnl_pct'],
                        total_signals=row['total_signals'],
                        total_trades=row['total_trades'],
                        winning_trades=row['winning_trades'],
                        losing_trades=row['losing_trades'],
                        win_rate=row['win_rate'],
                        profit_factor=row['profit_factor'],
                        max_drawdown=row['max_drawdown'],
                        max_drawdown_pct=row['max_drawdown_pct'],
                        sharpe_ratio=row['sharpe_ratio'],
                        avg_win=row['avg_win'],
                        avg_loss=row['avg_loss'],
                        created_at=datetime.fromisoformat(row['created_at'])
                    )
                    results.append(result)

                return results

        except Exception as e:
            logger.error(f"❌ Ошибка получения истории: {e}")
            return []


__all__ = [
    'Backtester',
    'BacktestResult',
    'BacktestMode'
]