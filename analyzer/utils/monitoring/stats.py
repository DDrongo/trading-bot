#!/usr/bin/env python3
# analyzer/utils/monitoring/stats.py
"""
📊 STATS - Статистика и аналитика
Запуск: python -m analyzer.utils.monitoring.stats
"""

import sys
import asyncio
import logging
import csv
import json
import zipfile
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from analyzer.utils.monitoring.lib import MonitorBase, TableBuilder
from analyzer.core.signal_repository import signal_repository
from analyzer.core.trade_repository import trade_repository
from analyzer.core.paper_account import PaperAccount
from analyzer.core.data_provider import data_provider
from analyzer.core.websocket_client import BybitWebSocketClient
from analyzer.core.time_utils import now, utc_now, to_local, format_local, TIMEZONE_OFFSET

logger = logging.getLogger('stats')


class Statistik(MonitorBase):
    """Статистика и аналитика"""

    def __init__(self):
        super().__init__(color_enabled=True)
        self.signal_repo = signal_repository
        self.trade_repo = trade_repository
        self.paper_account = PaperAccount({})
        self.data_provider = data_provider
        self.websocket = BybitWebSocketClient()

        import yaml
        config_path = Path(__file__).parent.parent.parent / 'config/config.yaml'
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.timezone_offset = self.config.get('display', {}).get('timezone_offset', TIMEZONE_OFFSET)
        self.starting_balance = self.config.get('paper_trading', {}).get('starting_virtual_balance', 10000.0)

        self.table_builder = TableBuilder()
        self._price_cache: Dict[str, float] = {}
        self._price_cache_ttl = 5

    def utc_to_local(self, utc_str: str) -> datetime:
        if not utc_str:
            return now()
        try:
            if '+' in utc_str or 'Z' in utc_str:
                dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(utc_str)
            return dt + timedelta(hours=self.timezone_offset)
        except Exception:
            return now()

    def format_time(self, timestamp_str: str) -> str:
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%H:%M")
        except:
            return timestamp_str[11:16] if ':' in timestamp_str else '-'

    def format_date(self, timestamp_str: str) -> str:
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%m-%d")
        except:
            return timestamp_str[:10] if '-' in timestamp_str else '-'

    def format_datetime(self, timestamp_str: str) -> str:
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%d.%m.%Y %H:%M:%S")
        except:
            return timestamp_str

    async def get_current_price(self, symbol: str) -> Optional[float]:
        if self.websocket:
            try:
                price = self.websocket.get_latest_price(symbol)
                if price and price > 0:
                    return price
            except Exception:
                pass

        cache_key = f"price_{symbol}"
        if cache_key in self._price_cache:
            cached_price, cached_time = self._price_cache[cache_key]
            if (datetime.now() - cached_time).total_seconds() < self._price_cache_ttl:
                return cached_price

        if self.data_provider:
            try:
                price = await self.data_provider.get_current_price(symbol)
                if price:
                    self._price_cache[cache_key] = (price, datetime.now())
                return price
            except Exception:
                pass
        return None

    async def get_account_state(self) -> Dict[str, Any]:
        state = {
            'initial_balance': self.starting_balance,
            'current_balance': self.starting_balance,
            'used_margin': 0.0,
            'reserved_for_watch': 0.0,
            'available': self.starting_balance,
            'total_pnl': 0.0
        }

        try:
            db_pnl = 0
            if self.trade_repo:
                closed_trades = await self.trade_repo.get_closed_trades(limit=1000)
                db_pnl = sum(t.get('pnl', 0) for t in closed_trades)
                state['total_pnl'] = db_pnl

            state['current_balance'] = self.starting_balance + db_pnl

            if self.paper_account:
                await self.paper_account.cleanup_expired_reservations()
                positions = await self.paper_account.get_open_positions()
                for pos in positions.values():
                    state['used_margin'] += pos.margin

            if self.signal_repo:
                watch_signals = await self.signal_repo.get_watch_signals_with_reserve()
                for watch in watch_signals:
                    reserved = watch.get('reserved_margin')
                    if reserved is not None:
                        state['reserved_for_watch'] += float(reserved)

            state['available'] = state['current_balance'] - state['used_margin'] - state['reserved_for_watch']

        except Exception as e:
            logger.error(f"Ошибка: {e}")

        return state

    async def display_signal_details(self, signal_id: int):
        """Детальная информация о сигнале"""
        try:
            if not self.signal_repo:
                print(f"{self.Fore.RED}❌ SignalRepository не инициализирован{self.Style.RESET_ALL}")
                return

            signal = await self.signal_repo.get_signal_by_id(signal_id)
            if not signal:
                print(f"{self.Fore.RED}❌ Сигнал #{signal_id} не найден{self.Style.RESET_ALL}")
                input(f"\n{self.Fore.GREEN}Нажмите Enter для возврата...{self.Style.RESET_ALL}")
                return

            current_price = await self.get_current_price(signal.get('symbol', ''))
            if not current_price:
                current_price = signal.get('entry_price', 0)

            trade = None
            if self.trade_repo:
                trade = await self.trade_repo.get_trade_by_signal_id(signal_id)

            self.clear_screen()

            symbol = signal.get('symbol', 'N/A')
            direction = signal.get('direction', '')
            status = signal.get('status', '')
            entry_type = signal.get('entry_type', 'LEGACY')
            zone_low = signal.get('zone_low', 0)
            zone_high = signal.get('zone_high', 0)
            screen2_score = signal.get('screen2_score', 0)
            expected_pattern = signal.get('expected_pattern', '-')
            trigger_pattern = signal.get('trigger_pattern', '-')
            entry_price = signal.get('entry_price', 0)
            stop_loss = signal.get('stop_loss', 0)
            take_profit = signal.get('take_profit', 0)
            rr_ratio = signal.get('risk_reward_ratio', 0)

            grab_price = signal.get('grab_price', None)
            grab_time = signal.get('grab_time', None)
            grab_timeframe = signal.get('grab_timeframe', 'M15')
            fvg_zones = signal.get('fvg_zones', [])

            import json
            liquidity_pools_raw = signal.get('liquidity_pools', '[]')
            if isinstance(liquidity_pools_raw, str):
                liquidity_pools = json.loads(liquidity_pools_raw) if liquidity_pools_raw else []
            else:
                liquidity_pools = liquidity_pools_raw

            quantity = 0
            margin = 0
            position_value = 0
            leverage = signal.get('leverage', 10)
            commission_open = 0
            commission_close = 0

            if trade:
                quantity = trade.get('quantity', 0)
                margin = trade.get('margin', 0)
                position_value = trade.get('position_value', 0)
                commission_open = trade.get('commission_open', 0)
                commission_close = trade.get('commission_close', 0)
            elif status == 'ACTIVE':
                position_size = signal.get('position_size', 0)
                if entry_price > 0 and position_size > 0:
                    quantity = position_size
                    position_value = quantity * entry_price
                    margin = position_value / leverage

            position_multiplier = 1.0 if entry_type == 'SNIPER' else 0.75 if entry_type == 'TREND' else 0.5

            print(f"{self.Fore.CYAN}{self.Style.BRIGHT}{'═' * 90}{self.Style.RESET_ALL}")
            if status == 'ACTIVE':
                print(
                    f"{self.Fore.CYAN}{self.Style.BRIGHT}{' ' * 30}ПОЗИЦИЯ #{signal_id}: {symbol} [{entry_type}] — АКТИВНА{self.Style.RESET_ALL}")
            elif status == 'CLOSED':
                print(
                    f"{self.Fore.CYAN}{self.Style.BRIGHT}{' ' * 30}ПОЗИЦИЯ #{signal_id}: {symbol} [{entry_type}] — ЗАКРЫТА{self.Style.RESET_ALL}")
            else:
                print(
                    f"{self.Fore.CYAN}{self.Style.BRIGHT}{' ' * 30}АНАЛИЗ {symbol} [{entry_type}]{self.Style.RESET_ALL}")
            print(f"{self.Fore.CYAN}{self.Style.BRIGHT}{'═' * 90}{self.Style.RESET_ALL}\n")

            if status == 'ACTIVE':
                print(f"{self.Fore.YELLOW}💰 ТОРГОВЫЕ ПАРАМЕТРЫ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  Направление:        {self.format_direction(direction)}")
                print(f"  Entry price:        {self.format_price(entry_price)}")
                if signal.get('created_time'):
                    print(f"  Время входа:        {self.format_datetime(signal.get('created_time'))}")
                print(f"  Stop Loss:          {self.format_price(stop_loss)}")
                print(f"  Take Profit:        {self.format_price(take_profit)}")
                print()

                print(f"{self.Fore.YELLOW}📈 ПОЗИЦИЯ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  Размер позиции:     {position_multiplier:.0%} (от стандарта)")
                print(f"  Маржа (залог):      {margin:.2f} USDT")
                print(f"  Стоимость позиции:  {position_value:.2f} USDT (с плечом {leverage:.0f}x)")
                print(f"  Плечо:              {leverage:.0f}x")
                print(f"  Количество:         {quantity:.4f} {symbol}")
                print()

                print(f"{self.Fore.YELLOW}📊 СТАТУС ПОЗИЦИИ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                if current_price > 0 and entry_price > 0:
                    if direction == 'BUY':
                        pnl = (current_price - entry_price) * quantity if quantity > 0 else 0
                        pnl_pct = ((current_price - entry_price) / entry_price * 100) if entry_price > 0 else 0
                        to_sl = (stop_loss - current_price) * quantity if quantity > 0 else 0
                        to_tp = (take_profit - current_price) * quantity if quantity > 0 else 0
                        sl_dist = abs(stop_loss - current_price) / current_price * 100
                        tp_dist = abs(take_profit - current_price) / current_price * 100
                    else:
                        pnl = (entry_price - current_price) * quantity if quantity > 0 else 0
                        pnl_pct = ((entry_price - current_price) / entry_price * 100) if entry_price > 0 else 0
                        to_sl = (current_price - stop_loss) * quantity if quantity > 0 else 0
                        to_tp = (current_price - take_profit) * quantity if quantity > 0 else 0
                        sl_dist = abs(stop_loss - current_price) / current_price * 100
                        tp_dist = abs(take_profit - current_price) / current_price * 100

                    print(f"  Текущая цена:      {self.format_price(current_price)} ({pnl_pct:+.2f}%)")
                    print()
                    print(f"  До Stop Loss:      {self.format_price(stop_loss)} ({sl_dist:.2f}%) → {to_sl:+.2f} USDT")
                    print(f"  До Take Profit:    {self.format_price(take_profit)} ({tp_dist:.2f}%) → {to_tp:+.2f} USDT")
                    print()
                    print(f"  Текущий PnL:       {self.format_pnl(pnl)} ({pnl_pct:+.2f}%)")
                    print()

                print(f"{self.Fore.YELLOW}💰 КОМИССИИ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  За открытие:        {commission_open:.4f} USDT")
                print(f"  За закрытие:        {commission_close:.4f} USDT (ожидается)")
                print()

            elif status == 'CLOSED' and trade:
                close_price = trade.get('close_price', 0)
                pnl = trade.get('pnl', 0)
                pnl_pct = trade.get('pnl_percent', 0)
                close_reason = trade.get('close_reason', '-')

                print(f"{self.Fore.YELLOW}💰 ТОРГОВЫЕ ПАРАМЕТРЫ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  Направление:        {self.format_direction(direction)}")
                print(f"  Entry price:        {self.format_price(entry_price)}")
                if signal.get('created_time'):
                    print(f"  Время входа:        {self.format_datetime(signal.get('created_time'))}")
                print(f"  Stop Loss:          {self.format_price(stop_loss)}")
                print(f"  Take Profit:        {self.format_price(take_profit)}")
                print()

                print(f"{self.Fore.YELLOW}📈 ПОЗИЦИЯ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  Размер позиции:     {position_multiplier:.0%} (от стандарта)")
                print(f"  Маржа (залог):      {margin:.2f} USDT")
                print(f"  Стоимость позиции:  {position_value:.2f} USDT (с плечом {leverage:.0f}x)")
                print(f"  Плечо:              {leverage:.0f}x")
                print(f"  Количество:         {quantity:.4f} {symbol}")
                print()

                print(f"{self.Fore.YELLOW}💰 КОМИССИИ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  За открытие:        {commission_open:.4f} USDT")
                print(f"  За закрытие:        {commission_close:.4f} USDT")
                total_commission = commission_open + commission_close
                print(f"  Всего:              {total_commission:.4f} USDT")
                print()

                print(f"{self.Fore.YELLOW}📊 РЕЗУЛЬТАТ СДЕЛКИ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  Цена входа:         {self.format_price(entry_price)}")
                print(f"  Цена выхода:        {self.format_price(close_price)}")
                print(f"  Разница:            {close_price - entry_price:+.2f} USDT")
                print(f"  Комиссии:           {total_commission:.4f} USDT")
                print(f"  Итоговый PnL:       {self.format_pnl(pnl)} ({pnl_pct:+.2f}%)")
                print(f"  Причина закрытия:   {close_reason}")
                if signal.get('created_time') and trade.get('closed_at'):
                    opened = self.utc_to_local(signal.get('created_time'))
                    closed = self.utc_to_local(trade.get('closed_at'))
                    dur = closed - opened
                    print(f"  Время в сделке:     {dur.seconds // 3600}ч {(dur.seconds % 3600) // 60}м")
                print()

            else:
                print(f"{self.Fore.YELLOW}📊 СТАТУС ОЖИДАНИЯ{self.Style.RESET_ALL}")
                print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                print(f"  Тип сигнала:       {entry_type}")
                print(f"  Зона входа:        {self.format_price(zone_low)} - {self.format_price(zone_high)}")
                print(f"  Score Screen2:     {self.format_score(screen2_score)}")
                print(f"  Ожидаемый паттерн: {expected_pattern}")
                print()

                if current_price > 0 and zone_low > 0 and zone_high > 0:
                    if zone_low <= current_price <= zone_high:
                        print(f"  Текущая цена:      {self.format_price(current_price)} (В ЗОНЕ)")
                    elif current_price < zone_low:
                        diff = (zone_low - current_price) / zone_low * 100
                        print(f"  Текущая цена:      {self.format_price(current_price)} (НИЖЕ зоны на {diff:.2f}%)")
                    else:
                        diff = (current_price - zone_high) / zone_high * 100
                        print(f"  Текущая цена:      {self.format_price(current_price)} (ВЫШЕ зоны на {diff:.2f}%)")

                if entry_type == 'SNIPER':
                    pool_price = 0
                    for p in liquidity_pools:
                        if isinstance(p, dict) and p.get('type') in ('SELL_SIDE', 'BUY_SIDE'):
                            pool_price = p.get('price', 0)
                            break

                    print(f"\n{self.Fore.YELLOW}🎯 SNIPER УСЛОВИЯ{self.Style.RESET_ALL}")
                    print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
                    if pool_price > 0:
                        print(f"  Пул ликвидности:    {self.format_price(pool_price)}")
                        if current_price < pool_price:
                            print(f"  Цена относительно:  НИЖЕ пула")
                        elif current_price > pool_price:
                            print(f"  Цена относительно:  ВЫШЕ пула")
                        else:
                            print(f"  Цена относительно:  НА УРОВНЕ пула")
                    print(f"  Для входа нужен:   прокол пула ликвидности и возврат в FVG")
                print()

            print(f"{self.Fore.YELLOW}📊 ДЕТАЛИ АНАЛИЗА{self.Style.RESET_ALL}")
            print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")

            trend_direction = signal.get('trend_direction', '-')
            adx = signal.get('adx', 0)
            print(f"  D1 тренд:           {trend_direction} (ADX: {adx:.1f})")

            if fvg_zones:
                print(f"\n{self.Fore.CYAN}  FVG зоны:{self.Style.RESET_ALL}")
                for i, fvg in enumerate(fvg_zones[:3], 1):
                    fvg_type = "Бычий" if fvg.get('type') == 'bullish' else "Медвежий"
                    fvg_low = fvg.get('low', 0)
                    fvg_high = fvg.get('high', 0)
                    fvg_strength = fvg.get('strength', 'WEAK')
                    print(
                        f"    {i}. {fvg_type}: {self.format_price(fvg_low)} - {self.format_price(fvg_high)} [{fvg_strength}]")

            if liquidity_pools and entry_type == 'SNIPER':
                print(f"\n{self.Fore.CYAN}  Пулы ликвидности:{self.Style.RESET_ALL}")
                for i, p in enumerate(liquidity_pools[:3], 1):
                    if isinstance(p, dict):
                        pool_type = "SELL_SIDE" if p.get('type') == 'SELL_SIDE' else "BUY_SIDE"
                        pool_price = p.get('price', 0)
                        touches = p.get('touches', 0)
                        print(f"    {i}. {pool_type}: {self.format_price(pool_price)} ({touches} касаний)")

            print(f"\n{self.Fore.CYAN}{'═' * 90}{self.Style.RESET_ALL}")
            input(f"\n{self.Fore.GREEN}Нажмите Enter для возврата...{self.Style.RESET_ALL}")

        except Exception as e:
            print(f"{self.Fore.RED}❌ Ошибка: {e}{self.Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            input(f"\n{self.Fore.GREEN}Нажмите Enter для возврата...{self.Style.RESET_ALL}")

    async def display_trend_analysis(self):
        """Анализ тренда D1"""
        try:
            if not self.signal_repo:
                print(f"{self.Fore.RED}❌ SignalRepository не инициализирован{self.Style.RESET_ALL}")
                return

            trends = await self.signal_repo.get_latest_trends(limit=50)
            trends = sorted(trends, key=lambda x: x.get('symbol', ''))

            self.clear_screen()
            self.print_header("📊 АНАЛИЗ ТРЕНДА (D1)")

            if not trends:
                print(f"{self.Fore.YELLOW}📭 Нет данных о трендах{self.Style.RESET_ALL}")
                input(f"\n{self.Fore.GREEN}Нажмите Enter для возврата...{self.Style.RESET_ALL}")
                return

            table_data = []
            for idx, trend in enumerate(trends, 1):
                symbol = trend.get('symbol', '')
                direction = trend.get('trend_direction', '')
                adx = trend.get('adx', 0)
                ema20 = trend.get('ema20', 0)
                ema50 = trend.get('ema50', 0)
                macd = trend.get('macd_line', 0)
                created = trend.get('created_time', '')

                if direction == 'BULL':
                    dir_display = f"{self.Fore.GREEN}▲ BULL{self.Style.RESET_ALL}"
                elif direction == 'BEAR':
                    dir_display = f"{self.Fore.RED}▼ BEAR{self.Style.RESET_ALL}"
                else:
                    dir_display = f"{self.Fore.YELLOW}● SIDEWAYS{self.Style.RESET_ALL}"

                if adx > 25:
                    adx_display = f"{self.Fore.GREEN}{adx:.1f}{self.Style.RESET_ALL}"
                elif adx > 20:
                    adx_display = f"{self.Fore.YELLOW}{adx:.1f}{self.Style.RESET_ALL}"
                else:
                    adx_display = f"{self.Fore.RED}{adx:.1f}{self.Style.RESET_ALL}"

                if macd > 0:
                    macd_display = f"{self.Fore.GREEN}+{macd:.2f}{self.Style.RESET_ALL}"
                elif macd < 0:
                    macd_display = f"{self.Fore.RED}{macd:.2f}{self.Style.RESET_ALL}"
                else:
                    macd_display = "0.00"

                table_data.append([
                    str(idx), symbol, dir_display, adx_display,
                    self.format_price(ema20), self.format_price(ema50),
                    macd_display, self.format_time(created)
                ])

            headers = ["#", "Монета", "Тренд", "ADX", "EMA20", "EMA50", "MACD", "Время"]
            print(self.table_builder.create_table(headers, table_data))

            input(f"\n{self.Fore.GREEN}Нажмите Enter для возврата...{self.Style.RESET_ALL}")

        except Exception as e:
            logger.error(f"Ошибка: {e}")
            print(f"{self.Fore.RED}❌ Ошибка: {e}{self.Style.RESET_ALL}")
            input(f"\n{self.Fore.GREEN}Нажмите Enter для возврата...{self.Style.RESET_ALL}")

    async def display_database_stats(self):
        """Статистика БД"""
        if not self.signal_repo:
            print(f"{self.Fore.RED}❌ SignalRepository не инициализирован{self.Style.RESET_ALL}")
            return

        stats = await self.signal_repo.get_database_stats()
        self.clear_screen()
        self.print_header("СТАТИСТИКА БАЗЫ ДАННЫХ")

        print(f"{self.Fore.YELLOW}📊 СТАТИСТИКА СИГНАЛОВ:{self.Style.RESET_ALL}")
        print(f"  Всего сигналов:      {stats.get('total_signals', 0)}")
        print(f"  WATCH:               {stats.get('subtypes_stats', {}).get('WATCH', 0)}")
        print(f"  M15:                 {stats.get('subtypes_stats', {}).get('M15', 0)}")
        print(f"  Активных:            {stats.get('active_signals', 0)}")
        print(f"  BUY:                 {stats.get('buy_signals', 0)}")
        print(f"  SELL:                {stats.get('sell_signals', 0)}")

        print(f"\n{self.Fore.YELLOW}🎯 ТИПЫ ВХОДОВ (SMC):{self.Style.RESET_ALL}")
        print(f"  SNIPER:              {stats.get('entry_type_stats', {}).get('SNIPER', 0)}")
        print(f"  TREND:               {stats.get('entry_type_stats', {}).get('TREND', 0)}")
        print(f"  LEGACY:              {stats.get('entry_type_stats', {}).get('LEGACY', 0)}")

        print(f"\n{self.Fore.YELLOW}📈 СТАТИСТИКА СДЕЛОК:{self.Style.RESET_ALL}")
        print(f"  Закрытых сделок:      {stats.get('closed_trades', 0)}")
        print(f"  Общий PnL:            {self.format_pnl(stats.get('total_pnl', 0))}")
        print(f"  Win Rate:             {stats.get('win_rate', 0):.1f}%")

        print(f"\n{self.Fore.YELLOW}📊 СТАТИСТИКА ТРЕНДОВ:{self.Style.RESET_ALL}")
        print(f"  Всего анализов:       {stats.get('total_trends', 0)}")

        print(f"\n{self.Fore.CYAN}{'=' * 80}{self.Style.RESET_ALL}")
        input(f"\n{self.Fore.GREEN}Нажмите Enter для продолжения...{self.Style.RESET_ALL}")

    def _get_export_dir(self) -> Path:
        project_root = Path(__file__).parent.parent.parent.parent
        date_folder = now().strftime("%Y-%m-%d")
        export_dir = project_root / 'logs' / 'exports' / date_folder
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

    async def _export_signals(self, fmt: str = 'csv') -> Optional[str]:
        if not self.signal_repo:
            return None
        signals = await self.signal_repo.get_signals(limit=1000)
        if not signals:
            return None

        export_dir = self._get_export_dir()
        timestamp = now().strftime("%Y%m%d_%H%M%S")
        fields = ['id', 'symbol', 'direction', 'signal_subtype', 'status', 'entry_price', 'stop_loss',
                  'take_profit', 'risk_reward_ratio', 'zone_low', 'zone_high', 'screen2_score', 'expected_pattern',
                  'trigger_pattern', 'confidence', 'created_time', 'expiration_time', 'entry_type']

        if fmt == 'csv':
            filename = export_dir / f"signals_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for signal in signals:
                    row = {field: signal.get(field, '') for field in fields}
                    writer.writerow(row)
            print(f"{self.Fore.GREEN}✅ Файл сохранён: {filename}{self.Style.RESET_ALL}")
            return str(filename)
        elif fmt == 'json':
            filename = export_dir / f"signals_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(signals, f, ensure_ascii=False, indent=2, default=str)
            print(f"{self.Fore.GREEN}✅ Файл сохранён: {filename}{self.Style.RESET_ALL}")
            return str(filename)
        return None

    async def _export_trades(self, fmt: str = 'csv') -> Optional[str]:
        if not self.trade_repo:
            return None
        trades = await self.trade_repo.get_closed_trades(limit=1000)
        if not trades:
            return None

        export_dir = self._get_export_dir()
        timestamp = now().strftime("%Y%m%d_%H%M%S")
        fields = ['id', 'signal_id', 'symbol', 'direction', 'entry_price', 'close_price', 'quantity', 'leverage',
                  'margin', 'position_value', 'pnl', 'pnl_percent', 'commission_open', 'commission_close',
                  'close_reason', 'opened_at', 'closed_at', 'status']

        if fmt == 'csv':
            filename = export_dir / f"trades_{timestamp}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for trade in trades:
                    row = {field: trade.get(field, '') for field in fields}
                    writer.writerow(row)
            print(f"{self.Fore.GREEN}✅ Файл сохранён: {filename}{self.Style.RESET_ALL}")
            return str(filename)
        elif fmt == 'json':
            filename = export_dir / f"trades_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(trades, f, ensure_ascii=False, indent=2, default=str)
            print(f"{self.Fore.GREEN}✅ Файл сохранён: {filename}{self.Style.RESET_ALL}")
            return str(filename)
        return None

    async def _export_stats(self) -> str:
        stats = await self.signal_repo.get_database_stats() if self.signal_repo else {}
        export_dir = self._get_export_dir()
        timestamp = now().strftime("%Y%m%d_%H%M%S")
        filename = export_dir / f"stats_{timestamp}.txt"

        content = f"""
========================================
СТАТИСТИКА ТОРГОВОГО БОТА
Дата: {format_local(now())}
========================================

СИГНАЛЫ:
  Всего сигналов:      {stats.get('total_signals', 0)}
  WATCH:               {stats.get('subtypes_stats', {}).get('WATCH', 0)}
  M15:                 {stats.get('subtypes_stats', {}).get('M15', 0)}
  Активных:            {stats.get('active_signals', 0)}
  BUY:                 {stats.get('buy_signals', 0)}
  SELL:                {stats.get('sell_signals', 0)}

ТИПЫ ВХОДОВ (SMC):
  SNIPER:              {stats.get('entry_type_stats', {}).get('SNIPER', 0)}
  TREND:               {stats.get('entry_type_stats', {}).get('TREND', 0)}
  LEGACY:              {stats.get('entry_type_stats', {}).get('LEGACY', 0)}

СДЕЛКИ:
  Закрытых сделок:     {stats.get('closed_trades', 0)}
  Общий PnL:           {stats.get('total_pnl', 0):.2f}
  Win Rate:            {stats.get('win_rate', 0):.1f}%

ТРЕНДЫ:
  Всего анализов:      {stats.get('total_trends', 0)}
"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"{self.Fore.GREEN}✅ Файл сохранён: {filename}{self.Style.RESET_ALL}")
        return str(filename)

    async def _export_all(self) -> str:
        export_dir = self._get_export_dir()
        timestamp = now().strftime("%Y%m%d_%H%M%S")
        zip_path = export_dir / f"full_export_{timestamp}.zip"

        temp_files = []
        signals_file = await self._export_signals('csv')
        if signals_file:
            temp_files.append(signals_file)
        trades_file = await self._export_trades('csv')
        if trades_file:
            temp_files.append(trades_file)
        stats_file = await self._export_stats()
        if stats_file:
            temp_files.append(stats_file)

        if not temp_files:
            print(f"{self.Fore.YELLOW}⚠️ Нет данных для архивации{self.Style.RESET_ALL}")
            return ""

        import zipfile
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file_path in temp_files:
                zipf.write(file_path, os.path.basename(file_path))

        print(f"{self.Fore.GREEN}✅ ZIP архив создан: {zip_path}{self.Style.RESET_ALL}")
        return str(zip_path)

    async def export_menu(self):
        """Меню экспорта данных"""
        while True:
            self.clear_screen()
            self.print_header("📁 ЭКСПОРТ ДАННЫХ")
            export_dir = self._get_export_dir()
            print(f"{self.Fore.YELLOW}Выберите формат экспорта:{self.Style.RESET_ALL}")
            print("1. 📊 Экспорт сигналов (CSV)")
            print("2. 📊 Экспорт сигналов (JSON)")
            print("3. 📈 Экспорт сделок (CSV)")
            print("4. 📈 Экспорт сделок (JSON)")
            print("5. 📋 Экспорт статистики (TXT)")
            print("6. 📦 Экспорт всех данных (ZIP)")
            print("0. 🔙 Назад")
            print(f"{self.Fore.CYAN}📂 Папка экспорта: {export_dir}{self.Style.RESET_ALL}")

            try:
                choice = input(f"\n{self.Fore.CYAN}🎯 Выбор (0-6): {self.Style.RESET_ALL}").strip()
                if choice == '1':
                    await self._export_signals('csv')
                    await asyncio.sleep(2)
                elif choice == '2':
                    await self._export_signals('json')
                    await asyncio.sleep(2)
                elif choice == '3':
                    await self._export_trades('csv')
                    await asyncio.sleep(2)
                elif choice == '4':
                    await self._export_trades('json')
                    await asyncio.sleep(2)
                elif choice == '5':
                    await self._export_stats()
                    await asyncio.sleep(2)
                elif choice == '6':
                    await self._export_all()
                    await asyncio.sleep(2)
                elif choice == '0':
                    break
            except KeyboardInterrupt:
                break

    async def run(self):
        await self.signal_repo.initialize()
        await self.trade_repo.initialize()
        data_provider.configure(self.config)

        while True:
            self.clear_screen()
            self.print_header("STATISTIK - СТАТИСТИКА И АНАЛИТИКА")

            account_state = await self.get_account_state()

            print(f"{self.Fore.YELLOW}💰 СОСТОЯНИЕ СЧЁТА:{self.Style.RESET_ALL}")
            print(f"{self.Fore.CYAN}{'─' * 90}{self.Style.RESET_ALL}")
            print(
                f"  Депозит (начальный):    {self.Fore.WHITE}{account_state['initial_balance']:.2f} USDT{self.Style.RESET_ALL}")
            print(
                f"  Текущий баланс:         {self.Fore.WHITE}{account_state['current_balance']:.2f} USDT{self.Style.RESET_ALL}")
            print(f"  Общий PnL:              {self.format_pnl(account_state['total_pnl'])}")
            print()

            print(f"{self.Fore.YELLOW}📋 ВЫБЕРИТЕ РЕЖИМ:{self.Style.RESET_ALL}")
            print("1. 🔍 Детали сигнала по ID")
            print("2. 📊 Анализ тренда (D1)")
            print("3. 📊 Статистика БД")
            print("4. 💾 Экспорт данных")
            print("0. 🚪 Выход")
            print()

            try:
                choice = input(f"{self.Fore.CYAN}🎯 Выбор (0-4): {self.Style.RESET_ALL}").strip()

                if choice == '1':
                    try:
                        signal_id = int(input(f"{self.Fore.CYAN}🔢 Введите ID сигнала: {self.Style.RESET_ALL}"))
                        await self.display_signal_details(signal_id)
                    except ValueError:
                        print(f"{self.Fore.RED}❌ Введите корректный ID{self.Style.RESET_ALL}")
                        await asyncio.sleep(1)
                elif choice == '2':
                    await self.display_trend_analysis()
                elif choice == '3':
                    await self.display_database_stats()
                elif choice == '4':
                    await self.export_menu()
                elif choice == '0':
                    break
                else:
                    print(f"{self.Fore.RED}❌ Неверный выбор!{self.Style.RESET_ALL}")
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                break


async def main():
    stats = Statistik()
    await stats.run()


if __name__ == "__main__":
    asyncio.run(main())