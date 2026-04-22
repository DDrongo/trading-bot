#!/usr/bin/env python3
"""
🎯 МОНИТОР ДЛЯ THREE SCREEN ANALYZER - ВЕРСИЯ 2.1 (FINAL)
"""

import asyncio
import logging
import os
import re
import sys
import yaml
import csv
import json
import zipfile
import aiosqlite
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

try:
    from colorama import init, Fore, Style

    init()
except ImportError:
    class Fore:
        RED = '';
        GREEN = '';
        YELLOW = '';
        CYAN = '';
        MAGENTA = '';
        WHITE = '';
        RESET = ''


    class Style:
        BRIGHT = '';
        RESET_ALL = ''


    def init():
        pass

from analyzer.core.time_utils import now, utc_now, to_local, format_local, parse_iso_to_local, TIMEZONE_OFFSET

logger = logging.getLogger('three_screen_monitor')


class ThreeScreenMonitor:

    def __init__(self, config: Optional[Dict] = None):
        if config is None:
            config_path = Path(__file__).parent / 'analyzer/config/config.yaml'
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            except Exception:
                config = {}

        self.config = config

        self.signal_repo = None
        self.trade_repo = None
        self.paper_account = None

        try:
            from analyzer.core.signal_repository import signal_repository
            self.signal_repo = signal_repository
        except ImportError:
            pass

        try:
            from analyzer.core.trade_repository import trade_repository
            self.trade_repo = trade_repository
        except ImportError:
            pass

        try:
            from analyzer.core.paper_account import PaperAccount
            self.paper_account = PaperAccount(config) if config else None
        except ImportError:
            pass

        self.data_provider = None
        try:
            from analyzer.core.data_provider import data_provider
            self.data_provider = data_provider
            self.data_provider.configure(self.config)
        except Exception:
            pass

        self.websocket = None
        try:
            from analyzer.core.websocket_client import BybitWebSocketClient
            self.websocket = BybitWebSocketClient()
        except Exception:
            pass

        display_config = self.config.get('display', {})
        self.timezone_offset = display_config.get('timezone_offset', TIMEZONE_OFFSET)
        self.refresh_interval = display_config.get('refresh_interval', 5)

        paper_config = self.config.get('paper_trading', {})
        self.starting_balance = paper_config.get('starting_virtual_balance', 10000.0)

        db_config = self.config.get('database', {})
        db_signals_config = self.config.get('database_signals', {})
        db_path = None
        if isinstance(db_signals_config, dict) and db_signals_config.get('enabled', False):
            db_path = db_signals_config.get('path')
        if not db_path and isinstance(db_config, dict):
            db_path = db_config.get('path')
        if not db_path:
            db_path = 'data/trading_bot.db'

        if not os.path.isabs(db_path):
            project_root = Path(__file__).parent
            self.db_path = str(project_root / db_path)
        else:
            self.db_path = db_path

        self._shutdown = False
        self.settings = {
            'refresh_interval': self.refresh_interval,
            'signals_limit': 20,
            'show_confidence': True,
            'show_rr_ratio': True
        }

        self._price_cache: Dict[str, float] = {}
        self._price_cache_time: Optional[datetime] = None
        self._price_cache_ttl = 5

        print(f"Monitor using DB: {self.db_path}")
        print(f"Timezone: UTC+{self.timezone_offset}")

    @staticmethod
    def strip_ansi(text: str) -> str:
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', str(text))

    def get_visible_length(self, text: str) -> int:
        return len(self.strip_ansi(str(text)))

    @staticmethod
    def clear_screen() -> None:
        os.system('cls' if os.name == 'nt' else 'clear')

    @staticmethod
    def print_header(title: str) -> None:
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 90}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{title}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 90}{Style.RESET_ALL}")

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

    @staticmethod
    def format_direction(direction: str) -> str:
        direction_lower = direction.lower() if direction else ''
        if direction_lower in ['buy', 'long']:
            return f"{Fore.GREEN}BUY{Style.RESET_ALL}"
        elif direction_lower in ['sell', 'short']:
            return f"{Fore.RED}SELL{Style.RESET_ALL}"
        return direction or 'N/A'

    @staticmethod
    def format_status(status: str) -> str:
        status_lower = status.lower() if status else ''
        if status_lower == 'watch':
            return f"{Fore.YELLOW}WATCH{Style.RESET_ALL}"
        elif status_lower == 'active':
            return f"{Fore.CYAN}ACTIVE{Style.RESET_ALL}"
        elif status_lower in ['closed', 'completed']:
            return f"{Fore.MAGENTA}CLOSED{Style.RESET_ALL}"
        elif status_lower == 'cancelled':
            return f"{Fore.RED}CANCELLED{Style.RESET_ALL}"
        elif status_lower == 'expired':
            return f"{Fore.WHITE}EXPIRED{Style.RESET_ALL}"
        elif status_lower == 'rejected':
            return f"{Fore.RED}REJECTED{Style.RESET_ALL}"
        else:
            return status or 'N/A'

    @staticmethod
    def format_confidence(confidence: float) -> str:
        if confidence is None:
            return "-"
        if confidence > 0.8:
            color = Fore.GREEN
        elif confidence > 0.6:
            color = Fore.YELLOW
        else:
            color = Fore.RED
        return f"{color}{confidence * 100:.1f}%{Style.RESET_ALL}"

    def format_price(self, price: float) -> str:
        if price is None or price == 0:
            return "-"
        if price < 0.01:
            return f"{price:.6f}"
        elif price < 0.1:
            return f"{price:.5f}"
        elif price < 1:
            return f"{price:.4f}"
        elif price < 10:
            return f"{price:.3f}"
        elif price < 100:
            return f"{price:.2f}"
        else:
            return f"{price:.2f}"

    @staticmethod
    def format_pnl(pnl: float) -> str:
        if pnl is None:
            return "-"
        if abs(pnl) < 0.01 and pnl != 0:
            return f"{Fore.YELLOW}{pnl:.6f}{Style.RESET_ALL}"
        if pnl > 0:
            return f"{Fore.GREEN}+{pnl:.2f}{Style.RESET_ALL}"
        elif pnl < 0:
            return f"{Fore.RED}{pnl:.2f}{Style.RESET_ALL}"
        else:
            return f"{Fore.YELLOW}0.00{Style.RESET_ALL}"

    @staticmethod
    def format_rr_ratio(rr: float) -> str:
        if rr is None or rr == 0:
            return "-"
        if rr >= 3.0:
            color = Fore.GREEN
        elif rr >= 2.0:
            color = Fore.YELLOW
        else:
            color = Fore.RED
        return f"{color}{rr:.2f}:1{Style.RESET_ALL}"

    def format_zone(self, zone_low: float, zone_high: float) -> str:
        if zone_low is None or zone_high is None or zone_low == 0 or zone_high == 0:
            return "-"
        return f"{self.format_price(zone_low)}-{self.format_price(zone_high)}"

    @staticmethod
    def format_score(score: int) -> str:
        if score is None or score == 0:
            return "-"
        if score >= 7:
            return f"{Fore.GREEN}{score}/8{Style.RESET_ALL}"
        elif score >= 5:
            return f"{Fore.YELLOW}{score}/8{Style.RESET_ALL}"
        else:
            return f"{Fore.RED}{score}/8{Style.RESET_ALL}"

    @staticmethod
    def format_entry_type(entry_type: str) -> str:
        if not entry_type:
            return "-"
        entry_type_upper = entry_type.upper()
        if entry_type_upper == 'SNIPER':
            return f"{Fore.GREEN}SNIPER{Style.RESET_ALL}"
        elif entry_type_upper == 'TREND':
            return f"{Fore.CYAN}TREND{Style.RESET_ALL}"
        elif entry_type_upper == 'LEGACY':
            return f"{Fore.YELLOW}LEGACY{Style.RESET_ALL}"
        else:
            return entry_type

    def create_table(self, headers: List[str], data: List[List[str]]) -> str:
        if not data:
            return "No data"

        col_widths = []
        for i, header in enumerate(headers):
            max_width = len(header)
            for row in data:
                if i < len(row):
                    visible_text = self.strip_ansi(str(row[i]))
                    max_width = max(max_width, len(visible_text))
            col_widths.append(max_width + 2)

        result = []
        top_border = "┌" + "┬".join("─" * width for width in col_widths) + "┐"
        result.append(top_border)

        header_line = "│"
        for i, header in enumerate(headers):
            padding = col_widths[i] - len(header)
            left_padding = padding // 2
            right_padding = padding - left_padding
            centered_header = " " * left_padding + header + " " * right_padding
            header_line += centered_header + "│"
        result.append(header_line)

        separator = "├" + "┼".join("─" * width for width in col_widths) + "┤"
        result.append(separator)

        for row in data:
            row_line = "│"
            for i in range(len(headers)):
                cell = row[i] if i < len(row) else ""
                visible_cell = str(cell)
                padding = col_widths[i] - self.get_visible_length(visible_cell)
                aligned_cell = visible_cell + " " * padding
                row_line += aligned_cell + "│"
            result.append(row_line)

        bottom_border = "└" + "┴".join("─" * width for width in col_widths) + "┘"
        result.append(bottom_border)
        return "\n".join(result)

    def _get_export_dir(self) -> Path:
        project_root = Path(__file__).parent
        date_folder = now().strftime("%Y-%m-%d")
        export_dir = project_root / 'logs' / 'exports' / date_folder
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

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
            except Exception as e:
                logger.error(f"Error getting price for {symbol}: {e}")

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
            # 1. Получаем PnL из закрытых сделок
            db_pnl = 0
            if self.trade_repo:
                closed_trades = await self.trade_repo.get_closed_trades(limit=1000)
                db_pnl = sum(t.get('pnl', 0) for t in closed_trades)
                state['total_pnl'] = db_pnl

            # 2. Текущий баланс = начальный + PnL
            state['current_balance'] = self.starting_balance + db_pnl

            # 3. Маржа по открытым позициям
            if self.paper_account:
                await self.paper_account.cleanup_expired_reservations()
                positions = await self.paper_account.get_open_positions()
                for pos in positions.values():
                    state['used_margin'] += pos.margin

            # 4. Резерв под WATCH (только активные)
            if self.signal_repo:
                watch_signals = await self.signal_repo.get_watch_signals_with_reserve()
                for watch in watch_signals:
                    reserved = watch.get('reserved_margin')
                    if reserved is not None:
                        state['reserved_for_watch'] += float(reserved)

            # 5. Доступно = текущий баланс - маржа - резерв
            state['available'] = state['current_balance'] - state['used_margin'] - state['reserved_for_watch']

        except Exception as e:
            logger.error(f"❌ Ошибка получения состояния счёта: {e}")

        return state

    async def display_signal_details(self, signal_id: int):
        """Детальная информация о сигнале с полным блоком позиции"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signal = await self.signal_repo.get_signal_by_id(signal_id)
            if not signal:
                print(f"{Fore.RED}❌ Сигнал #{signal_id} не найден{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
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

            print(f"{Fore.CYAN}{Style.BRIGHT}{'═' * 90}{Style.RESET_ALL}")
            if status == 'ACTIVE':
                print(
                    f"{Fore.CYAN}{Style.BRIGHT}{' ' * 30}ПОЗИЦИЯ #{signal_id}: {symbol} [{entry_type}] — АКТИВНА{Style.RESET_ALL}")
            elif status == 'CLOSED':
                print(
                    f"{Fore.CYAN}{Style.BRIGHT}{' ' * 30}ПОЗИЦИЯ #{signal_id}: {symbol} [{entry_type}] — ЗАКРЫТА{Style.RESET_ALL}")
            else:
                print(f"{Fore.CYAN}{Style.BRIGHT}{' ' * 30}АНАЛИЗ {symbol} [{entry_type}]{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{Style.BRIGHT}{'═' * 90}{Style.RESET_ALL}\n")

            # ========== ДЛЯ ACTIVE ==========
            if status == 'ACTIVE':
                print(f"{Fore.YELLOW}💰 ТОРГОВЫЕ ПАРАМЕТРЫ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(f"  Направление:        {'▲ BUY' if direction == 'BUY' else '▼ SELL'}")
                print(f"  Entry price:        {self.format_price(entry_price)}")
                if signal.get('created_time'):
                    print(f"  Время входа:        {self.format_datetime(signal.get('created_time'))}")
                print(f"  Stop Loss:          {self.format_price(stop_loss)}")
                print(f"  Take Profit:        {self.format_price(take_profit)}")
                print()

                print(f"{Fore.YELLOW}📈 ПОЗИЦИЯ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(f"  Размер позиции:     {position_multiplier:.0%} (от стандарта)")
                print(f"  Маржа (залог):      {margin:.2f} USDT")
                print(f"  Стоимость позиции:  {position_value:.2f} USDT (с плечом {leverage:.0f}x)")
                print(f"  Плечо:              {leverage:.0f}x")
                print(f"  Количество:         {quantity:.4f} {symbol}")
                print()

                print(f"{Fore.YELLOW}📊 СТАТУС ПОЗИЦИИ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
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

                print(f"{Fore.YELLOW}💰 КОМИССИИ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(f"  За открытие:        {commission_open:.4f} USDT")
                print(f"  За закрытие:        {commission_close:.4f} USDT (ожидается)")
                print()

            # ========== ДЛЯ CLOSED ==========
            elif status == 'CLOSED' and trade:
                close_price = trade.get('close_price', 0)
                pnl = trade.get('pnl', 0)
                pnl_pct = trade.get('pnl_percent', 0)
                close_reason = trade.get('close_reason', '-')

                print(f"{Fore.YELLOW}💰 ТОРГОВЫЕ ПАРАМЕТРЫ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(f"  Направление:        {'▲ BUY' if direction == 'BUY' else '▼ SELL'}")
                print(f"  Entry price:        {self.format_price(entry_price)}")
                if signal.get('created_time'):
                    print(f"  Время входа:        {self.format_datetime(signal.get('created_time'))}")
                print(f"  Stop Loss:          {self.format_price(stop_loss)}")
                print(f"  Take Profit:        {self.format_price(take_profit)}")
                print()

                print(f"{Fore.YELLOW}📈 ПОЗИЦИЯ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(f"  Размер позиции:     {position_multiplier:.0%} (от стандарта)")
                print(f"  Маржа (залог):      {margin:.2f} USDT")
                print(f"  Стоимость позиции:  {position_value:.2f} USDT (с плечом {leverage:.0f}x)")
                print(f"  Плечо:              {leverage:.0f}x")
                print(f"  Количество:         {quantity:.4f} {symbol}")
                print()

                print(f"{Fore.YELLOW}💰 КОМИССИИ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(f"  За открытие:        {commission_open:.4f} USDT")
                print(f"  За закрытие:        {commission_close:.4f} USDT")
                total_commission = commission_open + commission_close
                print(f"  Всего:              {total_commission:.4f} USDT")
                print()

                print(f"{Fore.YELLOW}📊 РЕЗУЛЬТАТ СДЕЛКИ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
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

            # ========== ДЛЯ WATCH ==========
            elif status == 'WATCH':
                print(f"{Fore.YELLOW}📊 СТАТУС ОЖИДАНИЯ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
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

                    print(f"\n{Fore.YELLOW}🎯 SNIPER УСЛОВИЯ{Style.RESET_ALL}")
                    print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
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

            # ========== АНАЛИТИЧЕСКАЯ ИНФОРМАЦИЯ (ДЛЯ ВСЕХ) ==========
            print(f"{Fore.YELLOW}📊 ДЕТАЛИ АНАЛИЗА{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")

            trend_direction = signal.get('trend_direction', '-')
            adx = signal.get('adx', 0)
            print(f"  D1 тренд:           {trend_direction} (ADX: {adx:.1f})")

            if fvg_zones:
                print(f"\n{Fore.CYAN}  FVG зоны:{Style.RESET_ALL}")
                for i, fvg in enumerate(fvg_zones[:3], 1):
                    fvg_type = "Бычий" if fvg.get('type') == 'bullish' else "Медвежий"
                    fvg_low = fvg.get('low', 0)
                    fvg_high = fvg.get('high', 0)
                    fvg_strength = fvg.get('strength', 'WEAK')
                    print(
                        f"    {i}. {fvg_type}: {self.format_price(fvg_low)} - {self.format_price(fvg_high)} [{fvg_strength}]")

            if liquidity_pools and entry_type == 'SNIPER':
                print(f"\n{Fore.CYAN}  Пулы ликвидности:{Style.RESET_ALL}")
                for i, p in enumerate(liquidity_pools[:3], 1):
                    if isinstance(p, dict):
                        pool_type = "SELL_SIDE" if p.get('type') == 'SELL_SIDE' else "BUY_SIDE"
                        pool_price = p.get('price', 0)
                        touches = p.get('touches', 0)
                        print(f"    {i}. {pool_type}: {self.format_price(pool_price)} ({touches} касаний)")

            print(f"\n{Fore.CYAN}{'═' * 90}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_trades_table(self):
        """Отображение таблицы сделок"""
        try:
            if not self.trade_repo:
                print(f"{Fore.RED}❌ TradeRepository не инициализирован{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            trades = await self.trade_repo.get_closed_trades(limit=50)

            self.clear_screen()
            self.print_header("📈 ТАБЛИЦА СДЕЛОК")

            if not trades:
                print(f"{Fore.YELLOW}📭 Нет закрытых сделок{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            table_data = []
            total_pnl = 0
            winning = 0
            losing = 0

            for trade in trades:
                pnl = trade.get('pnl', 0)
                total_pnl += pnl
                if pnl > 0:
                    winning += 1
                elif pnl < 0:
                    losing += 1

                commission_open = trade.get('commission_open', 0)
                commission_close = trade.get('commission_close', 0)
                total_commission = commission_open + commission_close

                table_data.append([
                    str(trade.get('id', '')),
                    trade.get('symbol', ''),
                    self.format_direction(trade.get('direction', '')),
                    self.format_price(trade.get('entry_price', 0)),
                    self.format_price(trade.get('close_price', 0)),
                    self.format_pnl(pnl),
                    f"{trade.get('pnl_percent', 0):+.2f}%" if trade.get('pnl_percent') else "-",
                    f"{total_commission:.4f}",
                    trade.get('close_reason', '-'),
                    self.format_datetime(trade.get('closed_at', ''))
                ])

            headers = ["ID", "Монета", "Напр", "Entry", "Exit", "PnL", "PnL%", "Комисс", "Причина", "Закрыта"]
            table = self.create_table(headers, table_data)
            print(table)

            print(f"\n{Fore.CYAN}📊 ИТОГИ:{Style.RESET_ALL}")
            print(f"   Всего сделок: {len(trades)}")
            print(f"   Прибыльных: {Fore.GREEN}{winning}{Style.RESET_ALL}")
            print(f"   Убыточных: {Fore.RED}{losing}{Style.RESET_ALL}")
            print(f"   Общий PnL: {self.format_pnl(total_pnl)}")

            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

        except Exception as e:
            logger.error(f"❌ Ошибка отображения сделок: {e}")
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_all_signals(self):
        """Таблица всех сигналов (с SL/TP)"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signals = await self.signal_repo.get_signals_with_trades(limit=200)
            self.clear_screen()
            self.print_header("ВСЕ СИГНАЛЫ")

            if not signals:
                print(f"{Fore.YELLOW}📭 Нет сигналов{Style.RESET_ALL}")
            else:
                table_data = []
                symbols_to_subscribe = []

                for signal in signals:
                    signal_id = signal.get('id', '')
                    symbol = signal.get('symbol', '')
                    status = signal.get('status', '')
                    entry_type = signal.get('entry_type', 'LEGACY')
                    direction = signal.get('direction', '')
                    entry_price = signal.get('entry_price', 0)
                    stop_loss = signal.get('stop_loss', 0)
                    take_profit = signal.get('take_profit', 0)
                    zone_low = signal.get('zone_low', 0)
                    zone_high = signal.get('zone_high', 0)
                    score = signal.get('screen2_score', 0)
                    rr_ratio = signal.get('risk_reward_ratio', 0)
                    trade_pnl = signal.get('trade_pnl')
                    position_size = signal.get('position_size', 0)

                    symbols_to_subscribe.append(symbol)

                    # Текущая цена
                    current_price = await self.get_current_price(symbol)

                    # Расчёт PnL для ACTIVE
                    if status.upper() == 'ACTIVE' and current_price and current_price > 0 and entry_price > 0:
                        if direction == 'BUY':
                            pnl = (current_price - entry_price) * position_size
                        else:
                            pnl = (entry_price - current_price) * position_size
                        pnl_display = self.format_pnl(pnl)
                    else:
                        pnl_display = self.format_pnl(trade_pnl) if trade_pnl is not None else "-"

                    # Entry, SL, TP показываем для ACTIVE и CLOSED
                    if status.upper() in ['ACTIVE', 'CLOSED']:
                        entry_display = self.format_price(entry_price) if entry_price > 0 else "-"
                        sl_display = self.format_price(stop_loss) if stop_loss > 0 else "-"
                        tp_display = self.format_price(take_profit) if take_profit > 0 else "-"
                    else:
                        entry_display = "-"
                        sl_display = "-"
                        tp_display = "-"

                    # Зона
                    if zone_low > 0 and zone_high > 0:
                        zone_display = self.format_zone(zone_low, zone_high)
                    else:
                        zone_display = "-"

                    # Текущая цена с индикатором
                    if current_price and current_price > 0:
                        if zone_low <= current_price <= zone_high:
                            price_display = f"{Fore.YELLOW}{self.format_price(current_price)}*{Style.RESET_ALL}"
                        else:
                            price_display = self.format_price(current_price)
                    else:
                        price_display = "-"

                    time_str = self.format_time(signal.get('created_time', ''))
                    date_str = self.format_date(signal.get('created_time', ''))

                    # Направление
                    dir_display = "BUY" if direction == 'BUY' else "SELL" if direction == 'SELL' else "-"

                    table_data.append([
                        str(signal_id),
                        symbol,
                        dir_display,
                        self.format_status(status),
                        self.format_entry_type(entry_type),
                        entry_display,
                        sl_display,
                        tp_display,
                        zone_display,
                        self.format_score(score),
                        price_display,
                        pnl_display,
                        self.format_rr_ratio(rr_ratio) if rr_ratio else "-",
                        f"{date_str} {time_str}"
                    ])

                if self.websocket and symbols_to_subscribe:
                    try:
                        self.websocket.add_symbols(symbols_to_subscribe)
                    except Exception:
                        pass

                headers = ["ID", "Монета", "Напр", "Статус", "Тип", "Entry", "SL", "TP", "Зона", "Score", "Цена", "PnL",
                           "R/R", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                # Статистика
                active_count = sum(1 for s in signals if s.get('status', '').lower() in ['active'])
                watch_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'WATCH')
                m15_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'M15')
                sniper_count = sum(1 for s in signals if s.get('entry_type', '') == 'SNIPER')
                trend_count = sum(1 for s in signals if s.get('entry_type', '') == 'TREND')
                legacy_count = sum(1 for s in signals if s.get('entry_type', '') == 'LEGACY')
                buy_count = sum(1 for s in signals if s.get('direction', '').lower() == 'buy')
                sell_count = sum(1 for s in signals if s.get('direction', '').lower() == 'sell')

                print(f"\n{Fore.CYAN}📈 СТАТИСТИКА:{Style.RESET_ALL}")
                print(f"  Всего сигналов: {len(signals)}")
                print(f"  Активных: {active_count}")
                print(f"  WATCH: {watch_count} | M15: {m15_count}")
                print(f"  SNIPER: {sniper_count} | TREND: {trend_count} | LEGACY: {legacy_count}")
                print(f"  BUY: {buy_count} | SELL: {sell_count}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def display_realtime_monitor(self):
        """Реалтайм монитор (таблица с автообновлением)"""
        self._shutdown = False

        if self.websocket:
            try:
                if not self.websocket.running:
                    asyncio.create_task(self.websocket.connect())
                    await asyncio.sleep(1)
            except Exception:
                pass

        try:
            while not self._shutdown:
                self.clear_screen()
                self.print_header("THREE SCREEN ANALYZER - РЕАЛТАЙМ МОНИТОР (v2.1)")

                trading_mode = self.config.get('trading_mode', 'pro')
                mode_color = Fore.GREEN if trading_mode == 'light' else Fore.CYAN
                print(f"{mode_color}🎯 Режим торговли: {trading_mode.upper()}{Style.RESET_ALL}")
                print()

                if not self.signal_repo:
                    print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                    break

                signals = await self.signal_repo.get_signals_with_trades(limit=50)
                stats = await self.signal_repo.get_database_stats()
                account_state = await self.get_account_state()

                # Состояние счёта
                print(f"{Fore.YELLOW}💰 СОСТОЯНИЕ СЧЁТА:{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
                print(
                    f"  Депозит (начальный):    {Fore.WHITE}{account_state['initial_balance']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Текущий баланс:         {Fore.WHITE}{account_state['current_balance']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Использовано маржи:     {Fore.YELLOW}{account_state['used_margin']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Зарезервировано (WATCH):{Fore.YELLOW}{account_state['reserved_for_watch']:.2f} USDT{Style.RESET_ALL}")
                print(
                    f"  Доступно средств:       {Fore.GREEN if account_state['available'] > 0 else Fore.RED}{account_state['available']:.2f} USDT{Style.RESET_ALL}")
                print(f"  Общий PnL:              {self.format_pnl(account_state['total_pnl'])}")
                print()

                # Статистика системы
                print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИСТЕМЫ:{Style.RESET_ALL}")
                print(f"  Всего сигналов: {stats.get('total_signals', 0)}")
                print(f"  WATCH: {stats.get('subtypes_stats', {}).get('WATCH', 0)}")
                print(f"  M15: {stats.get('subtypes_stats', {}).get('M15', 0)}")
                print(f"  Активных: {stats.get('active_signals', 0)}")
                print(f"  BUY: {stats.get('buy_signals', 0)}  |  SELL: {stats.get('sell_signals', 0)}")
                print(f"  SNIPER: {stats.get('entry_type_stats', {}).get('SNIPER', 0)}")
                print(f"  TREND: {stats.get('entry_type_stats', {}).get('TREND', 0)}")
                print(f"  LEGACY: {stats.get('entry_type_stats', {}).get('LEGACY', 0)}")
                print()

                if not signals:
                    print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
                else:
                    table_data = []
                    symbols_to_subscribe = []

                    for signal in signals:
                        signal_id = signal.get('id', '')
                        symbol = signal.get('symbol', '')
                        status = signal.get('status', '')
                        entry_type = signal.get('entry_type', 'LEGACY')
                        direction = signal.get('direction', '')
                        entry_price = signal.get('entry_price', 0)
                        stop_loss = signal.get('stop_loss', 0)
                        take_profit = signal.get('take_profit', 0)
                        zone_low = signal.get('zone_low', 0)
                        zone_high = signal.get('zone_high', 0)
                        score = signal.get('screen2_score', 0)
                        rr_ratio = signal.get('risk_reward_ratio', 0)
                        trade_pnl = signal.get('trade_pnl')
                        position_size = signal.get('position_size', 0)

                        symbols_to_subscribe.append(symbol)

                        # Текущая цена
                        current_price = await self.get_current_price(symbol)

                        # Расчёт PnL для ACTIVE
                        if status.upper() == 'ACTIVE' and current_price and current_price > 0 and entry_price > 0:
                            if direction == 'BUY':
                                pnl = (current_price - entry_price) * position_size
                            else:
                                pnl = (entry_price - current_price) * position_size
                            pnl_display = self.format_pnl(pnl)
                        else:
                            pnl_display = self.format_pnl(trade_pnl) if trade_pnl is not None else "-"

                        # Entry, SL, TP показываем для ACTIVE и CLOSED
                        if status.upper() in ['ACTIVE', 'CLOSED']:
                            entry_display = self.format_price(entry_price) if entry_price > 0 else "-"
                            sl_display = self.format_price(stop_loss) if stop_loss > 0 else "-"
                            tp_display = self.format_price(take_profit) if take_profit > 0 else "-"
                        else:
                            entry_display = "-"
                            sl_display = "-"
                            tp_display = "-"

                        # Зона
                        if zone_low > 0 and zone_high > 0:
                            zone_display = self.format_zone(zone_low, zone_high)
                        else:
                            zone_display = "-"

                        # Текущая цена с индикатором (звездочка если в зоне)
                        if current_price and current_price > 0:
                            if zone_low <= current_price <= zone_high:
                                price_display = f"{Fore.YELLOW}{self.format_price(current_price)}*{Style.RESET_ALL}"
                            else:
                                price_display = self.format_price(current_price)
                        else:
                            price_display = "-"

                        time_str = self.format_time(signal.get('created_time', ''))
                        date_str = self.format_date(signal.get('created_time', ''))

                        # Направление для отображения
                        dir_display = "▲ BUY" if direction == 'BUY' else "▼ SELL" if direction == 'SELL' else "-"

                        table_data.append([
                            str(signal_id),
                            symbol,
                            dir_display,
                            self.format_status(status),
                            self.format_entry_type(entry_type),
                            entry_display,
                            sl_display,
                            tp_display,
                            zone_display,
                            self.format_score(score),
                            price_display,
                            pnl_display,
                            self.format_rr_ratio(rr_ratio) if rr_ratio else "-",
                            f"{date_str} {time_str}"
                        ])

                    if self.websocket and symbols_to_subscribe:
                        try:
                            self.websocket.add_symbols(symbols_to_subscribe)
                        except Exception:
                            pass

                    headers = ["ID", "Монета", "Напр", "Статус", "Тип", "Entry", "SL", "TP", "Зона", "Score", "Цена",
                               "PnL", "R/R", "Время"]
                    table = self.create_table(headers, table_data)
                    print(table)

                print(
                    f"\n{Fore.CYAN}🔄 Автообновление через {self.settings['refresh_interval']} сек...{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Нажмите Enter для возврата в меню{Style.RESET_ALL}")

                try:
                    await asyncio.wait_for(self._wait_for_enter(), timeout=self.settings['refresh_interval'])
                    break
                except asyncio.TimeoutError:
                    continue

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        finally:
            self._shutdown = False

    @staticmethod
    async def _wait_for_enter():
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, input)

    async def main_menu(self):
        while True:
            self.clear_screen()
            self.print_header("THREE SCREEN ANALYZER - МОНИТОР СИГНАЛОВ (v2.1)")

            trading_mode = self.config.get('trading_mode', 'pro')
            mode_color = Fore.GREEN if trading_mode == 'light' else Fore.CYAN
            print(f"{mode_color}🎯 Режим торговли: {trading_mode.upper()}{Style.RESET_ALL}")

            account_state = await self.get_account_state()

            print(f"{Fore.YELLOW}💰 СОСТОЯНИЕ СЧЁТА:{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{'─' * 90}{Style.RESET_ALL}")
            print(f"  Депозит (начальный):    {Fore.WHITE}{account_state['initial_balance']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Текущий баланс:         {Fore.WHITE}{account_state['current_balance']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Использовано маржи:     {Fore.YELLOW}{account_state['used_margin']:.2f} USDT{Style.RESET_ALL}")
            print(
                f"  Зарезервировано (WATCH):{Fore.YELLOW}{account_state['reserved_for_watch']:.2f} USDT{Style.RESET_ALL}")
            print(
                f"  Доступно средств:       {Fore.GREEN if account_state['available'] > 0 else Fore.RED}{account_state['available']:.2f} USDT{Style.RESET_ALL}")
            print(f"  Общий PnL:              {self.format_pnl(account_state['total_pnl'])}")
            print()

            print(f"{Fore.YELLOW}📋 ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:{Style.RESET_ALL}")
            print("1. 📊 Реалтайм монитор (WATCH + M15)")
            print("2. 📋 Таблица всех сигналов")
            print("3. 📈 Таблица сделок (trades)")
            print("4. 🔍 Детали сигнала по ID")
            print("5. 📊 Анализ тренда (D1)")
            print("6. 📊 Статистика БД")
            print("7. 💾 Экспорт данных")
            print("8. 🚪 Выход")
            print()
            print(f"{Fore.CYAN}🕐 Локальное время: UTC+{self.timezone_offset}{Style.RESET_ALL}")
            export_dir = self._get_export_dir()
            print(f"{Fore.CYAN}📂 Папка экспорта: {export_dir}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🎯 Выбор (1-8): {Style.RESET_ALL}").strip()

                if choice == '1':
                    await self.display_realtime_monitor()
                elif choice == '2':
                    await self.display_all_signals()
                elif choice == '3':
                    await self.display_trades_table()
                elif choice == '4':
                    try:
                        signal_id = int(input(f"{Fore.CYAN}🔢 Введите ID сигнала: {Style.RESET_ALL}"))
                        await self.display_signal_details(signal_id)
                    except ValueError:
                        print(f"{Fore.RED}❌ Введите корректный ID{Style.RESET_ALL}")
                        await asyncio.sleep(1)
                elif choice == '5':
                    await self.display_trend_analysis()
                elif choice == '6':
                    await self.display_database_stats()
                elif choice == '7':
                    await self.export_menu()
                elif choice == '8':
                    print(f"{Fore.GREEN}👋 До свидания!{Style.RESET_ALL}")
                    break
                else:
                    print(f"{Fore.RED}❌ Неверный выбор!{Style.RESET_ALL}")
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                print(f"\n{Fore.YELLOW}🛑 Выход...{Style.RESET_ALL}")
                break

    async def display_database_stats(self):
        """Отображение статистики базы данных"""
        if not self.signal_repo:
            print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
            return

        stats = await self.signal_repo.get_database_stats()
        self.clear_screen()
        self.print_header("СТАТИСТИКА БАЗЫ ДАННЫХ")

        print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИГНАЛОВ:{Style.RESET_ALL}")
        print(f"  Всего сигналов:      {stats.get('total_signals', 0)}")
        print(f"  WATCH:               {stats.get('subtypes_stats', {}).get('WATCH', 0)}")
        print(f"  M15:                 {stats.get('subtypes_stats', {}).get('M15', 0)}")
        print(f"  Активных:            {stats.get('active_signals', 0)}")
        print(f"  BUY:                 {stats.get('buy_signals', 0)}")
        print(f"  SELL:                {stats.get('sell_signals', 0)}")

        print(f"\n{Fore.YELLOW}🎯 ТИПЫ ВХОДОВ (SMC):{Style.RESET_ALL}")
        print(f"  SNIPER:              {stats.get('entry_type_stats', {}).get('SNIPER', 0)}")
        print(f"  TREND:               {stats.get('entry_type_stats', {}).get('TREND', 0)}")
        print(f"  LEGACY:              {stats.get('entry_type_stats', {}).get('LEGACY', 0)}")

        print(f"\n{Fore.YELLOW}📈 СТАТИСТИКА СДЕЛОК:{Style.RESET_ALL}")
        print(f"  Закрытых сделок:      {stats.get('closed_trades', 0)}")
        print(f"  Общий PnL:            {self.format_pnl(stats.get('total_pnl', 0))}")
        print(f"  Win Rate:             {stats.get('win_rate', 0):.1f}%")

        print(f"\n{Fore.YELLOW}📊 СТАТИСТИКА ТРЕНДОВ:{Style.RESET_ALL}")
        print(f"  Всего анализов:       {stats.get('total_trends', 0)}")

        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def run(self):
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}SignalRepository not found{Style.RESET_ALL}")
                return

            if not await self.signal_repo.initialize():
                print(f"{Fore.RED}Failed to initialize database{Style.RESET_ALL}")
                return

            if self.trade_repo:
                await self.trade_repo.initialize()

            print(f"{Fore.GREEN}Database connected: {self.db_path}{Style.RESET_ALL}")
            await self.main_menu()
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Monitor stopped{Style.RESET_ALL}")


async def main():
    print(f"{Fore.GREEN}Starting Three Screen Analyzer Monitor (v2.1 FINAL)...{Style.RESET_ALL}")
    monitor = ThreeScreenMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Program terminated{Style.RESET_ALL}")