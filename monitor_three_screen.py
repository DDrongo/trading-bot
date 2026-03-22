#!/usr/bin/env python3
# monitor_three_screen.py
"""
🎯 МОНИТОР ДЛЯ THREE SCREEN ANALYZER - ПОЛНАЯ ВЕРСИЯ (Фаза 1.3.1)
Особенности:
- Таблица сигналов с колонками: Экран, Тренд, Средства
- Таблица сделок (trades) с PnL статистикой
- Детали сигнала по ID с полной информацией
- Преобразование UTC → локальное время
- Отображение текущей цены и расстояний
"""

import asyncio
import logging
import os
import re
import yaml
from typing import List, Dict, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta
from colorama import init, Fore, Style

init()
logger = logging.getLogger('three_screen_monitor')


class ThreeScreenMonitor:
    def __init__(self, config=None):
        # Если config не передан, загружаем из файла
        if config is None:
            config_path = Path(__file__).parent / 'analyzer/config/config.yaml'
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            except Exception as e:
                print(f"❌ Ошибка загрузки конфига: {e}")
                config = {}

        self.config = config

        # Импортируем репозитории
        try:
            from analyzer.core.signal_repository import signal_repository
            self.signal_repo = signal_repository
        except ImportError as e:
            print(f"❌ Ошибка импорта signal_repository: {e}")
            self.signal_repo = None

        try:
            from analyzer.core.trade_repository import trade_repository
            self.trade_repo = trade_repository
        except ImportError as e:
            print(f"❌ Ошибка импорта trade_repository: {e}")
            self.trade_repo = None

        # Настройки отображения
        display_config = self.config.get('display', {})
        self.timezone_offset = display_config.get('timezone_offset', 3)  # Москва +3
        self.refresh_interval = display_config.get('refresh_interval', 5)

        # Получаем путь к БД
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

        print(f"📊 Монитор использует БД: {self.db_path}")
        print(f"🕐 Часовой пояс: UTC+{self.timezone_offset}")

    def strip_ansi(self, text: str) -> str:
        """Удаляет ANSI escape sequences"""
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', str(text))

    def get_visible_length(self, text: str) -> int:
        """Возвращает видимую длину строки без учета ANSI кодов"""
        return len(self.strip_ansi(str(text)))

    def clear_screen(self):
        """Очистка экрана"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def print_header(self, title: str):
        """Печать заголовка"""
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * 80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{title}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * 80}{Style.RESET_ALL}")

    def utc_to_local(self, utc_str: str) -> datetime:
        """Преобразование UTC строки в локальное время"""
        if not utc_str:
            return datetime.now()
        try:
            utc_time = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
            local_time = utc_time + timedelta(hours=self.timezone_offset)
            return local_time
        except Exception:
            return datetime.now()

    def format_time(self, timestamp_str: str) -> str:
        """Форматирование времени (локальное)"""
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%H:%M")
        except:
            return timestamp_str[11:16] if ':' in timestamp_str else '-'

    def format_date(self, timestamp_str: str) -> str:
        """Форматирование даты (локальная)"""
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%m-%d")
        except:
            return timestamp_str[:10] if '-' in timestamp_str else '-'

    def format_datetime(self, timestamp_str: str) -> str:
        """Форматирование полной даты и времени (локальное)"""
        if not timestamp_str:
            return "-"
        try:
            local_dt = self.utc_to_local(timestamp_str)
            return local_dt.strftime("%d.%m.%Y %H:%M:%S")
        except:
            return timestamp_str

    def format_direction(self, direction: str) -> str:
        """Форматирование направления"""
        direction_lower = direction.lower() if direction else ''
        if direction_lower in ['buy', 'long']:
            return f"{Fore.GREEN}▲ BUY{Style.RESET_ALL}"
        elif direction_lower in ['sell', 'short']:
            return f"{Fore.RED}▼ SELL{Style.RESET_ALL}"
        return direction or 'N/A'

    def format_status(self, status: str) -> str:
        """Форматирование статуса"""
        status_lower = status.lower() if status else ''

        if status_lower == 'pending':
            return f"{Fore.YELLOW}PENDING{Style.RESET_ALL}"
        elif status_lower == 'active':
            return f"{Fore.CYAN}ACTIVE{Style.RESET_ALL}"
        elif status_lower in ['closed', 'completed']:
            return f"{Fore.MAGENTA}CLOSED{Style.RESET_ALL}"
        elif status_lower == 'cancelled':
            return f"{Fore.RED}CANCELLED{Style.RESET_ALL}"
        elif status_lower == 'expired':
            return f"{Fore.WHITE}EXPIRED{Style.RESET_ALL}"
        else:
            return status or 'N/A'

    def format_confidence(self, confidence: float) -> str:
        """Форматирование уверенности"""
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
        """Форматирование цены"""
        if price is None or price == 0:
            return "-"
        if price >= 1:
            return f"{price:.2f}"
        return f"{price:.6f}"

    def format_rr_ratio(self, rr: float) -> str:
        """Форматирование Risk/Reward"""
        if rr is None or rr == 0:
            return "-"

        if rr >= 3.0:
            color = Fore.GREEN
        elif rr >= 2.0:
            color = Fore.YELLOW
        else:
            color = Fore.RED

        return f"{color}{rr:.2f}:1{Style.RESET_ALL}"

    def format_pnl(self, pnl: float) -> str:
        """Форматирование PnL"""
        if pnl is None:
            return "-"
        if pnl > 0:
            return f"{Fore.GREEN}+{pnl:.2f}{Style.RESET_ALL}"
        elif pnl < 0:
            return f"{Fore.RED}{pnl:.2f}{Style.RESET_ALL}"
        else:
            return f"{Fore.YELLOW}0.00{Style.RESET_ALL}"

    def format_trend(self, trend: str) -> str:
        """Форматирование тренда"""
        if not trend:
            return "-"
        trend_upper = trend.upper()
        if trend_upper == 'BULL':
            return f"{Fore.GREEN}BULL{Style.RESET_ALL}"
        elif trend_upper == 'BEAR':
            return f"{Fore.RED}BEAR{Style.RESET_ALL}"
        return trend

    def format_screen(self, screen: str) -> str:
        """Форматирование экрана"""
        if not screen:
            return "-"
        if screen == 'D1':
            return f"{Fore.CYAN}D1{Style.RESET_ALL}"
        elif screen == 'H4':
            return f"{Fore.YELLOW}H4{Style.RESET_ALL}"
        elif screen == 'M15':
            return f"{Fore.GREEN}M15{Style.RESET_ALL}"
        return screen

    def format_position_size(self, size: float) -> str:
        """Форматирование размера позиции"""
        if size is None or size == 0:
            return "-"
        return f"{size:.4f}"

    def create_table(self, headers: List[str], data: List[List[str]]) -> str:
        """Создание красивой таблицы"""
        if not data:
            return "Нет данных"

        col_widths = []
        for i in range(len(headers)):
            max_width = len(headers[i])
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

    async def get_current_price(self, symbol: str) -> Optional[float]:
        """Получить текущую цену для символа"""
        try:
            from analyzer.core.api_client_bybit import BybitAPIClient
            api_config = self.config.get('api', {})
            client = BybitAPIClient(api_config)
            return await client.get_current_price(symbol)
        except Exception as e:
            return None

    async def display_realtime_monitor(self):
        """Реалтайм мониторинг сигналов"""
        self._shutdown = False

        try:
            while not self._shutdown:
                self.clear_screen()
                self.print_header("THREE SCREEN ANALYZER - РЕАЛТАЙМ МОНИТОР")

                if not self.signal_repo:
                    print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                    break

                # Получаем сигналы с информацией о трейдах
                signals = await self.signal_repo.get_signals_with_trades(self.settings['signals_limit'])

                # Получаем статистику
                stats = await self.signal_repo.get_database_stats()

                # Показываем статистику
                print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИСТЕМЫ:{Style.RESET_ALL}")
                print(f"   Всего сигналов: {stats.get('total_signals', 0)}")
                print(f"   Активных сигналов: {stats.get('active_signals', 0)}")
                print(f"   BUY: {stats.get('buy_signals', 0)}  |  SELL: {stats.get('sell_signals', 0)}")
                print(f"   Закрыто сделок: {stats.get('closed_trades', 0)}")
                print(f"   Общий PnL: {self.format_pnl(stats.get('total_pnl', 0))}")
                print(f"   Win Rate: {stats.get('win_rate', 0):.1f}%")
                print()

                if not signals:
                    print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
                else:
                    # Подготавливаем данные для таблицы сигналов
                    table_data = []
                    for signal in signals:
                        time_str = self.format_time(signal.get('created_time', ''))
                        date_str = self.format_date(signal.get('created_time', ''))

                        # Получаем PnL если есть трейд
                        pnl_str = self.format_pnl(signal.get('trade_pnl', 0)) if signal.get(
                            'trade_pnl') is not None else "-"

                        table_data.append([
                            str(signal.get('id', '')),
                            signal.get('symbol', ''),
                            self.format_screen(signal.get('screen', 'M15')),
                            self.format_direction(signal.get('direction', '')),
                            self.format_trend(signal.get('trend_direction', '')),
                            self.format_status(signal.get('status', 'PENDING')),
                            self.format_confidence(signal.get('confidence', 0)),
                            self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                            self.format_price(signal.get('entry_price', 0)),
                            self.format_price(signal.get('stop_loss', 0)),
                            self.format_price(signal.get('take_profit', 0)),
                            pnl_str,
                            self.format_position_size(signal.get('position_size', 0)),
                            f"{date_str} {time_str}"
                        ])

                    headers = ["ID", "Монета", "Экран", "Тип", "Тренд", "Статус", "Уверенность", "R/R", "Entry", "SL",
                               "TP", "PnL", "Средства", "Время"]

                    table = self.create_table(headers, table_data)
                    print(table)

                print(f"\n{Fore.CYAN}🔄 Автообновление через {self.settings['refresh_interval']} сек...")
                print(f"{Fore.YELLOW}Нажмите Enter для возврата в меню{Style.RESET_ALL}")

                try:
                    await asyncio.wait_for(self.wait_for_enter(), timeout=self.settings['refresh_interval'])
                    print(f"\n{Fore.YELLOW}↩️ Возврат в меню...{Style.RESET_ALL}")
                    break
                except asyncio.TimeoutError:
                    continue

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        except Exception as e:
            logger.error(f"❌ Ошибка в реалтайм мониторе: {e}")
        finally:
            self._shutdown = False

    async def display_trades_table(self):
        """Таблица сделок (trades)"""
        try:
            if not self.trade_repo:
                print(f"{Fore.RED}❌ TradeRepository не инициализирован{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            trades = await self.trade_repo.get_closed_trades(limit=50)

            self.clear_screen()
            self.print_header("ИСТОРИЯ СДЕЛОК (TRADES)")

            if not trades:
                print(f"{Fore.YELLOW}📭 Нет закрытых сделок в базе данных{Style.RESET_ALL}")
            else:
                table_data = []
                total_pnl = 0
                winning_trades = 0

                for trade in trades:
                    pnl = trade.get('pnl', 0)
                    total_pnl += pnl
                    if pnl > 0:
                        winning_trades += 1

                    time_str = self.format_time(trade.get('closed_at', ''))
                    date_str = self.format_date(trade.get('closed_at', ''))

                    table_data.append([
                        str(trade.get('id', '')),
                        trade.get('symbol', ''),
                        self.format_direction(trade.get('direction', '')),
                        self.format_price(trade.get('entry_price', 0)),
                        self.format_price(trade.get('close_price', 0)),
                        self.format_pnl(pnl),
                        f"{(pnl / trade.get('entry_price', 1) * 100) if trade.get('entry_price', 0) > 0 else 0:.2f}%",
                        trade.get('close_reason', '-'),
                        f"{date_str} {time_str}"
                    ])

                headers = ["ID", "Монета", "Тип", "Entry", "Close", "PnL", "PnL %", "Причина", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                # Статистика
                total_trades = len(trades)
                win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
                avg_pnl = total_pnl / total_trades if total_trades > 0 else 0

                print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}📊 СТАТИСТИКА ПО СДЕЛКАМ:{Style.RESET_ALL}")
                print(f"   Всего сделок:  {total_trades}")
                print(f"   Прибыльных:    {winning_trades} ({win_rate:.1f}%)")
                print(f"   Убыточных:     {total_trades - winning_trades} ({100 - win_rate:.1f}%)")
                print(f"   Общий PnL:     {self.format_pnl(total_pnl)}")
                print(f"   Средняя PnL:   {self.format_pnl(avg_pnl)}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()

        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def display_signal_details(self, signal_id: int):
        """Детальная информация о сигнале (полная переработка)"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signal = await self.signal_repo.get_signal_by_id(signal_id)
            if not signal:
                print(f"{Fore.RED}❌ Сигнал #{signal_id} не найден{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            # Получаем текущую цену
            current_price = await self.get_current_price(signal.get('symbol', ''))
            if not current_price:
                current_price = signal.get('entry_price', 0)

            # Получаем трейд если есть
            trade = None
            if self.trade_repo:
                trade = await self.trade_repo.get_trade_by_signal_id(signal_id)

            self.clear_screen()
            self.print_header(f"ДЕТАЛИ СИГНАЛА #{signal_id}")

            # ========== ОСНОВНАЯ ИНФОРМАЦИЯ ==========
            print(f"{Fore.CYAN}🎯 ОСНОВНАЯ ИНФОРМАЦИЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Монета:        {Fore.WHITE}{signal.get('symbol', 'N/A')}{Style.RESET_ALL}")
            print(f"  Тип сигнала:   {self.format_direction(signal.get('direction', ''))}")
            print(f"  Экран:         {self.format_screen(signal.get('screen', 'M15'))} (сигнальный таймфрейм)")
            print(f"  Статус:        {self.format_status(signal.get('status', ''))}")
            print(f"  Уверенность:   {self.format_confidence(signal.get('confidence', 0))}")
            print(f"  Стратегия:     {Fore.WHITE}{signal.get('strategy', 'Three Screen')}{Style.RESET_ALL}")

            # ========== ТРЕНД (D1) ==========
            print(f"\n{Fore.CYAN}📊 ТРЕНД (D1){Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            trend_direction = signal.get('trend_direction', '')
            if trend_direction:
                trend_emoji = "🐂" if trend_direction.upper() == 'BULL' else "🐻"
                print(f"  Направление:   {self.format_trend(trend_direction)} {trend_emoji}")
            else:
                print(f"  Направление:   {Fore.WHITE}—{Style.RESET_ALL}")
            print(f"  Сила тренда:   {Fore.WHITE}{signal.get('trend_strength', '—')}{Style.RESET_ALL}")

            # ========== ЗОНЫ ВХОДА (H4) ==========
            print(f"\n{Fore.CYAN}🎯 ЗОНЫ ВХОДА (H4){Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Цена входа:    {Fore.WHITE}{self.format_price(signal.get('entry_price', 0))}{Style.RESET_ALL}")
            if signal.get('signal_subtype') == 'LIMIT':
                print(f"  Тип ордера:    {Fore.YELLOW}Лимитный (LIMIT){Style.RESET_ALL} — ожидание цены")

            # ========== СИГНАЛ (M15) ==========
            print(f"\n{Fore.CYAN}⚡ СИГНАЛ (M15){Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Паттерн:       {Fore.WHITE}{signal.get('trigger_pattern', '—')}{Style.RESET_ALL}")
            print(f"  Сила сигнала:  {Fore.WHITE}{signal.get('signal_strength', '—')}{Style.RESET_ALL}")
            print(f"  Тип сигнала:   {Fore.WHITE}{signal.get('signal_subtype', 'LIMIT')}{Style.RESET_ALL}")

            # ========== ТОРГОВЫЕ ПАРАМЕТРЫ ==========
            print(f"\n{Fore.CYAN}💸 ТОРГОВЫЕ ПАРАМЕТРЫ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            order_type = signal.get('order_type', 'LIMIT')
            if order_type == 'LIMIT':
                print(f"  Тип ордера:    {Fore.YELLOW}Лимитный (LIMIT){Style.RESET_ALL}")
            else:
                print(f"  Тип ордера:    {Fore.GREEN}Рыночный (MARKET){Style.RESET_ALL}")
            print(f"  Цена входа:    {Fore.WHITE}{self.format_price(signal.get('entry_price', 0))}{Style.RESET_ALL}")
            print(f"  Stop Loss:     {Fore.RED}{self.format_price(signal.get('stop_loss', 0))}{Style.RESET_ALL}")
            print(f"  Take Profit:   {Fore.GREEN}{self.format_price(signal.get('take_profit', 0))}{Style.RESET_ALL}")
            print(f"  Risk/Reward:   {self.format_rr_ratio(signal.get('risk_reward_ratio', 0))}")
            print(f"  Риск на сделку:{Fore.WHITE} {signal.get('risk_pct', 0):.2f}%{Style.RESET_ALL}")

            # ========== РАЗМЕР ПОЗИЦИИ ==========
            position_size = signal.get('position_size', 0)
            if position_size > 0:
                print(f"  Объём сделки:  {Fore.WHITE}{position_size:.4f}{Style.RESET_ALL}")

            # ========== ТЕКУЩАЯ СИТУАЦИЯ ==========
            print(f"\n{Fore.CYAN}📈 ТЕКУЩАЯ СИТУАЦИЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            print(f"  Текущая цена:  {Fore.WHITE}{self.format_price(current_price)}{Style.RESET_ALL}")

            entry = signal.get('entry_price', 0)
            if entry > 0 and current_price > 0:
                distance_to_entry = current_price - entry
                distance_pct = (distance_to_entry / entry) * 100

                if signal.get('direction', '').upper() == 'BUY':
                    if distance_to_entry > 0:
                        status_emoji = "📈"
                        status_text = "цена выше входа"
                    else:
                        status_emoji = "📉"
                        status_text = "цена ниже входа"
                else:
                    if distance_to_entry < 0:
                        status_emoji = "📈"
                        status_text = "цена ниже входа (SELL)"
                    else:
                        status_emoji = "📉"
                        status_text = "цена выше входа (SELL)"

                print(f"  Расстояние до входа: {self.format_price(distance_to_entry)} ({distance_pct:+.2f}%)")
                print(f"    → {status_emoji} {status_text}")

                # Расстояние до SL и TP
                sl = signal.get('stop_loss', 0)
                tp = signal.get('take_profit', 0)

                if sl > 0:
                    if signal.get('direction', '').upper() == 'BUY':
                        sl_distance = current_price - sl
                        sl_pct = (sl_distance / sl) * 100 if sl > 0 else 0
                    else:
                        sl_distance = sl - current_price
                        sl_pct = (sl_distance / sl) * 100 if sl > 0 else 0
                    print(f"  Расстояние до SL: {self.format_price(sl_distance)} ({sl_pct:+.2f}%)")

                if tp > 0:
                    if signal.get('direction', '').upper() == 'BUY':
                        tp_distance = tp - current_price
                        tp_pct = (tp_distance / current_price) * 100 if current_price > 0 else 0
                    else:
                        tp_distance = current_price - tp
                        tp_pct = (tp_distance / current_price) * 100 if current_price > 0 else 0
                    print(f"  Расстояние до TP: {self.format_price(tp_distance)} ({tp_pct:+.2f}%)")

            # ========== ВРЕМЯ ==========
            print(f"\n{Fore.CYAN}⏱ ВРЕМЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            created_utc = signal.get('created_time', '')
            if created_utc:
                local_dt = self.utc_to_local(created_utc)
                print(f"  Создан:        {Fore.WHITE}{created_utc} (UTC){Style.RESET_ALL}")
                print(
                    f"  Локальное:     {Fore.WHITE}{local_dt.strftime('%d.%m.%Y %H:%M:%S')} (UTC+{self.timezone_offset}){Style.RESET_ALL}")

            expiration = signal.get('expiration_time')
            if expiration:
                exp_local = self.utc_to_local(expiration)
                now_local = datetime.now()
                if exp_local > now_local:
                    remaining = exp_local - now_local
                    hours = remaining.seconds // 3600
                    minutes = (remaining.seconds % 3600) // 60
                    print(
                        f"  Истекает:      {Fore.WHITE}{exp_local.strftime('%d.%m.%Y %H:%M:%S')} (через {hours}ч {minutes}м){Style.RESET_ALL}")
                else:
                    print(
                        f"  Истекает:      {Fore.RED}{exp_local.strftime('%d.%m.%Y %H:%M:%S')} (истек){Style.RESET_ALL}")

            # ========== ИНФОРМАЦИЯ О СДЕЛКЕ ==========
            if trade:
                print(f"\n{Fore.CYAN}📝 ИНФОРМАЦИЯ О СДЕЛКЕ{Style.RESET_ALL}")
                print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
                print(f"  Цена закрытия: {self.format_price(trade.get('close_price', 0))}")
                print(f"  PnL:           {self.format_pnl(trade.get('pnl', 0))}")
                print(f"  PnL %:         {trade.get('pnl_percent', 0):+.2f}%")
                print(f"  Причина:       {Fore.WHITE}{trade.get('close_reason', '—')}{Style.RESET_ALL}")
                if trade.get('closed_at'):
                    closed_local = self.utc_to_local(trade['closed_at'])
                    print(f"  Закрыта:       {closed_local.strftime('%d.%m.%Y %H:%M:%S')}")

            # ========== ПРИМЕЧАНИЯ ==========
            print(f"\n{Fore.CYAN}📝 ПРИМЕЧАНИЯ{Style.RESET_ALL}")
            print(f"{Fore.CYAN}────────────────────────────────────────────────{Style.RESET_ALL}")
            if signal.get('status') == 'PENDING':
                print(f"  • Лимитный ордер ожидает исполнения")
                print(f"  • Цена входа: {self.format_price(signal.get('entry_price', 0))}")
                if current_price > 0:
                    if signal.get('direction', '').upper() == 'BUY' and current_price <= signal.get('entry_price', 0):
                        print(f"  • Цена достигнута! Ордер будет исполнен при следующем мониторинге")
                    elif signal.get('direction', '').upper() == 'SELL' and current_price >= signal.get('entry_price',
                                                                                                       0):
                        print(f"  • Цена достигнута! Ордер будет исполнен при следующем мониторинге")
                    else:
                        print(f"  • Ожидаем достижения цены {self.format_price(signal.get('entry_price', 0))}")
            elif signal.get('status') == 'ACTIVE':
                print(f"  • Позиция открыта, ведётся мониторинг TP/SL")
                if current_price > 0 and signal.get('take_profit', 0) > 0:
                    if signal.get('direction', '').upper() == 'BUY':
                        dist_to_tp = signal.get('take_profit', 0) - current_price
                        if dist_to_tp > 0:
                            print(f"  • До TP осталось: {self.format_price(dist_to_tp)}")
                    else:
                        dist_to_tp = current_price - signal.get('take_profit', 0)
                        if dist_to_tp > 0:
                            print(f"  • До TP осталось: {self.format_price(dist_to_tp)}")
            elif signal.get('status') == 'CLOSED':
                if trade:
                    if trade.get('pnl', 0) > 0:
                        print(f"  • Сделка закрыта с прибылью! 🎉")
                    else:
                        print(f"  • Сделка закрыта с убытком")
                    print(f"  • Причина закрытия: {trade.get('close_reason', '—')}")

            print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата в меню...{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка при отображении деталей: {e}{Style.RESET_ALL}")
            import traceback
            traceback.print_exc()
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")

    async def display_all_signals(self):
        """Таблица всех сигналов"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signals = await self.signal_repo.get_signals(limit=50)

            self.clear_screen()
            self.print_header("ВСЕ СИГНАЛЫ")

            if not signals:
                print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
            else:
                table_data = []
                for signal in signals:
                    time_str = self.format_time(signal.get('created_time', ''))
                    date_str = self.format_date(signal.get('created_time', ''))

                    table_data.append([
                        str(signal.get('id', '')),
                        signal.get('symbol', ''),
                        self.format_screen(signal.get('screen', 'M15')),
                        self.format_direction(signal.get('direction', '')),
                        self.format_trend(signal.get('trend_direction', '')),
                        self.format_status(signal.get('status', 'PENDING')),
                        self.format_confidence(signal.get('confidence', 0)),
                        self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                        self.format_price(signal.get('entry_price', 0)),
                        self.format_price(signal.get('stop_loss', 0)),
                        self.format_price(signal.get('take_profit', 0)),
                        self.format_position_size(signal.get('position_size', 0)),
                        f"{date_str} {time_str}"
                    ])

                headers = ["ID", "Монета", "Экран", "Тип", "Тренд", "Статус", "Уверенность", "R/R", "Entry", "SL", "TP",
                           "Средства", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                print(f"\n{Fore.CYAN}📈 СТАТИСТИКА:{Style.RESET_ALL}")
                active_count = sum(1 for s in signals if s.get('status', '').lower() in ['pending', 'active'])
                buy_count = sum(1 for s in signals if s.get('direction', '').lower() == 'buy')
                sell_count = sum(1 for s in signals if s.get('direction', '').lower() == 'sell')
                limit_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'LIMIT')
                instant_count = sum(1 for s in signals if s.get('signal_subtype', '') == 'INSTANT')

                print(f"   Всего сигналов: {len(signals)}")
                print(f"   Активных: {active_count}")
                print(f"   BUY: {buy_count} | SELL: {sell_count}")
                print(f"   LIMIT: {limit_count} | INSTANT: {instant_count}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")

        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def wait_for_enter(self):
        """Ожидание нажатия Enter в асинхронном режиме"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, input)

    async def main_menu(self):
        """Главное меню монитора"""
        while True:
            self.clear_screen()
            self.print_header("THREE SCREEN ANALYZER - МОНИТОР СИГНАЛОВ (v1.3.1)")

            print(f"{Fore.YELLOW}📋 ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:{Style.RESET_ALL}")
            print("1. 📊 Реалтайм монитор (автообновление)")
            print("2. 📋 Таблица всех сигналов")
            print("3. 📈 Таблица сделок (trades)")
            print("4. 🔍 Детали сигнала по ID")
            print("5. 📊 Статистика БД")
            print("6. 🚪 Выход")
            print()
            print(f"{Fore.CYAN}🕐 Локальное время: UTC+{self.timezone_offset}{Style.RESET_ALL}")

            try:
                choice = input(f"\n{Fore.CYAN}🎯 Выбор (1-6): {Style.RESET_ALL}").strip()

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
                    await self.display_database_stats()
                elif choice == '6':
                    print(f"{Fore.GREEN}👋 До свидания!{Style.RESET_ALL}")
                    break
                else:
                    print(f"{Fore.RED}❌ Неверный выбор!{Style.RESET_ALL}")
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                print(f"\n{Fore.YELLOW}🛑 Выход...{Style.RESET_ALL}")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в главном меню: {e}")
                await asyncio.sleep(1)

    async def display_database_stats(self):
        """Отображение статистики БД"""
        if not self.signal_repo:
            print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
            return

        stats = await self.signal_repo.get_database_stats()

        self.clear_screen()
        self.print_header("СТАТИСТИКА БАЗЫ ДАННЫХ")

        print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИГНАЛОВ:{Style.RESET_ALL}")
        print(f"  Всего сигналов:      {stats.get('total_signals', 0)}")
        print(f"  Активных сигналов:   {stats.get('active_signals', 0)}")
        print(f"  BUY сигналов:        {stats.get('buy_signals', 0)}")
        print(f"  SELL сигналов:       {stats.get('sell_signals', 0)}")

        subtypes = stats.get('subtypes_stats', {})
        if subtypes:
            print(f"\n{Fore.YELLOW}📊 СТАТИСТИКА ПО ТИПАМ СИГНАЛОВ:{Style.RESET_ALL}")
            for subtype, count in subtypes.items():
                print(f"  {subtype}: {count}")

        print(f"\n{Fore.YELLOW}📈 СТАТИСТИКА СДЕЛОК:{Style.RESET_ALL}")
        print(f"  Закрытых сделок:      {stats.get('closed_trades', 0)}")
        print(f"  Общий PnL:            {self.format_pnl(stats.get('total_pnl', 0))}")
        print(f"  Win Rate:             {stats.get('win_rate', 0):.1f}%")

        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def run(self):
        """Запуск монитора"""
        try:
            # Инициализируем репозитории
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не найден{Style.RESET_ALL}")
                return

            if not await self.signal_repo.initialize():
                print(f"{Fore.RED}❌ Не удалось инициализировать БД{Style.RESET_ALL}")
                return

            if self.trade_repo:
                await self.trade_repo.initialize()

            print(f"{Fore.GREEN}✅ База данных подключена: {self.db_path}{Style.RESET_ALL}")
            await self.main_menu()
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}❌ Критическая ошибка: {e}{Style.RESET_ALL}")


# Точка входа
async def main():
    print(f"{Fore.GREEN}🚀 Запуск Three Screen Analyzer Monitor (v1.3.1)...{Style.RESET_ALL}")

    # Устанавливаем переменную TERM чтобы избежать предупреждения
    os.environ['TERM'] = os.environ.get('TERM', 'xterm-256color')

    monitor = ThreeScreenMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}👋 Программа завершена{Style.RESET_ALL}")
    except Exception as e:
        print(f"{Fore.RED}💥 Фатальная ошибка: {e}{Style.RESET_ALL}")