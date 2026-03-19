#!/usr/bin/env python3
# monitor_three_screen.py
"""
🎯 МОНИТОР ДЛЯ THREE SCREEN ANALYZER
Адаптированная версия для отображения сигналов от ThreeScreenAnalyzer
"""


import asyncio
import logging
import os
import re
import yaml
from typing import List
from pathlib import Path
from datetime import datetime
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

        # Импортируем signal_repository
        try:
            from analyzer.core.signal_repository import signal_repository
            self.signal_repo = signal_repository
        except ImportError as e:
            print(f"❌ Ошибка импорта signal_repository: {e}")
            self.signal_repo = None

        # Получаем путь к БД
        db_config = self.config.get('database', {})
        db_signals_config = self.config.get('database_signals', {})

        # Логика выбора пути
        db_path = None
        if isinstance(db_signals_config, dict) and db_signals_config.get('enabled', False):
            db_path = db_signals_config.get('path')

        if not db_path and isinstance(db_config, dict):
            db_path = db_config.get('path')

        if not db_path:
            db_path = 'data/trading_bot.db'

        # Нормализуем путь
        if not os.path.isabs(db_path):
            project_root = Path(__file__).parent
            self.db_path = str(project_root / db_path)
        else:
            self.db_path = db_path

        print(f"📊 Монитор использует БД: {self.db_path}")

        self._shutdown = False
        self.settings = {
            'refresh_interval': 5,
            'signals_limit': 20,
            'show_confidence': True,
            'show_rr_ratio': True
        }

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

    # ==================== ФОРМАТИРОВАНИЕ ====================

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
        if price is None:
            return "N/A"
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

    def format_time(self, timestamp_str: str) -> str:
        """Форматирование времени"""
        if not timestamp_str:
            return "-"
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime("%H:%M")
        except:
            return timestamp_str[11:16] if ':' in timestamp_str else '-'

    def format_date(self, timestamp_str: str) -> str:
        """Форматирование даты"""
        if not timestamp_str:
            return "-"
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return dt.strftime("%m-%d")
        except:
            return timestamp_str[:10] if '-' in timestamp_str else '-'

    # ==================== МЕТОДЫ ТАБЛИЦ ====================

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

        # Верхняя граница
        top_border = "┌" + "┬".join("─" * width for width in col_widths) + "┐"
        result.append(top_border)

        # Заголовки
        header_line = "│"
        for i, header in enumerate(headers):
            padding = col_widths[i] - len(header)
            left_padding = padding // 2
            right_padding = padding - left_padding
            centered_header = " " * left_padding + header + " " * right_padding
            header_line += centered_header + "│"
        result.append(header_line)

        # Разделитель
        separator = "├" + "┼".join("─" * width for width in col_widths) + "┤"
        result.append(separator)

        # Данные
        for row in data:
            row_line = "│"
            for i in range(len(headers)):
                cell = row[i] if i < len(row) else ""
                visible_cell = str(cell)
                padding = col_widths[i] - self.get_visible_length(visible_cell)
                aligned_cell = visible_cell + " " * padding
                row_line += aligned_cell + "│"
            result.append(row_line)

        # Нижняя граница
        bottom_border = "└" + "┴".join("─" * width for width in col_widths) + "┘"
        result.append(bottom_border)

        return "\n".join(result)

    # ==================== ОТОБРАЖЕНИЕ ДАННЫХ ====================

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

                # Получаем сигналы из БД
                signals = await self.signal_repo.get_signals_with_trades(self.settings['signals_limit'])

                # Получаем статистику
                stats = await self.signal_repo.get_database_stats()

                # Показываем статистику
                print(f"{Fore.YELLOW}📊 СТАТИСТИКА СИСТЕМЫ:{Style.RESET_ALL}")
                print(f"   Всего сигналов: {stats.get('total_signals', 0)}")
                print(f"   Активных сигналов: {stats.get('active_signals', 0)}")
                print(f"   Three Screen сигналов: {stats.get('three_screen_signals', 0)}")
                print(f"   BUY сигналов: {stats.get('buy_signals', 0)}")
                print(f"   SELL сигналов: {stats.get('sell_signals', 0)}")
                print()

                if not signals:
                    print(f"{Fore.YELLOW}📭 Нет сигналов в базе данных{Style.RESET_ALL}")
                else:
                    # Подготавливаем данные для таблицы
                    table_data = []
                    for signal in signals:
                        # Форматируем время и дату
                        time_str = self.format_time(signal.get('created_time', ''))
                        date_str = self.format_date(signal.get('created_time', ''))

                        table_data.append([
                            str(signal.get('id', '')),
                            signal.get('symbol', ''),
                            self.format_direction(signal.get('direction', '')),
                            self.format_status(signal.get('status', 'PENDING')),
                            self.format_confidence(signal.get('confidence', 0)),
                            self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                            self.format_price(signal.get('entry_price', 0)),
                            self.format_price(signal.get('stop_loss', 0)),
                            self.format_price(signal.get('take_profit', 0)),
                            f"{signal.get('trend_direction', '-')}",
                            f"{signal.get('signal_strength', '-')}",
                            time_str,
                            date_str
                        ])

                    # Заголовки таблицы
                    headers = ["ID", "Symbol", "Тип", "Статус", "Уверенность", "R/R", "Entry", "SL", "TP", "Тренд",
                               "Сила", "Время", "Дата"]

                    # Выводим таблицу
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

    async def display_signal_details(self, signal_id: int):
        """Детальная информация о сигнале"""
        try:
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не инициализирован{Style.RESET_ALL}")
                return

            signal = await self.signal_repo.get_signal_by_id(signal_id)
            if not signal:
                print(f"{Fore.RED}❌ Сигнал #{signal_id} не найден{Style.RESET_ALL}")
                input(f"\n{Fore.GREEN}Нажмите Enter для возврата...{Style.RESET_ALL}")
                return

            self.clear_screen()
            self.print_header(f"ДЕТАЛИ СИГНАЛА #{signal_id}")

            print(f"{Fore.CYAN}🎯 ОСНОВНАЯ ИНФОРМАЦИЯ:{Style.RESET_ALL}")
            print(f"  Символ: {Fore.WHITE}{signal.get('symbol', 'N/A')}{Style.RESET_ALL}")
            print(f"  Направление: {self.format_direction(signal.get('direction', ''))}")
            print(f"  Статус: {self.format_status(signal.get('status', ''))}")
            print(f"  Уверенность: {self.format_confidence(signal.get('confidence', 0))}")
            print(f"  Стратегия: {Fore.WHITE}{signal.get('strategy', 'three_screen')}{Style.RESET_ALL}")
            print(f"  Создан: {Fore.WHITE}{signal.get('created_time', 'N/A')}{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}📊 ТОРГОВЫЕ ПАРАМЕТРЫ:{Style.RESET_ALL}")
            print(f"  Цена входа: {Fore.WHITE}{self.format_price(signal.get('entry_price', 0))}{Style.RESET_ALL}")
            print(f"  Stop Loss: {Fore.WHITE}{self.format_price(signal.get('stop_loss', 0))}{Style.RESET_ALL}")
            print(f"  Take Profit: {Fore.WHITE}{self.format_price(signal.get('take_profit', 0))}{Style.RESET_ALL}")
            print(f"  Risk/Reward: {self.format_rr_ratio(signal.get('risk_reward_ratio', 0))}")
            print(f"  Риск: {Fore.WHITE}{signal.get('risk_pct', 0):.2f}%{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}📈 АНАЛИЗ ТРЕНДА:{Style.RESET_ALL}")
            print(f"  Направление тренда: {Fore.WHITE}{signal.get('trend_direction', 'N/A')}{Style.RESET_ALL}")
            print(f"  Сила тренда: {Fore.WHITE}{signal.get('trend_strength', 'N/A')}{Style.RESET_ALL}")
            print(f"  Сила сигнала: {Fore.WHITE}{signal.get('signal_strength', 'N/A')}{Style.RESET_ALL}")
            print(f"  Триггер паттерн: {Fore.WHITE}{signal.get('trigger_pattern', 'N/A')}{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}📋 ДОПОЛНИТЕЛЬНО:{Style.RESET_ALL}")
            print(f"  ID: {Fore.WHITE}{signal.get('id', 'N/A')}{Style.RESET_ALL}")
            print(f"  Risk %: {Fore.WHITE}{signal.get('risk_pct', 0):.2f}%{Style.RESET_ALL}")
            print(f"  Создан: {Fore.WHITE}{signal.get('created_time', 'N/A')}{Style.RESET_ALL}")

            print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
            input(f"\n{Fore.GREEN}Нажмите Enter для возврата в меню...{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка при отображении деталей: {e}{Style.RESET_ALL}")
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
                        self.format_direction(signal.get('direction', '')),
                        self.format_status(signal.get('status', 'PENDING')),
                        self.format_confidence(signal.get('confidence', 0)),
                        self.format_rr_ratio(signal.get('risk_reward_ratio', 0)),
                        self.format_price(signal.get('entry_price', 0)),
                        self.format_price(signal.get('stop_loss', 0)),
                        self.format_price(signal.get('take_profit', 0)),
                        signal.get('trend_direction', '-'),
                        signal.get('signal_strength', '-'),
                        date_str,
                        time_str
                    ])

                headers = ["ID", "Symbol", "Тип", "Статус", "Уверенность", "R/R", "Entry", "SL", "TP", "Тренд", "Сила",
                           "Дата", "Время"]
                table = self.create_table(headers, table_data)
                print(table)

                print(f"\n{Fore.CYAN}📈 СТАТИСТИКА:{Style.RESET_ALL}")
                active_count = sum(1 for s in signals if s.get('status', '').lower() in ['pending', 'active'])
                buy_count = sum(1 for s in signals if s.get('direction', '').lower() == 'buy')
                sell_count = sum(1 for s in signals if s.get('direction', '').lower() == 'sell')

                print(f"   Всего сигналов: {len(signals)}")
                print(f"   Активных: {active_count}")
                print(f"   BUY: {buy_count}")
                print(f"   SELL: {sell_count}")

        except Exception as e:
            print(f"{Fore.RED}❌ Ошибка: {e}{Style.RESET_ALL}")

        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def wait_for_enter(self):
        """Ожидание нажатия Enter в асинхронном режиме"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, input)

    # ==================== ГЛАВНОЕ МЕНЮ ====================

    async def main_menu(self):
        """Главное меню монитора"""
        while True:
            self.clear_screen()
            self.print_header("THREE SCREEN ANALYZER - МОНИТОР СИГНАЛОВ")

            print(f"{Fore.YELLOW}📋 ВЫБЕРИТЕ РЕЖИМ РАБОТЫ:{Style.RESET_ALL}")
            print("1. 📊 Реалтайм монитор (автообновление)")
            print("2. 📋 Таблица всех сигналов")
            print("3. 🔍 Детали сигнала по ID")
            print("4. 📈 Статистика БД")
            print("5. 🚪 Выход")
            print()

            try:
                choice = input(f"{Fore.CYAN}🎯 Выбор (1-5): {Style.RESET_ALL}").strip()

                if choice == '1':
                    await self.display_realtime_monitor()
                elif choice == '2':
                    await self.display_all_signals()
                elif choice == '3':
                    try:
                        signal_id = int(input(f"{Fore.CYAN}🔢 Введите ID сигнала: {Style.RESET_ALL}"))
                        await self.display_signal_details(signal_id)
                    except ValueError:
                        print(f"{Fore.RED}❌ Введите корректный ID{Style.RESET_ALL}")
                        await asyncio.sleep(1)
                elif choice == '4':
                    await self.display_database_stats()
                elif choice == '5':
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

        print(f"{Fore.YELLOW}📊 ОБЩАЯ СТАТИСТИКА:{Style.RESET_ALL}")
        print(f"  Всего сигналов: {stats.get('total_signals', 0)}")
        print(f"  Активных сигналов: {stats.get('active_signals', 0)}")
        print(f"  Three Screen сигналов: {stats.get('three_screen_signals', 0)}")
        print(f"  BUY сигналов: {stats.get('buy_signals', 0)}")
        print(f"  SELL сигналов: {stats.get('sell_signals', 0)}")
        print(f"  Активных трейдов: {stats.get('active_trades', 0)}")
        print(f"  Закрытых трейдов: {stats.get('closed_trades', 0)}")
        print(f"  Общий PnL: ${stats.get('total_pnl', 0):+.2f}")

        print(f"\n{Fore.CYAN}{'=' * 80}{Style.RESET_ALL}")
        input(f"\n{Fore.GREEN}Нажмите Enter для продолжения...{Style.RESET_ALL}")

    async def run(self):
        """Запуск монитора"""
        try:
            # Инициализируем репозиторий
            if not self.signal_repo:
                print(f"{Fore.RED}❌ SignalRepository не найден{Style.RESET_ALL}")
                return

            if not await self.signal_repo.initialize():
                print(f"{Fore.RED}❌ Не удалось инициализировать БД{Style.RESET_ALL}")
                return

            print(f"{Fore.GREEN}✅ База данных подключена: {self.db_path}{Style.RESET_ALL}")
            await self.main_menu()
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}🛑 Монитор остановлен{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}❌ Критическая ошибка: {e}{Style.RESET_ALL}")


# Точка входа
async def main():
    print(f"{Fore.GREEN}🚀 Запуск Three Screen Analyzer Monitor...{Style.RESET_ALL}")

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