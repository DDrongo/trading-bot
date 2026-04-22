# analyzer/utils/monitoring/base.py
"""
📊 BASE - Базовый класс для мониторов
"""

import re
import os


class MonitorBase:
    """Базовый класс с методами форматирования и цветами"""

    def __init__(self, color_enabled: bool = True):
        self.color_enabled = color_enabled
        self._init_colors()

    def _init_colors(self):
        if self.color_enabled:
            try:
                from colorama import init, Fore, Style
                init()
                self.Fore = Fore
                self.Style = Style
            except ImportError:
                self._set_null_colors()
        else:
            self._set_null_colors()

    def _set_null_colors(self):
        class NullColor:
            def __getattr__(self, name):
                return ''
        self.Fore = NullColor()
        self.Style = NullColor()

    @staticmethod
    def strip_ansi(text: str) -> str:
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', str(text))

    def get_visible_length(self, text: str) -> int:
        return len(self.strip_ansi(str(text)))

    @staticmethod
    def clear_screen():
        os.system('cls' if os.name == 'nt' else 'clear')

    @staticmethod
    def print_header(title: str, width: int = 90):
        from colorama import Fore, Style
        print(f"\n{Fore.CYAN}{Style.BRIGHT}{'=' * width}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{title}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{Style.BRIGHT}{'=' * width}{Style.RESET_ALL}")

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

    def format_pnl(self, pnl: float) -> str:
        if pnl is None:
            return "-"
        if abs(pnl) < 0.01 and pnl != 0:
            return f"{self.Fore.YELLOW}{pnl:.6f}{self.Style.RESET_ALL}"
        if pnl > 0:
            return f"{self.Fore.GREEN}+{pnl:.2f}{self.Style.RESET_ALL}"
        elif pnl < 0:
            return f"{self.Fore.RED}{pnl:.2f}{self.Style.RESET_ALL}"
        else:
            return f"{self.Fore.YELLOW}0.00{self.Style.RESET_ALL}"

    def format_direction(self, direction: str) -> str:
        direction_lower = direction.lower() if direction else ''
        if direction_lower in ['buy', 'long']:
            return f"{self.Fore.GREEN}BUY{self.Style.RESET_ALL}"
        elif direction_lower in ['sell', 'short']:
            return f"{self.Fore.RED}SELL{self.Style.RESET_ALL}"
        return direction or 'N/A'

    def format_status(self, status: str) -> str:
        status_lower = status.lower() if status else ''
        if status_lower == 'watch':
            return f"{self.Fore.YELLOW}WATCH{self.Style.RESET_ALL}"
        elif status_lower == 'active':
            return f"{self.Fore.CYAN}ACTIVE{self.Style.RESET_ALL}"
        elif status_lower in ['closed', 'completed']:
            return f"{self.Fore.MAGENTA}CLOSED{self.Style.RESET_ALL}"
        elif status_lower == 'rejected':
            return f"{self.Fore.RED}REJECTED{self.Style.RESET_ALL}"
        else:
            return status or 'N/A'

    def format_entry_type(self, entry_type: str) -> str:
        if not entry_type:
            return "-"
        entry_type_upper = entry_type.upper()
        if entry_type_upper == 'SNIPER':
            return f"{self.Fore.GREEN}SNIPER{self.Style.RESET_ALL}"
        elif entry_type_upper == 'TREND':
            return f"{self.Fore.CYAN}TREND{self.Style.RESET_ALL}"
        elif entry_type_upper == 'LEGACY':
            return f"{self.Fore.YELLOW}LEGACY{self.Style.RESET_ALL}"
        else:
            return entry_type

    def format_score(self, score: int) -> str:
        if score is None or score == 0:
            return "-"
        if score >= 7:
            return f"{self.Fore.GREEN}{score}/8{self.Style.RESET_ALL}"
        elif score >= 5:
            return f"{self.Fore.YELLOW}{score}/8{self.Style.RESET_ALL}"
        else:
            return f"{self.Fore.RED}{score}/8{self.Style.RESET_ALL}"

    def format_rr_ratio(self, rr: float) -> str:
        if rr is None or rr == 0:
            return "-"
        if rr >= 3.0:
            color = self.Fore.GREEN
        elif rr >= 2.0:
            color = self.Fore.YELLOW
        else:
            color = self.Fore.RED
        return f"{color}{rr:.2f}:1{self.Style.RESET_ALL}"

    def format_zone(self, zone_low: float, zone_high: float) -> str:
        if zone_low is None or zone_high is None or zone_low == 0 or zone_high == 0:
            return "-"
        return f"{self.format_price(zone_low)}-{self.format_price(zone_high)}"