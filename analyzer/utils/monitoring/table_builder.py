# analyzer/utils/monitoring/table_builder.py
"""
📊 TABLE BUILDER - Построитель таблиц
"""

import re
from typing import List


class TableBuilder:
    """Построитель таблиц без цветов (только для данных)"""

    @staticmethod
    def strip_ansi(text: str) -> str:
        ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        return ansi_escape.sub('', str(text))

    @staticmethod
    def get_visible_length(text: str) -> int:
        return len(TableBuilder.strip_ansi(str(text)))

    def create_table(self, headers: List[str], data: List[List[str]], min_width: int = 2) -> str:
        if not data:
            return "Нет данных"

        col_widths = []
        for i, header in enumerate(headers):
            max_width = len(header)
            for row in data:
                if i < len(row):
                    visible_text = self.strip_ansi(str(row[i]))
                    max_width = max(max_width, len(visible_text))
            col_widths.append(max(max_width + min_width, min_width))

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