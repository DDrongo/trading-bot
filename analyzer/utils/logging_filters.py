# analyzer/utils/logging_filters.py
"""
🎯 Фильтры для логирования
Отсеивают спам-сообщения из консоли
"""

import logging


class ConsoleFilter(logging.Filter):
    """
    Фильтр для консольного вывода.
    Пропускает только важные сообщения, отсеивает отладочный спам.
    """

    # Сообщения, которые НЕ показываем в консоли
    SUPPRESS_PATTERNS = [
        # Префильтр - спам о непрохождении
        "не прошел ликвидность",
        "цена слишком низкая",
        "недостаточный объем",
        "слишком большой спред",
        "недостаточная глубина",
        "В черном списке",
        "Не удалось получить тикер",
        "пустой или некорректный стакан",
        "символ невалидный",
        "Ширина диапазона",

        # Screen1 - отладочная информация
        "Бычьи условия:",
        "Медвежьи условия:",
        "Обнаружена структура",
        "Сильный уровень поддержки",
        "Уровень сопротивления",
        "Расчет MACD",
        "Расчет ADX",

        # API - технические сообщения
        "Кэш попадание",
        "Получено",
        "свечей",
        "Загружена exchange_info",
        "Использую кэш",

        # Общие отладочные
        "DEBUG:",
        "Получен стакан",
        "Request delay",
        "Max concurrent",
    ]

    # Сообщения, которые ВСЕГДА показываем (даже если содержат SUPPRESS_PATTERNS)
    ALLOW_PATTERNS = [
        "СИГНАЛ",
        "СИГНАЛ НАЙДЕН",
        "LIGHT СИГНАЛ",
        "ПОЗИЦИЯ",
        "ОТКРЫТА",
        "ЗАКРЫТА",
        "СДЕЛКА",
        "ОШИБКА",
        "КРИТИЧЕСКАЯ",
        "WATCH сигнал сохранён",
        "WATCH → ACTIVE",
        "Баланс",
        "PnL",
        "✅",  # Важные успехи
        "❌",  # Важные ошибки
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Решает, показывать ли сообщение в консоли
        """
        message = record.getMessage()

        # Уровень WARNING и выше - показываем всегда
        if record.levelno >= logging.WARNING:
            return True

        # Проверяем, есть ли важные паттерны
        for allow_pattern in self.ALLOW_PATTERNS:
            if allow_pattern in message:
                return True

        # Проверяем, нужно ли подавить сообщение
        for suppress_pattern in self.SUPPRESS_PATTERNS:
            if suppress_pattern.lower() in message.lower():
                return False

        # По умолчанию показываем (для INFO)
        return True


class DetailedFileFilter(logging.Filter):
    """
    Фильтр для файлового вывода - пропускает всё
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return True