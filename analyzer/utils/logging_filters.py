# analyzer/utils/logging_filters.py

import logging


class ConsoleFilter(logging.Filter):
    """
    Фильтр ТОЛЬКО для консоли.
    Показываем только названия процессов и итоги.
    """

    # Разрешённые сообщения в консоль (только суть)
    ALLOWED_MESSAGES = [
        "🚀 Инициализация...",
        "✅ Готов к работе",
        "📡 Получено монет:",
        "🔍 Фильтруем монеты по ликвидности...",
        "🕳️ Ищем FVG зоны...",
        "💧 Ищем пулы ликвидности...",
        "🔬 Анализируем монеты...",
        "📈 Ищем паттерны на M15...",
        "💰 Открываем позицию...",
        "👀 WATCH:",
        "⚡ СИГНАЛОВ:",
        "💰 PnL:",
        "🎯 СИГНАЛ",
        "📈 СИГНАЛ",
        "📊 СИГНАЛ",
        "❌ Ошибка:",
        "✅ Закрыта позиция",
        "🛑 Остановлено",
        "⏳ Ждём",
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        # В консоль показываем только signal_generator
        if record.name != 'signal_generator':
            return False

        msg = record.getMessage()

        # Проверяем разрешённые сообщения
        for allowed in self.ALLOWED_MESSAGES:
            if allowed in msg:
                return True

        return False


class DetailedFileFilter(logging.Filter):
    """Для файла — пропускаем всё"""

    def filter(self, record: logging.LogRecord) -> bool:
        return True