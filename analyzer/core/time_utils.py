# analyzer/core/time_utils.py
"""
🕐 УТИЛИТЫ ДЛЯ РАБОТЫ СО ВРЕМЕНЕМ
Единый источник времени для всего бота
"""

from datetime import datetime, timedelta
from typing import Optional

# Часовой пояс бота (UTC+3)
TIMEZONE_OFFSET = 3


def now() -> datetime:
    """
    Текущее время в локальной зоне бота (UTC+3)
    Использовать ВЕЗДЕ вместо datetime.now()
    """
    return datetime.now() + timedelta(hours=TIMEZONE_OFFSET)


def utc_now() -> datetime:
    """
    Текущее время в UTC
    Использовать для сохранения в БД
    """
    return datetime.utcnow()


def to_local(utc_time: datetime) -> datetime:
    """
    Конвертировать UTC в локальное время бота
    """
    return utc_time + timedelta(hours=TIMEZONE_OFFSET)


def to_utc(local_time: datetime) -> datetime:
    """
    Конвертировать локальное время в UTC
    """
    return local_time - timedelta(hours=TIMEZONE_OFFSET)


def iso_local() -> str:
    """
    ISO строка локального времени
    """
    return now().isoformat()


def iso_utc() -> str:
    """
    ISO строка UTC времени
    """
    return datetime.utcnow().isoformat()


def parse_iso_to_local(iso_str: str) -> Optional[datetime]:
    """
    Парсит ISO строку и возвращает локальное время
    """
    try:
        if 'Z' in iso_str or '+' in iso_str:
            utc_time = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        else:
            utc_time = datetime.fromisoformat(iso_str)
        return to_local(utc_time)
    except Exception:
        return None


def format_local(dt: datetime, fmt: str = "%d.%m.%Y %H:%M:%S") -> str:
    """
    Форматирует локальное время для отображения
    """
    return dt.strftime(fmt)


__all__ = [
    'now',
    'utc_now',
    'to_local',
    'to_utc',
    'iso_local',
    'iso_utc',
    'parse_iso_to_local',
    'format_local',
    'TIMEZONE_OFFSET'
]