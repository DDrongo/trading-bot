# core/__init__.py
"""
🎯 Пакет core - основные модули анализатора
"""

# Экспортируем все основные классы для удобного импорта
from .api_client_bybit import APIClient, BybitAPIClient
from .data_classes import (
    Direction, SignalStatus, TradeStatus,
    Screen1Result, Screen2Result, Screen3Result,
    ThreeScreenAnalysis, Signal, Trade, PaperTrade
)
from .event_bus import EventType, Event, EventBus, event_bus
from .prefilter_liquidity import LiquidityPrefilter, PrefilterResult, LiquidityMetrics
from .screen1_trend_analyzer import Screen1TrendAnalyzer, Screen1Result
from .screen2_entry_zones import Screen2EntryZonesAnalyzer, Screen2Result
from .screen3_signal_generator import Screen3SignalGenerator, Screen3Result, PatternType
from .signal_repository import SignalRepository, signal_repository
from .three_screen_analyzer import ThreeScreenAnalyzer
from .orchestrator import AnalysisOrchestrator, AnalysisSession

__all__ = [
    # API
    'APIClient',
    'BybitAPIClient',

    # Data Classes
    'Direction',
    'SignalStatus',
    'TradeStatus',
    'Screen1Result',
    'Screen2Result',
    'Screen3Result',
    'ThreeScreenAnalysis',
    'Signal',
    'Trade',
    'PaperTrade',

    # Event Bus
    'EventType',
    'Event',
    'EventBus',
    'event_bus',

    # Modules
    'LiquidityPrefilter',
    'PrefilterResult',
    'LiquidityMetrics',
    'Screen1TrendAnalyzer',
    'Screen2EntryZonesAnalyzer',
    'Screen3SignalGenerator',
    'PatternType',
    'SignalRepository',
    'ThreeScreenAnalyzer',
    'AnalysisOrchestrator',
    'AnalysisSession'
]