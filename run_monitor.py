# run_monitor.py
#!/usr/bin/env python3
"""
🎯 ЗАПУСК МОНИТОРА ДЛЯ THREE SCREEN ANALYZER
Запускать отдельно от основного бота для просмотра сигналов
"""

import asyncio
import sys
import os

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from monitor_three_screen import main

if __name__ == "__main__":
    asyncio.run(main())