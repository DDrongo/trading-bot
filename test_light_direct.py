import asyncio
import yaml
import logging
from analyzer.core.light_trader import LightTrader
from analyzer.core.data_provider import data_provider

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def test():
    with open('analyzer/config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Инициализируем DataProvider
    data_provider.configure(config)
    
    # Создаём LightTrader напрямую
    light_trader = LightTrader(config, data_provider)
    
    print('\n🔍 Анализируем BTCUSDT напрямую через LightTrader...\n')
    
    result = await light_trader.analyze_symbol('BTCUSDT')
    
    if result and result.should_trade:
        print('=' * 50)
        print('✅✅✅ СИГНАЛ НАЙДЕН!')
        print('=' * 50)
        print(f'   Направление: {result.screen3.signal_type}')
        print(f'   Вход: {result.screen3.entry_price:.2f}')
        print(f'   SL: {result.screen3.stop_loss:.2f}')
        print(f'   TP: {result.screen3.take_profit:.2f}')
        print(f'   Паттерн: {result.screen3.trigger_pattern}')
        print(f'   R/R: {result.risk_reward_ratio:.2f}:1')
        print(f'   Уверенность: {result.overall_confidence:.1%}')
        print('=' * 50)
    else:
        print('=' * 50)
        print('❌ Сигнал не сгенерирован')
        print('=' * 50)
        if result:
            print(f'   Тренд D1: {result.screen1.trend_direction}')
            ema20 = result.screen1.indicators.get('ema_20', 0)
            ema50 = result.screen1.indicators.get('ema_50', 0)
            print(f'   EMA20: {ema20:.2f}')
            print(f'   EMA50: {ema50:.2f}')
            if result.screen3 and result.screen3.rejection_reason:
                print(f'   Причина: {result.screen3.rejection_reason}')
    
    await data_provider.close()

asyncio.run(test())
