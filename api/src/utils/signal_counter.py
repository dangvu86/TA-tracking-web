import pandas as pd
import numpy as np
from typing import Dict, Tuple

# Oscillator signal names (from evaluate_oscillator_signals function)
OSCILLATOR_SIGNALS = ['RSI_Signal', 'Stochastic_Signal', 'CCI_Signal', 'ADX_Signal', 
                     'AO_Signal', 'Momentum_Signal', 'MACD_Signal', 'StochRSI_Signal', 
                     'Williams_R_Signal', 'BBP_Signal', 'UO_Signal']

# Moving Average signal names (from evaluate_ma_signals function)  
MA_SIGNALS = ['MA_10_Signal', 'MA_20_Signal', 'MA_30_Signal', 'MA_50_Signal', 
              'MA_100_Signal', 'MA_200_Signal', 'EMA_10_Signal', 'EMA_20_Signal', 
              'EMA_30_Signal', 'EMA_50_Signal', 'EMA_100_Signal', 'EMA_200_Signal',
              'VWMA_Signal', 'Hull_MA_Signal', 'Ichimoku_Signal']


def count_signals(signals_dict: Dict) -> Tuple[int, int, int, int]:
    """
    Count buy/sell signals for oscillators and moving averages
    
    Returns:
        Tuple of (osc_buy, osc_sell, ma_buy, ma_sell)
    """
    osc_buy = 0
    osc_sell = 0
    ma_buy = 0
    ma_sell = 0
    
    for signal_name, signal_value in signals_dict.items():
        if signal_name in OSCILLATOR_SIGNALS:
            if signal_value == 'Buy':
                osc_buy += 1
            elif signal_value == 'Sell':
                osc_sell += 1
        elif signal_name in MA_SIGNALS:
            if signal_value == 'Buy':
                ma_buy += 1
            elif signal_value == 'Sell':
                ma_sell += 1
                
    return osc_buy, osc_sell, ma_buy, ma_sell


def calculate_price_change(current_price: float, previous_price: float) -> float:
    """Calculate percentage price change"""
    if pd.isna(previous_price) or previous_price == 0:
        return 0.0
    return ((current_price - previous_price) / previous_price) * 100


def calculate_ratings(osc_buy: int, osc_sell: int, ma_buy: int, ma_sell: int) -> Tuple[float, float]:
    """
    Calculate Rating 1 and Rating 2 based on signal counts
    
    Args:
        osc_buy: Oscillator buy signals
        osc_sell: Oscillator sell signals  
        ma_buy: Moving average buy signals
        ma_sell: Moving average sell signals
        
    Returns:
        Tuple of (Rating 1, Rating 2)
    """
    # Rating 1 = Osc buy*2 - Osc sell + MA Buy*1 - MA Sell
    rating1 = osc_buy * 2 - osc_sell + ma_buy * 1 - ma_sell
    
    # Rating 2 = Osc buy*2 + MA Buy*1
    rating2 = osc_buy * 2 + ma_buy * 1
    
    return rating1, rating2