import pandas as pd
from io import BytesIO
from datetime import datetime


def create_summary_dataframe(stock_data: dict, selected_date: datetime) -> pd.DataFrame:
    """
    Create a summary DataFrame for all stocks with indicators and signals
    
    Args:
        stock_data: Dictionary with stock data and signals
        selected_date: Selected analysis date
    
    Returns:
        DataFrame with summary data
    """
    summary_rows = []
    
    for ticker, data in stock_data.items():
        if not data or 'indicators' not in data or 'signals' not in data:
            continue
            
        indicators = data['indicators']
        signals = data['signals']
        signal_summary = data.get('signal_summary', {})
        
        row = {
            'Date': selected_date.strftime('%Y-%m-%d'),
            'Ticker': ticker,
            'Price': indicators.get('Price', ''),
            'High': indicators.get('High', ''),
            'Low': indicators.get('Low', ''),
            
            # Ichimoku
            'Ichimoku_Base': indicators.get('Ichimoku_Base', ''),
            'Ichimoku_Conversion': indicators.get('Ichimoku_Conversion', ''),
            'Ichimoku_A': indicators.get('Ichimoku_A', ''),
            'Ichimoku_B': indicators.get('Ichimoku_B', ''),
            'Ichimoku_Signal': signals.get('Ichimoku_Signal', ''),
            
            # Simple Moving Averages
            'SMA_10': indicators.get('SMA_10', ''),
            'SMA_20': indicators.get('SMA_20', ''),
            'SMA_30': indicators.get('SMA_30', ''),
            'SMA_50': indicators.get('SMA_50', ''),
            'SMA_100': indicators.get('SMA_100', ''),
            'SMA_200': indicators.get('SMA_200', ''),
            
            # SMA Signals
            'MA_10_Signal': signals.get('MA_10_Signal', ''),
            'MA_20_Signal': signals.get('MA_20_Signal', ''),
            'MA_30_Signal': signals.get('MA_30_Signal', ''),
            'MA_50_Signal': signals.get('MA_50_Signal', ''),
            'MA_100_Signal': signals.get('MA_100_Signal', ''),
            'MA_200_Signal': signals.get('MA_200_Signal', ''),
            
            # Exponential Moving Averages
            'EMA_10': indicators.get('EMA_10', ''),
            'EMA_13': indicators.get('EMA_13', ''),
            'EMA_20': indicators.get('EMA_20', ''),
            'EMA_30': indicators.get('EMA_30', ''),
            'EMA_50': indicators.get('EMA_50', ''),
            'EMA_100': indicators.get('EMA_100', ''),
            'EMA_200': indicators.get('EMA_200', ''),
            
            # EMA Signals
            'EMA_10_Signal': signals.get('EMA_10_Signal', ''),
            'EMA_20_Signal': signals.get('EMA_20_Signal', ''),
            'EMA_30_Signal': signals.get('EMA_30_Signal', ''),
            'EMA_50_Signal': signals.get('EMA_50_Signal', ''),
            'EMA_100_Signal': signals.get('EMA_100_Signal', ''),
            'EMA_200_Signal': signals.get('EMA_200_Signal', ''),
            
            # Other indicators
            'VWMA_20': indicators.get('VWMA_20', ''),
            'VWMA_Signal': signals.get('VWMA_Signal', ''),
            'Hull_MA_9': indicators.get('Hull_MA_9', ''),
            'Hull_MA_Signal': signals.get('Hull_MA_Signal', ''),
            
            # Oscillators
            'RSI_14': indicators.get('RSI_14', ''),
            'RSI_Signal': signals.get('RSI_Signal', ''),
            'Stoch_K': indicators.get('Stoch_K', ''),
            'Stoch_D': indicators.get('Stoch_D', ''),
            'Stochastic_Signal': signals.get('Stochastic_Signal', ''),
            'CCI_20': indicators.get('CCI_20', ''),
            'CCI_Signal': signals.get('CCI_Signal', ''),
            'ADX_14': indicators.get('ADX_14', ''),
            'ADX_Signal': signals.get('ADX_Signal', ''),
            'AO': indicators.get('AO', ''),
            'AO_Signal': signals.get('AO_Signal', ''),
            'Momentum_10': indicators.get('Momentum_10', ''),
            'Momentum_Signal': signals.get('Momentum_Signal', ''),
            'MACD': indicators.get('MACD', ''),
            'MACD_Signal_Value': indicators.get('MACD_Signal', ''),
            'MACD_Signal': signals.get('MACD_Signal', ''),
            'StochRSI_K': indicators.get('StochRSI_K', ''),
            'StochRSI_D': indicators.get('StochRSI_D', ''),
            'StochRSI_Signal': signals.get('StochRSI_Signal', ''),
            'Williams_R': indicators.get('Williams_R', ''),
            'Williams_R_Signal': signals.get('Williams_R_Signal', ''),
            'UO': indicators.get('UO', ''),
            'UO_Signal': signals.get('UO_Signal', ''),
            'DMI_Positive': indicators.get('DMI_Positive', ''),
            'DMI_Negative': indicators.get('DMI_Negative', ''),
            'Bull_Power': indicators.get('Bull_Power', ''),
            'Bear_Power': indicators.get('Bear_Power', ''),
            'BBP_Signal': signals.get('BBP_Signal', ''),
            
            # Previous day values
            'RSI_Prev': indicators.get('RSI_Prev', ''),
            'CCI_Prev': indicators.get('CCI_Prev', ''),
            'ADX_Prev': indicators.get('ADX_Prev', ''),
            'Momentum_Prev': indicators.get('Momentum_Prev', ''),
            'Williams_R_Prev': indicators.get('Williams_R_Prev', ''),
            'Bull_Power_Prev': indicators.get('Bull_Power_Prev', ''),
            'Bear_Power_Prev': indicators.get('Bear_Power_Prev', ''),
            'EMA_13_Prev': indicators.get('EMA_13_Prev', ''),
            'AO_Prev': indicators.get('AO_Prev', ''),
            
            # Signal Summary
            'Overall_Signal': signal_summary.get('Overall_Signal', ''),
            'Buy_Count': signal_summary.get('Buy_Count', 0),
            'Sell_Count': signal_summary.get('Sell_Count', 0),
            'Neutral_Count': signal_summary.get('Neutral_Count', 0),
            'Buy_Percentage': signal_summary.get('Buy_Percentage', 0),
            'Sell_Percentage': signal_summary.get('Sell_Percentage', 0)
        }
        
        summary_rows.append(row)
    
    return pd.DataFrame(summary_rows)


def export_to_csv(df: pd.DataFrame, filename: str = None) -> bytes:
    """
    Export DataFrame to CSV
    
    Args:
        df: DataFrame to export
        filename: Optional filename
    
    Returns:
        CSV data as bytes
    """
    if filename is None:
        filename = f"ta_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    return df.to_csv(index=False).encode('utf-8')


def export_to_excel(df: pd.DataFrame, filename: str = None) -> bytes:
    """
    Export DataFrame to Excel
    
    Args:
        df: DataFrame to export
        filename: Optional filename
    
    Returns:
        Excel data as bytes
    """
    if filename is None:
        filename = f"ta_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Write main data
        df.to_excel(writer, sheet_name='TA Analysis', index=False)
        
        # Get workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['TA Analysis']
        
        # Add formatting
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1
        })
        
        # Format signal columns
        buy_format = workbook.add_format({
            'bg_color': '#90EE90',
            'border': 1
        })
        
        sell_format = workbook.add_format({
            'bg_color': '#FFB6C1',
            'border': 1
        })
        
        neutral_format = workbook.add_format({
            'bg_color': '#FFFACD',
            'border': 1
        })
        
        # Apply header formatting
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Apply conditional formatting for signals
        signal_columns = [col for col in df.columns if 'Signal' in col]
        for col_name in signal_columns:
            if col_name in df.columns:
                col_index = df.columns.get_loc(col_name)
                
                # Apply formatting based on signal value
                for row_num in range(1, len(df) + 1):
                    cell_value = df.iloc[row_num - 1, col_index]
                    if cell_value == 'Buy':
                        worksheet.write(row_num, col_index, cell_value, buy_format)
                    elif cell_value == 'Sell':
                        worksheet.write(row_num, col_index, cell_value, sell_format)
                    else:
                        worksheet.write(row_num, col_index, cell_value, neutral_format)
        
        # Auto-adjust column width
        for i, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).map(len).max(),
                len(col)
            )
            worksheet.set_column(i, i, min(max_length + 2, 30))
    
    output.seek(0)
    return output.read()


