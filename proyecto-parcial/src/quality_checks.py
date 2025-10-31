import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def check_no_negative_prices(df):
    if 'price' not in df.columns:
        logger.error('Columna price no encontrada en el DataFrame.')
        return False, 'missing_price_column'
    bad = df[df['price'] < 0]
    ok = len(bad) == 0
    return ok, f'negative_prices_count={len(bad)}'

def check_stock_integer_positive(df):
    if 'current_stock' not in df.columns:
        logger.error('Columna current_stock no encontrada.')
        return False, 'missing_current_stock'
    bad = df[~df['current_stock'].apply(lambda x: (isinstance(x, (int,)) and x >= 0))]
    ok = len(bad) == 0
    return ok, f'bad_stock_count={len(bad)}'

def check_categories_exist(df, valid_categories):
    if 'category' not in df.columns:
        logger.error('Columna category no encontrada.')
        return False, 'missing_category'
    bad = df[~df['category'].isin(valid_categories)]
    ok = len(bad) == 0
    return ok, f'unknown_categories_count={len(bad)}'

def check_sale_dates_valid(df, date_col='sale_date'):
    if date_col not in df.columns:
        logger.error(f'Columna {date_col} no encontrada.')
        return False, 'missing_date'
    def valid_date(x):
        try:
            pd.to_datetime(x)
            return True
        except:
            return False
    import pandas as pd
    bad = df[~df[date_col].apply(valid_date)]
    ok = len(bad) == 0
    return ok, f'invalid_dates_count={len(bad)}'
