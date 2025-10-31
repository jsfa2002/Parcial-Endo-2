import pandas as pd
from src.quality_checks import (
    check_no_negative_prices, check_stock_integer_positive,
    check_categories_exist, check_sale_dates_valid
)

def make_sample_df():
    return pd.DataFrame([
        {'product_id':1, 'price': 10.0, 'category': 'A', 'current_stock': 5, 'sale_date': '2023-01-01'},
        {'product_id':2, 'price': 20.0, 'category': 'B', 'current_stock': 0, 'sale_date': '2023-02-01'},
    ])

def test_no_negative_prices():
    df = make_sample_df()
    ok, msg = check_no_negative_prices(df)
    assert ok

def test_stock_integer_positive():
    df = make_sample_df()
    ok, msg = check_stock_integer_positive(df)
    assert ok

def test_categories_exist():
    df = make_sample_df()
    ok, msg = check_categories_exist(df, ['A','B','C'])
    assert ok

def test_sale_dates_valid():
    df = make_sample_df()
    ok, msg = check_sale_dates_valid(df, 'sale_date')
    assert ok
