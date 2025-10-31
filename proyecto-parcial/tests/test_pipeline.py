import pandas as pd
from src.quality_checks import (
    check_no_negative_prices, check_stock_integer_positive,
    check_categories_exist, check_sale_dates_valid
)

# Esta función crea un pequeño DataFrame de ejemplo para probar las funciones de calidad.
# Simula un conjunto de productos con precio, categoría, stock y fecha de venta.
def make_sample_df():
    return pd.DataFrame([
        {'product_id': 1, 'price': 10.0, 'category': 'A', 'current_stock': 5, 'sale_date': '2023-01-01'},
        {'product_id': 2, 'price': 20.0, 'category': 'B', 'current_stock': 0, 'sale_date': '2023-02-01'},
    ])

# Prueba que no existan precios negativos en los datos, ya que esto no endría sentido.
def test_no_negative_prices():
    df = make_sample_df()
    ok, msg = check_no_negative_prices(df)
    assert ok  # Si hay precios negativos, la prueba fallará.

# Prueba que las cantidades de stock sean enteras y positivas.
def test_stock_integer_positive():
    df = make_sample_df()
    ok, msg = check_stock_integer_positive(df)
    assert ok

# Prueba que todas las categorías de productos existan en la lista de categorías válidas.
def test_categories_exist():
    df = make_sample_df()
    ok, msg = check_categories_exist(df, ['A', 'B', 'C'])
    assert ok

# Prueba que las fechas de venta sean válidas, o sea que se puedan convertir correctamente.
def test_sale_dates_valid():
    df = make_sample_df()
    ok, msg = check_sale_dates_valid(df, 'sale_date')
    assert ok
