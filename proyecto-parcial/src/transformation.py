import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def merge_datasets(products_df, sales_df, inventory_df):
    """Une los tres datasets: productos, ventas e inventario."""
    # Normalizar nombres de columnas
    products_df.columns = products_df.columns.str.strip().str.lower()
    sales_df.columns = sales_df.columns.str.strip().str.lower()
    inventory_df.columns = inventory_df.columns.str.strip().str.lower()

    # Asegurar consistencia en el ID del producto
    if 'id' in products_df.columns and 'product_id' not in products_df.columns:
        products_df = products_df.rename(columns={'id': 'product_id'})

    merged = sales_df.merge(products_df, on='product_id', how='left', suffixes=('_sale', '_prod'))
    merged = merged.merge(inventory_df, on='product_id', how='left')
    
    logger.info(f"Registros tras merge: {len(merged)}")
    return merged


def compute_metrics(merged_df, critical_stock_threshold=1.2):
    """Calcula métricas de negocio: stock crítico, ventas y rentabilidad."""
    df = merged_df.copy()

    # Convertir tipos numéricos
    df['price'] = pd.to_numeric(
        df.get('price', df.get('precio', df.get('unit_price'))),
        errors='coerce'
    )

    df['quantity'] = pd.to_numeric(df.get('quantity', df.get('cantidad')), errors='coerce').fillna(0).astype(int)
    df['current_stock'] = pd.to_numeric(df.get('current_stock', 0), errors='coerce').fillna(0).astype(int)
    df['min_stock'] = pd.to_numeric(df.get('min_stock', 0), errors='coerce').fillna(0).astype(int)

    # Stock crítico
    df['is_critical_stock'] = df['current_stock'] < df['min_stock'] * critical_stock_threshold
    critical_stock = df[df['is_critical_stock']][['product_id', 'current_stock', 'min_stock']]

    # Ventas totales por categoría
    sales_by_cat = (
        df.groupby('category', dropna=False)
        .apply(lambda x: (x['price'] * x['quantity']).sum())
        .rename('sales_total')
        .reset_index()
    )

    # Productos más vendidos
    group_cols = ['product_id']
    if 'title' in df.columns:
        group_cols.append('title')

    sold_by_product = (
        df.groupby(group_cols)
        .agg({'quantity': 'sum', 'price': 'mean'})
        .reset_index()
        .sort_values('quantity', ascending=False)
    )

    # Rentabilidad estimada
    if 'cost' not in df.columns:
        df['estimated_cost'] = df['price'] * 0.6
    else:
        df['estimated_cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(df['price'] * 0.6)

    profitability = (
        df.groupby(group_cols)
        .apply(lambda x: ((x['price'] - x['estimated_cost']) * x['quantity']).sum())
        .rename('profit_est')
        .reset_index()
    )

    return {
        'merged': df,
        'sales_by_category': sales_by_cat,
        'top_products': sold_by_product,
        'profitability': profitability,
        'critical_stock': critical_stock
    }


def save_outputs(outputs, output_dir="data/processed"):
    """Guarda los resultados en formato Parquet y CSV."""
    p = Path(output_dir)
    p.mkdir(parents=True, exist_ok=True)

    outputs['merged'].to_parquet(p / 'merged.parquet', index=False)
    outputs['sales_by_category'].to_csv(p / 'sales_by_category.csv', index=False)
    outputs['top_products'].to_csv(p / 'top_products.csv', index=False)
    outputs['profitability'].to_csv(p / 'profitability.csv', index=False)
    outputs['critical_stock'].to_csv(p / 'critical_stock.csv', index=False)

    logger.info(f"Outputs guardados en {p}")
