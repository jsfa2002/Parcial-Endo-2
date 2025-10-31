import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def merge_datasets(products_df, sales_df, inventory_df):
    """Une los tres conjuntos de datos principales: productos, ventas e inventario."""
    # Limpieza de nombres de columnas
    products_df = products_df.rename(columns={c: c.strip() for c in products_df.columns})
    sales_df = sales_df.rename(columns={c: c.strip() for c in sales_df.columns})
    inventory_df = inventory_df.rename(columns={c: c.strip() for c in inventory_df.columns})

    # Unificar la columna de ID de producto
    if 'id' in products_df.columns and 'product_id' not in products_df.columns:
        products_df = products_df.rename(columns={'id': 'product_id'})

    # Combinar todo en un solo DataFrame
    merged = sales_df.merge(products_df, on='product_id', how='left')
    merged = merged.merge(inventory_df, on='product_id', how='left')
    logger.info(f"Registros tras combinar: {len(merged)}")
    return merged

def compute_metrics(merged_df, critical_stock_threshold=1.2):
    """Calcula métricas de negocio: stock crítico, ventas y rentabilidad."""
    df = merged_df.copy()

    # Asegurar tipos numéricos correctos
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    df['current_stock'] = pd.to_numeric(df.get('current_stock', pd.Series([0]*len(df))), errors='coerce').fillna(0).astype(int)
    df['min_stock'] = pd.to_numeric(df.get('min_stock', pd.Series([0]*len(df))), errors='coerce').fillna(0).astype(int)

    # Detectar productos con stock crítico
    df['is_critical_stock'] = df['current_stock'] < df['min_stock'] * critical_stock_threshold

    # Ventas totales por categoría
    sales_by_cat = df.groupby('category').apply(lambda x: (x['price'] * x['quantity']).sum()).rename('sales_total').reset_index()

    # Productos más vendidos
    sold_by_product = df.groupby(['product_id', 'title']).agg({'quantity':'sum', 'price':'mean'}).reset_index().sort_values('quantity', ascending=False)

    # Rentabilidad estimada (usa 60% del precio como costo estimado si no hay columna de costo)
    if 'cost' not in df.columns:
        df['estimated_cost'] = df['price'] * 0.6
    else:
        df['estimated_cost'] = pd.to_numeric(df['cost'], errors='coerce').fillna(df['price'] * 0.6)

    profitability = df.groupby(['product_id', 'title']).apply(lambda x: ((x['price'] - x['estimated_cost']) * x['quantity']).sum()).rename('profit_est').reset_index()

    return {
        'merged': df,
        'sales_by_category': sales_by_cat,
        'top_products': sold_by_product,
        'profitability': profitability
    }

def save_outputs(outputs, output_dir="data/processed"):
    """Guarda los resultados intermedios y finales en diferentes formatos."""
    p = Path(output_dir)
    p.mkdir(parents=True, exist_ok=True)
    outputs['merged'].to_parquet(p / 'merged.parquet', index=False)
    outputs['sales_by_category'].to_csv(p / 'sales_by_category.csv', index=False)
    outputs['top_products'].to_csv(p / 'top_products.csv', index=False)
    outputs['profitability'].to_csv(p / 'profitability.csv', index=False)
    logger.info(f"Resultados guardados en {p}")
