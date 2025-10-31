import yaml
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path

from src.ingestion import fetch_products, load_csv, save_parquet
from src.transformation import merge_datasets, compute_metrics, save_outputs
from src.quality_checks import (
    check_no_negative_prices, check_stock_integer_positive,
    check_categories_exist, check_sale_dates_valid
)

class EcommerceDataPipeline:
    """Clase principal que orquesta todo el flujo del pipeline."""

    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.setup_logging()
        self.config_path = config_path

    def load_config(self, config_path):
        """Carga la configuración YAML."""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def setup_logging(self):
        """Configura los logs del pipeline."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pipeline_execution.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def run_pipeline(self):
        """Ejecuta todas las etapas del pipeline."""
        self.logger.info("Iniciando pipeline de e-commerce...")
        api_url = self.config.get('api', {}).get('url')
        timeout = self.config.get('api', {}).get('timeout', 30)
        cfg = self.config.get('data_sources', {})
        sales_file = cfg.get('sales_file')
        inventory_file = cfg.get('inventory_file')
        output_path = self.config.get('processing', {}).get('output_path', 'data/processed/')
        critical = self.config.get('processing', {}).get('critical_stock_threshold', 1.2)

        # Ingesta de datos
        try:
            products_df = fetch_products(api_url, timeout=timeout)
        except Exception:
            self.logger.warning('Fallo API: usando datos de respaldo.')
            sales_df = load_csv(sales_file)
            products_df = sales_df[['product_id','title','category']].drop_duplicates().assign(price=0)

        sales_df = load_csv(sales_file)
        inventory_df = load_csv(inventory_file)

        # Guardar datos crudos
        Path(output_path).mkdir(parents=True, exist_ok=True)
        save_parquet(sales_df, Path(output_path) / 'sales_raw.parquet')
        save_parquet(inventory_df, Path(output_path) / 'inventory_raw.parquet')
        save_parquet(products_df, Path(output_path) / 'products_raw.parquet')

        # Transformaciones
        merged = merge_datasets(products_df, sales_df, inventory_df)
        metrics = compute_metrics(merged, critical_stock_threshold=critical)

        # Validaciones de calidad
        qc_results = {}
        qc_results['no_negative_prices'] = check_no_negative_prices(metrics['merged'])
        qc_results['stock_integer_positive'] = check_stock_integer_positive(metrics['merged'])
        qc_results['categories_exist'] = check_categories_exist(metrics['merged'], metrics['merged']['category'].unique().tolist())
        qc_results['sale_dates_valid'] = check_sale_dates_valid(metrics['merged'], date_col='sale_date')

        self.logger.info(f"Resultados QC: {qc_results}")

        # Guardar resultados finales
        save_outputs(metrics, output_dir=output_path)

        # Generar reporte final
        report = {
            'run_at': datetime.utcnow().isoformat(),
            'rows_merged': len(metrics['merged']),
            'qc': {k: v for k, v in qc_results.items()}
        }
        import json
        Path('data/outputs').mkdir(parents=True, exist_ok=True)
        with open('data/outputs/report.json', 'w') as f:
            json.dump(report, f, indent=2)

        self.logger.info('Pipeline finalizado correctamente.')
