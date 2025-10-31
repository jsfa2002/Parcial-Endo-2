import requests
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def fetch_products(api_url, timeout=30, fallback_path=None):
    """Fetch products from API. If it fails, try to read from fallback_path (CSV/JSON)."""
    try:
        logger.info(f"Intentando descargar productos desde API: {api_url}")
        resp = requests.get(api_url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        df = pd.json_normalize(data)
        logger.info(f"Descargados {len(df)} productos desde la API.")
        return df
    except Exception as e:
        logger.warning(f"No se pudo descargar desde API: {e}")
        if fallback_path and Path(fallback_path).exists():
            logger.info(f"Cargando productos desde fallback: {fallback_path}")
            return pd.read_json(fallback_path)
        logger.error("No hay datos de productos disponibles.")
        raise

def load_csv(path):
    logger.info(f"Cargando CSV: {path}")
    return pd.read_csv(path)

def save_parquet(df, path):
    logger.info(f"Guardando Parquet en: {path}")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
