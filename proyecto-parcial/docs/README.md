# Proyecto Parcial - DataOps (Segundo Corte)

Autor: Juan Sebastián

## Estructura
- `src/` : Código fuente (ingesta, transformación, calidad, orquestador)
- `config/` : YAML de configuración del pipeline
- `data/raw/` : Datos de entrada de ejemplo (CSV/JSON)
- `data/processed/` : Salidas intermedias (parquet, CSV)
- `data/outputs/` : Reporte final y artefactos
- `tests/` : Tests unitarios básicos
- `run_pipeline.py` : Script principal para ejecutar el pipeline

## Cómo ejecutar (local sin cloud)
1. Crear un entorno virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate   # o venv\Scripts\activate en Windows
   pip install -r requirements.txt
   ```
2. Ejecutar el pipeline:
   ```bash
   python run_pipeline.py
   ```
   Nota: El pipeline intentará conectarse a la API definida en `config/pipeline_config.yaml`. Si no hay acceso a internet, utiliza los datos de ejemplo en `data/raw/products_fallback.json` o inferirá productos desde `sales.csv`.

3. Salidas:
   - `data/processed/` : parquet y CSVs con métricas
   - `data/outputs/report.json` : reporte con resumen de ejecución
   - `pipeline_execution.log` : log de ejecución

## Qué entrega este proyecto
- Diseño ELT básico: ingesta (API + CSVs), carga raw (parquet), transformaciones y métricas,
  tests de calidad y orquestación simple.
- Automatización mínima: configuración YAML, logging y manejo de errores básico.
- Tests unitarios para checks de calidad.

## Notas para la entrega (README del parcial)
En tu repositorio de GitHub:
- Incluye commits atómicos y mensajes claros.
- Explica en el `README.md` (este documento) las decisiones técnicas:
  - Diseño del pipeline y diagrama (pegalo en la sección justo aquí)
  - Estrategia de versionamiento (Git + migraciones)
  - Estrategia de pruebas (unitarias y de integración)
  - Cómo escalar (usar cloud, particionado, orquestadores: Airflow/Kubernetes)

