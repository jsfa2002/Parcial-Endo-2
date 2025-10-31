# Proyecto Parcial Enfoque de DataOps Segundo Corte

Autor: Juan Sebastián Fajardo Acevedo
Entregado a profesor: María José Torres

## Estructura
- `src/` : Código fuente con ingesta, transformación, calidad, orquestador.
- `config/` : YAML de configuración del pipeline
- `data/raw/` : Datos de entrada de ejemplo CSV/JSON
- `data/processed/` : Salidas intermedias: parquet, CSV
- `data/outputs/` : Reporte final y artefactos
- `tests/` : Tests unitarios básicos
- `run_pipeline.py` : Script principal para ejecutar el pipeline

## Cómo ejecutar 
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
El pipeline intentará conectarse a la API definida en `config/pipeline_config.yaml`. Si no hay acceso a internet, utiliza los datos de ejemplo en `data/raw/products_fallback.json` o inferirá productos desde `sales.csv`.

3. Salidas:
 - "data/processed/": Archivos Parquet o CSV con métricas limpias.
- "data/outputs/report.json":  Reporte resumen de ejecución.
- "pipeline_execution.log": Registro de logs con el detalle del proceso.

## Diseño del Pipeline de Datos
El pipeline fue diseñado bajo un enfoque ELT siguiendo los principios que vimos de DataOps, con el objetivo de garantizar calidad, trazabilidad y escalabilidad en el manejo de los datos.

**1. Ingesta**

Se emplearon tres fuentes principales que son, una API REST con información de productos en tiempo real, un CSV histórico de ventas y un CSV de inventario local. Todos los datos son almacenados inicialmente en formato crudo para mantener la trazabilidad, y asi que podamos revisar cualquier cambio o actualización en el futuro.

**2. Transformación**

En esta etapa se realiza la limpieza, estandarización y combinación de los datos provenientes de diferentes fuentes, luego se calculan las métricas del negocio clave, como los productos con stock crítico, ventas totales por categoría, artículos más vendidos y la rentabilidad por producto. Esta capa prepara la información para su uso analítico o de reportes.

**3. Calidad de Datos**
Se implementaron validaciones automáticas para asegurar la consistencia, ntre ellas están la verificación de que no existan precios negativos, el control de que los valores de stock sean enteros y positivos, la revisión de que todas las categorías estén definidas y validación de las fechas de venta. Los resultados de cada validación se registran tanto en los logs como en el reporte final de ejecución.

**4. Orquestación**

El control del flujo del pipeline se gestiona con el script principal run_pipeline.py y las configuraciones definidas en YAML, además el sistema incluye manejo de errores, registros detallados y una estructura modular que la ide es que haga más fácil la escalabilidad. En etapas futuras, la orquestación podría migrarse a plataformas especializadas como Apache Airflow.

**5. Análisis**

Los datos procesados se usan para generar reportes y dashboards que nos permiten monitorear el comportamiento de las ventas, el nivel de inventarios y la rentabilidad de los productos.Y esta información nos podría ayudar a la toma de decisiones basada en datos dentro de la organización en la que estemos.

## Diagrama del Pipeline

<img width="1941" height="1336" alt="mermaid-diagram-2025-10-31-162953" src="https://github.com/user-attachments/assets/e9bbac86-5a13-4831-8777-afb16c0f14b7" />

Diagrama hecho con la herramienta de Mermaid.

## Justificación Técnica

El pipeline se desarrolló con un enfoque modular, escalable y automatizado, siguiendo los principios de DataOps para que podamos asegurar la calidad, trazabilidad y eficiencia en el procesamiento de datos.
El uso de la arquitectura ELT nos deja almacenar los datos en su forma cruda antes de transformarlos, y así garantizamos el control y la reproducibilidad. Esta decisión técnica fue para ayudar a revisar los orígenes y mantener una línea clara de rastreo ante inconsistencias que puedan ocurrir
.
La separación de configuración y código mediante archivos YAML ayuda a la flexibilidad, ya que permite modificar parámetros, rutas o fuentes sin alterar la lógica del sistema, y esto simplifica la administración y el mantenimiento del pipeline.
El diseño modular en Python permite integrar nuevas fuentes, ampliar el volumen de datos o escalar a entornos distribuidos sin necesidad de reescribir el flujo completo. Además, la incorporación de los logs y validaciones automáticas da trazabilidad y control de calidad cada vez que se ejecute.
Poor último la estructura del proyecto ayuda a la colaboración y reproducibilidad, ya que cualquier miembro del equipo podría ejecutar el pipeline bajo las mismas condiciones, asegurando coherencia entre desarrollo, pruebas y producción.

## Qué Entrega Este Proyecto
- Un pipeline de datos completo con enfoque ELT.
- Ingesta desde API y CSV, transformación, calidad y generación de reportes.
- Configuración modular con YAML y logs de ejecución.
- Validaciones automáticas y pruebas unitarias básicas.
- Una base para escalar a sistemas de orquestación más avanzados.


