# Es el archivo principal que ejecuta todo el pipeline

from src.orchestrator import EcommerceDataPipeline

if __name__ == '__main__':
    pipeline = EcommerceDataPipeline('config/pipeline_config.yaml')
    pipeline.run_pipeline()
