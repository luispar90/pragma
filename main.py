import os
import logging

from Pipeline import Pipeline
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': int(os.getenv('DB_PORT', 5432))
}

# Directorio con archivos CSV
DATA_DIR = './data'

if __name__ == "__main__":

    # Crear pipeline
    pipeline = Pipeline(DB_CONFIG, chunk_size=1000)

    try:
        pipeline.connect()

        print("--INICIANDO PIPELINE DE PROCESAMIENTO--")
        print("FASE 1: Procesamiento de archivos principales")

        for i in range(1, 6):
            filepath = os.path.join(DATA_DIR, f'2012-{i}.csv')

            if not os.path.exists(filepath):
                logger.warning(f"Archivo {filepath} no encontrado")
                continue

            # Procesar archivo
            pipeline.process_csv_file(filepath)

            # Mostrar estadísticas actuales (calculadas en memoria)
            pipeline.stats.print_stats(f"Estadisticas después de 2012-{i}.csv")

            # Verificación 1: Comparar con BD antes de validation
            print("VERIFICACION 1: Estado antes de validation.csv")
            stats_before = pipeline.verify_results_from_db()

            # FASE 2: Procesar archivo de validación
            print("FASE 2: Procesamiento de validation.csv")

            validation_path = os.path.join(DATA_DIR, 'validation.csv')
            if os.path.exists(validation_path):
                # Procesar con update_per_row=True según requerimientos
                pipeline.process_csv_file(validation_path, update_per_row=True)

                # Mostrar estadísticas finales del pipeline
                pipeline.stats.print_stats("Estadisticas Finales del Pipeline")
            else:
                logger.warning("Archivo validation.csv no encontrado")

            # Verificación 2: Comparar con BD después de validation
            print("VERIFICACION 2: Estado despues de validation.csv")
            stats_after = pipeline.verify_results_from_db()

            # Mostrar cambios
            print("ANALISIS DE CAMBIOS:")
            print(f"Filas: +{stats_after['count'] - stats_before['count']:,}")
            print(f"Promedio: ${stats_after['mean'] - stats_before['mean']:.2f}")
            print(f"Minimo: ${stats_after['min'] - stats_before['min']:.2f}")
            print(f"Maximo: ${stats_after['max'] - stats_before['max']:.2f}")

            print("Pipeline completado exitosamente!")

    except Exception as e:
        logger.error(f"Error en pipeline: {e}")
        raise
    finally:
        pipeline.disconnect()