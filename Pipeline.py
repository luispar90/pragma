import logging
import polars as pl
import os
import numpy as np
from typing import Optional, Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_batch

from Stats import Stats
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Pipeline:
    """
    Pipeline principal para procesamiento de datos en micro-batches.
    """

    def __init__(self, db_config: Dict[str, str], chunk_size: int = 1000):
        """
        Inicializa el pipeline.

        Args:
            db_config: Configuración de conexión a PostgreSQL
            chunk_size: Tamaño de cada micro-batch
        """
        self.db_config = db_config
        self.chunk_size = chunk_size
        self.stats = Stats()
        self.connection: Optional[psycopg2.connection] = None
        self.cursor: Optional[psycopg2.cursor] = None

    def connect(self) -> None:
        """
        Establece conexión con la base de datos
        """
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            logger.info("Conexión a PostgreSQL establecida")
        except Exception as e:
            logger.error(f"Error conectando a PostgreSQL: {e}")
            raise


    def disconnect(self) -> None:
        """
        Cierra las conexiones de forma segura
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Conexiones cerradas")


    def process_csv_file(self, filepath: str, update_per_row: bool = False) -> int:
        """
        Procesa un archivo CSV en micro-batches.

        IMPORTANTE: Las estadísticas se actualizan en memoria, NO consultando la BD.

        Args:
            filepath: Ruta al archivo CSV
            update_per_row: Si True, actualiza stats después de cada fila (para validation.csv)

        Returns:
            Número de filas procesadas
        """
        filename = os.path.basename(filepath)
        logger.info(f"Iniciando procesamiento de: {filename}")

        total_rows_processed = 0

        try:
            # Crear LazyFrame para lectura eficiente
            lf = pl.scan_csv(filepath)

            # Procesar en batches
            offset = 0
            while True:
                # Leer siguiente chunk
                chunk = lf.slice(offset, self.chunk_size).collect()

                if chunk.height == 0:  # No más datos
                    break

                # Preparar datos para insercion
                records = self._prepare_records(chunk, filename)

                if update_per_row:
                    # Procesar fila por fila (menos eficiente, solo para validation)
                    self._process_row_by_row(records, chunk)
                else:
                    # Procesar todo el batch (más eficiente)
                    self._process_batch(records, chunk)

                total_rows_processed += len(records)
                offset += self.chunk_size

                # Log de progreso cada 10,000 registros
                if total_rows_processed % 10000 == 0:
                    logger.info(f"Procesados {total_rows_processed:,} registros")

            self.connection.commit()
            logger.info(f"{filename} completado: {total_rows_processed:,} filas procesadas")

        except Exception as e:
            logger.error(f"Error procesando {filename}: {e}")
            if self.connection:
                self.connection.rollback()
            raise

        return total_rows_processed


    def _prepare_records(self, chunk: pl.DataFrame, filename: str) -> List[Tuple]:
        """
        Prepara los registros del chunk para inserción en BD.

        Args:
            chunk: DataFrame de Polars con los datos
            filename: Nombre del archivo origen

        Returns:
            Lista de tuplas listas para insertar
        """

        # Reemplazamos los nulos
        print(chunk.head(15))
        chunk = chunk.with_columns(pl.col("price").fill_null(0))
        print(chunk.head(15))
        records = []
        for row in chunk.iter_rows():
            print(row)
            records.append((
                datetime.strptime(str(row[0]), "%m/%d/%Y"),
                float(row[1]),
                str(row[2]),
                filename
            ))
        return records


    def _process_batch(self, records: List[Tuple], chunk: pl.DataFrame) -> None:
        """
        Procesa un batch completo de registros.
        Actualiza estadísticas SIN consultar la BD.

        Args:
            records: Lista de tuplas para insertar
            chunk: DataFrame con los datos originales
        """
        # Insertar datos en BD
        insert_query = """
            INSERT INTO transactions (timestamp, price, user_id, file_source)
            VALUES (%s, %s, %s, %s)
        """
        execute_batch(self.cursor, insert_query, records)

        # Actualizar estadísticas en memoria (NO consultamos BD!)
        prices = chunk.get_column('price').cast(pl.Float64)
        self.stats.update_from_batch(prices)


    def _process_row_by_row(self, records: List[Tuple], chunk: pl.DataFrame) -> None:
        """
        Procesa registros uno por uno, actualizando stats después de cada inserción.
        Solo usado para el archivo validation.csv según requerimientos.

        Args:
            records: Lista de tuplas para insertar
            chunk: DataFrame con los datos originales
        """
        insert_query = """
            INSERT INTO transactions (timestamp, price, user_id, file_source)
            VALUES (%s, %s, %s, %s)
        """

        prices = chunk.get_column('price').cast(pl.Float64).to_numpy()

        for i, record in enumerate(records):
            # Insertar registro individual
            self.cursor.execute(insert_query, record)

            # Actualizar estadísticas con este precio individual
            single_price = pl.Series([prices[i]])
            self.stats.update_from_batch(single_price)

            # Mostrar progreso cada 100 registros
            if (i + 1) % 100 == 0:
                logger.debug(f"Procesados {i + 1} registros individualmente")

    def verify_results_from_db(self) -> Dict[str, float]:
        """
        Consulta las estadísticas directamente desde la BD.

        IMPORTANTE: Este método es SOLO para verificación final,
        NO se usa durante el procesamiento del pipeline.

        Returns:
            Diccionario con las estadísticas de la BD
        """
        query = """
            SELECT 
                COUNT(*) as total_rows,
                AVG(price)::numeric(10,2) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM transactions;
        """

        self.cursor.execute(query)
        result = self.cursor.fetchone()

        db_stats = {
            'count': result[0] or 0,
            'mean': float(result[1]) if result[1] else 0.0,
            'min': float(result[2]) if result[2] else 0.0,
            'max': float(result[3]) if result[3] else 0.0
        }

        print(f"{'Verificación desde Base de Datos':^60}")
        print(f"Recuento total: {db_stats['count']:,}")
        print(f"Precio promedio: ${db_stats['mean']:.2f}")
        print(f"Precio mínimo: ${db_stats['min']:.2f}")
        print(f"Precio máximo: ${db_stats['max']:.2f}")

        return db_stats
