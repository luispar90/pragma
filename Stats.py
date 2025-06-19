import polars as pl
from typing import Dict
import logging
from dataclasses import dataclass

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Stats:
    """
    Mantiene estadísticas de forma incremental sin consultar la BD.
    Toda la lógica de actualización está en memoria.
    """

    COUNT: int = 0
    SUM: float = 0.0
    MIN_VALUE: float = float('inf')
    MAX_VALUE: float = float('-inf')

    @property
    def _mean(self) -> float:
        """
        Calcula la media desde los valores acumulados
        """
        return self.SUM / self.COUNT if self.COUNT > 0 else 0.0


    def update_from_batch(self, prices: pl.Series) -> None:
        """
        Actualiza las estadísticas con un nuevo batch de precios.
        NO consulta la base de datos, solo hace cálculos en memoria.

        Args:
            prices: Serie de Polars con los precios del batch
        """
        if len(prices) == 0:
            return

        # Convertir a numpy para cálculos eficientes
        price_values = prices.to_numpy()

        # Actualizar estadísticas incrementalmente
        self.COUNT += len(price_values)
        self.SUM += float(price_values.sum())
        self.MIN_VALUE = min(self.MIN_VALUE, float(price_values.min()))
        self.MAX_VALUE = max(self.MAX_VALUE, float(price_values.max()))

        logger.debug(f"Stats actualizadas - Count: {self.COUNT}, "
                     f"Mean: {self._mean:.2f}, Min: {self.MIN_VALUE:.2f}, "
                     f"Max: {self.MAX_VALUE:.2f}")


    def get_current_stats(self) -> Dict[str, float]:
        """
        Retorna las estadísticas actuales
        :return: Dict
        """
        return {
            'count': self.COUNT,
            'mean': self._mean,
            'min': self.MIN_VALUE if self.MIN_VALUE != float('inf') else 0.0,
            'max': self.MAX_VALUE if self.MAX_VALUE != float('-inf') else 0.0
        }

    def print_stats(self, title: str = "Estadísticas Actuales") -> None:
        """
        Imprime las estadísticas de forma formateada
        :param title: default: Estadísticas Actuales
        :return: None
        """
        print(f"Recuento total: {self.COUNT:,}")
        print(f"Precio promedio: ${self._mean:.2f}")
        print(f"Precio mínimo: ${self.MIN_VALUE:.2f}" if self.MIN_VALUE != float('inf') else "Precio mínimo: N/A")
        print(f"Precio máximo: ${self.MAX_VALUE:.2f}" if self.MAX_VALUE != float('-inf') else "Precio máximo: N/A")