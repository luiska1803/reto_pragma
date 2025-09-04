import os
import pandas as pd
from typing import Any, Dict

from config.logging_utils import get_logger
logger = get_logger()

class CSVReader():
    """
        Clase para la lectura de CSV con la libreria de pandas

        Atributos:
        -----------
            config : Dict[str, Any] 
                Archivo de configuración en formato diccionario con las especificaciones
                para la lectura del CSV. 

        Metodos:
        --------
            run(file_path: str, chunksize: int)
                El metodo en el cual se realiza la lectura del dataframe de pandas, 
                tomando los parametros de configuración del archivo otorgado
    """
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config['CSV']

    def run(self, file_path: str = None, chunksize: int = 1) -> pd.DataFrame:
        """
            Ejecuta el nodo: valida la configuración, lee el archivo CSV
            y devuelve un DataFrame de pandas.

            Args:
            -----
                file_path (str = None)
                    Path con la dirección del archivo a leer
                chunksize (int = 1, opcinal)
                    El tamaño de chunksize prederminado para la lectura de batch.
                sep (str)
                    Caracter con el que esta separado el archivo CSV
                usecols (list, opcional)
                    columnas que tomara la lectura del archivo CSV
                usar_chunk (bool = False)
                    Parametro establecido para el uso de chunksize y lectura batch.
                    
            Returns:
            --------
            pd.DatraFrame: DataFrame
        """

        sep = self.config.get("separadores", ",")
        usecols = self.config.get("usecols", None)
        usar_chunk = self.config.get("usar_chunk", False)

        if usar_chunk:
            chunksize = chunksize
            if chunksize < 1 or not isinstance(chunksize, int): 
                logger.error(f"Usuario ingresa chunksize incorrecto: {chunksize}, debe ser un numero entero igual o mayor a 1.")
                raise ValueError("El valor del Chunksize debe ser un numero entero y ser igual o mayor a 1")

        if not file_path:
            logger.error(f"Se requiere 'file_path' en su configuración.")
            raise ValueError("CSVReader requiere un 'file_path' en config")
        
        if not os.path.exists(file_path):
            logger.error(f"No se encontró el archivo CSV '{file_path}'.")
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        logger.info(f"Leyendo archivo CSV desde: {os.path.basename(file_path)}")

        try:
            if usar_chunk:
                df = pd.read_csv(file_path, sep=sep, usecols=usecols, chunksize=chunksize)
                logger.info(f"Lectura parcial completada.")
            else:
                df = pd.read_csv(file_path, sep=sep, usecols=usecols)
                logger.info(f"Lectura completada: {df.shape[0]} filas, {df.shape[1]} columnas")
            return df
        except Exception as e:
            logger.error(f"Fallo al leer el archivo CSV.")
            logger.exception(f"leyendo archivo CSV: {e}")
            raise RuntimeError(f"[Error] leyendo archivo CSV: {e}")
