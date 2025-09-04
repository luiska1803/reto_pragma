import getpass
from pathlib import Path
from typing import Any, Dict

from src.modulos.db import insert_events, get_running_stats, update_running_stats

from src.modulos.stats import RunningStats
from src.submodulos.csv_reader import CSVReader
from src.modulos.limpieza import limpieza_df

from config.logging_utils import get_logger
logger = get_logger()


def iter_csv_files(config: Dict[str, Any], include_validation: bool = False):
    """
        Itera sobre los archivos CSV disponibles en el directorio configurado.
        Devuelve con ayuda de yield la lista 1 a 1

        Args:
            config (dict): Diccionario de configuración con clave 'CSV' que contiene:
                - "CSV_DIR" (str): Ruta al directorio con archivos CSV.
                - "file_validation" (str): Nombre del archivo de validación a excluir.
            include_validation (bool, optional): Si es False, excluye el archivo de validación
                del listado. Por defecto es False.

        Yields:
            pathlib.Path: Ruta de cada archivo CSV encontrado en el directorio.

    """
    config = config['CSV']
    csv_dir = Path(config.get("CSV_DIR"))
    csv_path_files = sorted(
        [f for f in csv_dir.iterdir() if f.is_file() and f.suffix == ".csv"]
    )
    
    if include_validation == False:
        csv_path_files.remove(csv_dir / config.get('file_validation'))

    for path_archivo in csv_path_files:
        # Usamos Yield para no retornar toda la lista, si no, 1 a 1 segun se vaya solicitando
        yield path_archivo


def load_running_stats_from_db(config) -> RunningStats:
    """
        Esta función carga las estadísticas acumuladas (running stats) 
        desde la base de datos, con ayuda de el archivo yaml de configuracion.

        Args:
            config (dict): Diccionario de configuración que contiene la clave 'SQL'
                con la query bajo la clave 'query_running'.

        Returns:
            RunningStats: Objeto con las estadísticas en ejecución:
                - count (int): Número de registros acumulados.
                - mean (float): Media acumulada.
                - min (float): Valor mínimo registrado.
                - max (float): Valor máximo registrado.
        
    """
    rs = get_running_stats(config)
    return RunningStats(count=rs["count"], mean=rs["mean"], min=rs["min"], max=rs["max"])


def persist_running_stats(rs: RunningStats, config: Dict[str, Any]):
    """
        Funcion que ayuda con la persistencia a las estadísticas acumuladas en la 
        base de datos.

        Args:
            rs (RunningStats): Objeto con estadísticas acumuladas que contiene:
                - count (int): Número de registros acumulados.
                - mean (float): Media acumulada.
                - min (float): Valor mínimo registrado.
                - max (float): Valor máximo registrado.
            config (dict): Diccionario de configuración que contiene la clave 'SQL'
                con la query bajo la clave 'query_update'.

        Side Effects:
            - Actualiza la tabla de estadísticas en la base de datos.
    """
    update_running_stats(rs.count, rs.mean, rs.min, rs.max, config)


def ingest_file(path: Path, mode: str, chunksize: int, config: Dict[str, Any]): 
    """
        Funcion principal del pipeline, esta funcion realiza el proceso de ingesta y estadistica,
        en donde ingresa un(os) archivo(s) CSV a la base de datos y actualiza las estadísticas acumuladas.

        Args:
            path (Path): Ruta del archivo CSV a procesar.
            mode (str): Modo de actualización de estadísticas:
                - "row": Actualiza por cada fila insertada (La ventaja que se tiene es que 
                    hace el update más preciso, pero se sacrifica el costo computacional).
                - "chunk": Actualiza por "lotes", usando el resumen del chunk (La ventaja que 
                    se tiene usando chunk, es que hace más eficiente el proceso).
            chunksize (int): Tamaño de los "lotes" a leer del archivo.
            config (dict): Diccionario de configuración que contiene:
                - 'CSV': Opciones para lectura de CSV.
                - 'SQL': Opciones para queries de inserción y actualización.

        Detalle del Workflow de la funcion:
            1. Lee el CSV en chunks usando `CSVReader`.
            2. Aplica transformaciones y limpieza con `limpieza_df`.
            3. Inserta los registros en la tabla 'events'.
            4. Actualiza las estadísticas acumuladas en memoria (`RunningStats`).
            5. Persiste las estadísticas en la base de datos tras cada chunk.

        Side Effects:
            - Inserta registros en la tabla 'events'.
            - Actualiza y persiste estadísticas en la base de datos.
            - Genera logs y prints de progreso.

        Raises:
            FileNotFoundError: Si el archivo CSV no existe.
            psycopg2.DatabaseError: Si ocurre un error durante la inserción o actualización.
            ValueError: Si el parámetro `mode` no es válido.
    """
    logger.info(f"→ Ingestando {path.name} ")
    print(f"→ Ingestando {path.name} ")
    
    # Cargamos las stats actuales de la BD
    rs = load_running_stats_from_db(config)
    
    # Usamos Pandas maneja parseo incremental
    csv_reader = CSVReader(config=config)
    for chunk in csv_reader.run(path, chunksize):
        # Normalizamos columnas esperadas
        chunk_limpio = (
            limpieza_df(chunk)
                .renombrar_columnas({"timestamp": "ts"})
                .cambiar_tipo_fecha(["ts"])
                .convertir_tipos({"price":"float", "user_id":"int"})
                .eliminar_nulos(["ts", "price", "user_id"])                
                .eliminar_duplicados()
                .resultado()
        )

        chunk_limpio['updated_by'] = getpass.getuser()
        # Cargamos la data a la BD
        rows = list(chunk_limpio[["user_id", "price", "ts", "updated_by"]].itertuples(index=False, name=None))
        insertados = insert_events(rows, config)

        # Actualización de estadísticas (Sin tocar el historico ya cargado en la BD)
        if mode == "row" and insertados != 0 :
            # Altualizamos segun cada row insertado en la bd
            for x in chunk["price"].astype(float).tolist():
                rs.update_one(float(x))
        if mode == "chunk": 
            # Calcula resumen del chunk y fusiona
            cnt = int(chunk.shape[0])
            if cnt > 0:
                mn = float(chunk["price"].min())
                mx = float(chunk["price"].max())
                mean = float(chunk["price"].mean())
                rs.merge_batch(cnt, mean, mn, mx)

        ## Persistimos progreso tras cada chunk
        persist_running_stats(rs, config)
        logger.info(f"   + {insertados} filas. Stats parciales → n={rs.count} mean={rs.mean:.2f} min={rs.min:.2f} max={rs.max:.2f}")
        print(f"   + {insertados} filas. Stats parciales → n={rs.count} mean={rs.mean:.2f} min={rs.min:.2f} max={rs.max:.2f}")



    logger.info(f"✓ Terminado {path.name}. Stats finales → n={rs.count} mean={rs.mean:.2f} min={rs.min:.2f} max={rs.max:.2f}")
    print(f"✓ Terminado {path.name}. Stats finales → n={rs.count} mean={rs.mean:.2f} min={rs.min:.2f} max={rs.max:.2f}")
