import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

from dotenv import load_dotenv
from config.logging_utils import get_logger
logger = get_logger()
load_dotenv()

def get_conn():
    """
        Crea y devuelve una conexión a la base de datos PostgreSQL usando variables de entorno.

        Variables de entorno requeridas:
            - POSTGRES_USER: Usuario de la base de datos.
            - POSTGRES_PASSWORD: Contraseña del usuario.
            - POSTGRES_DB: Nombre de la base de datos.
            - PGHOST: Host del servidor de PostgreSQL.
            - PGPORT: Puerto del servidor de PostgreSQL.

        Returns:
            psycopg2.extensions.connection: Objeto de conexión a la base de datos.
        
        Raises:
            psycopg2.OperationalError: Si la conexión no puede establecerse.

    """

    PG_USER = os.getenv("POSTGRES_USER")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    PG_DB = os.getenv("POSTGRES_DB")
    PG_HOST = os.getenv("PGHOST")
    PG_PORT = int(os.getenv("PGPORT"))
    
    logger.info(f"Conexión a base de datos {PG_DB} establecida con usuario {PG_USER} .")

    return psycopg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )


def init_db(config):
    """
        Inicializa la base de datos ejecutando un script SQL.

        Args:
            config (dict): Diccionario de configuración que contiene la clave 'SQL' 
                con la ruta relativa del script SQL bajo la clave 'sql_path'.
                
        Raises:
            FileNotFoundError: Si el archivo SQL no existe.
            psycopg2.DatabaseError: Si ocurre un error al ejecutar el script.
    """
    config = config['SQL']

    sql_path = Path(__file__).parents[2] / config.get("sql_path")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_path.read_text())
        conn.commit()
        logger.info('DB inicializada.')

def insert_events(rows, config):
    """
        Inserta registros en la tabla 'events'.

        Args:
            rows (list[tuple]): Lista de tuplas con los valores a insertar.
                Formato esperado: (user_id, price, ts, updated_by).
            config (dict): Diccionario de configuración que contiene la clave 'SQL'
                con 'insert_query' (query para insertar los datos) y 'page_size'.

        Returns:
            - len(rows): Número de filas insertadas.
            - Retorna 0 si `rows` está vacío.

    """
    if not rows:
        return 0
    
    config = config['SQL']
    page_size = config.get("page_size")
    query = config.get("insert_query")   

    with get_conn() as conn, conn.cursor() as cur:
        # excecute_values para inserción en batch.
        execute_values(cur, query, rows, page_size=page_size)
        conn.commit()
    
    logger.info(f"Se agregaron {len(rows)} nuevas filas a la tabla 'events'.")

    return len(rows) 


def fetch_db_stats(config):
    """
        Obtiene estadísticas agregadas de la tabla 'events'.

        Args:
            config (dict): Diccionario de configuración que contiene la clave 'SQL'
                con la query bajo la clave 'query_stats'.

        Returns:
            dict: Diccionario con estadísticas de la base de datos:
                {
                    "total_rows": int,
                    "avg_price": float,
                    "min_price": float,
                    "max_price": float
                }

    """
    config = config['SQL']
    query = config.get("query_stats")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        r = cur.fetchone()
        return {
            "total_rows": r[0] or 0,
            "avg_price": r[1],
            "min_price": r[2],
            "max_price": r[3],
        }


def get_running_stats(config):
    """
        Obtiene las estadísticas acumuladas (running stats) de la base de datos.

        Args:
            config (dict): Diccionario de configuración que contiene la clave 'SQL'
                con la query bajo la clave 'query_running'.

        Returns:
            dict: Diccionario con estadísticas en ejecución:
                {
                    "count": int,
                    "mean": float,
                    "min": float,
                    "max": float,
                    "updated_at": datetime
                }

    """
    config = config['SQL']
    query = config.get("query_running")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query)
        r = cur.fetchone()
        return {"count": r[0], "mean": r[1], "min": r[2], "max": r[3], "updated_at": r[4]}


def update_running_stats(count, mean, min_, max_, config):
    """
        Actualiza las estadísticas acumuladas en la base de datos.

        Args:
            count (int): Cantidad de registros acumulados.
            mean (float): Media acumulada.
            min_ (float): Valor mínimo acumulado.
            max_ (float): Valor máximo acumulado.
            config (dict): Diccionario de configuración que contiene la clave 'SQL'
                con la query bajo la clave 'query_update'.

        Side Effects:
            - Ejecuta un UPDATE en la tabla de estadísticas.
        
    """
    config = config['SQL']
    query = config.get("query_update")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (count, mean, min_, max_))
        conn.commit()


def db_query(config, limit_rows: int = None) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL definida en el archivo de configuración y 
    devuelve el resultado como un DataFrame de Pandas.

    Args:
        config (dict): Diccionario de configuración que contiene la clave 'SQL'
            con la query bajo la clave 'query_llm'.
        limit_rows (int, optional): Límite de filas a retornar. 
            Si es None no se aplica límite.

    Returns:
        pd.DataFrame: Resultado de la consulta en un DataFrame.
    """
    config = config['SQL']
    query = config.get("query_llm")

    # Validar si aplicar límite
    if limit_rows is not None:
        if not isinstance(limit_rows, int) or limit_rows < 1:
            raise ValueError("limit_rows debe ser un entero positivo")
        query = f"{query} LIMIT {limit_rows};"

    # Ejecutar query y pasar a DataFrame
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn)

    return df