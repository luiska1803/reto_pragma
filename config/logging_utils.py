import logging
import os
from datetime import datetime

def get_logger(ver_cli=False) ->  logging.Logger:
    """
        Crea un Logger para visualizacion de logs:
        El logger escribe en archivo .log ubicado en la carpeta destinada. 
        Se puede indicar ver_cli = True para ver el log por consola
    """

    LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(LOG_DIR, exist_ok=True)

    RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
    LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{RUN_ID}.log")

    # Logger global
    logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        if ver_cli: #Para visualización por consola de los logs. 
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            logger.addHandler(ch)

        fh = logging.FileHandler(LOG_FILE, mode="w")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(fh)

    return logger