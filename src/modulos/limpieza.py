import pandas as pd

from config.logging_utils import get_logger
logger = get_logger()

class limpieza_df: 
    """
        Clase para limpieza de dataframes.

            Attributes:
            -----------
            df : pd.DataFrame
                Dataframe de pandas que se utilizara para las funciones de limpieza
        Metodos:
        --------
            eliminar_nulos(columnas: list)
                Elimina filas con valores nulos del DataFrame.

            eliminar_duplicados (columnas: list)
                Elimina elemntos duplicados en el dataframe.

            convertir_tipos(conversiones: dict)
                Convierte los tipos de datos de las columnas especificadas en el DataFrame.

            cambiar_tipo_fecha(col_fecha: list)
                Convierte columnas de fecha a tipo timestamp en el DataFrame.
            
            renombrar_columnas(col_renom: dict)
                Renombra columnas específicas del DataFrame según el diccionario proporcionado.
            
            resultado
                Devuelve el DataFrame resultante de la clase después de aplicar las transformaciones.
    """
    def __init__(self, df : pd.DataFrame):
        """ 
            Funcion init de la clase. 
            Args:
                df (pd.DataFrame): Dataframe de pandas con el que se iniciara el proceso 
                    de limpieza
        """
        self.df = df
            
    def eliminar_nulos(
        self, 
        columnas: list = None
    ):
        """ 
            Elimina filas con valores nulos del DataFrame.
            Args:
                columnas (list, optional): Lista de nombres de columnas sobre las cuales
                    se verifican los valores nulos. Si no se proporciona, se eliminan
                    todas las filas que contengan al menos un valor nulo en cualquier columna.

            Returns:
                self: Devuelve la instancia del objeto con el DataFrame actualizado.
        """
        logger.info(f"Eliminando Valores Nulos de las columnas [{columnas}]")
        self.df = self.df.dropna(subset=columnas) if columnas else self.df.dropna()

        return self

    def eliminar_duplicados(
        self, 
        columnas: list = None
    ):
        """ 
            Elimina filas duplicadas del DataFrame.
            Args:
                columnas (list, optional): Lista de nombres de columnas sobre las cuales
                    se verifican duplicados. Si no se proporciona, se consideran todas 
                    las columnas del DataFrame para detectar duplicados.

            Returns:
                self: Devuelve la instancia del objeto con el DataFrame actualizado.
        """
        logger.info(f"Eliminando Valores duplicados de las columnas: [{columnas}]")
        self.df = self.df.drop_duplicates(subset=columnas)

        return self

    def convertir_tipos(
        self, 
        conversiones: dict
    ):
        """ 
            Convierte los tipos de datos de las columnas especificadas en el DataFrame.
            Args:
                conversiones (dict): Diccionario donde las claves son los nombres de 
                    las columnas a convertir y los valores son los tipos de datos 
                    a los que se desea convertir, por ejemplo {'col1': 'int', 'col2': 'float'}.

            Returns:
                self: Devuelve la instancia del objeto con el DataFrame actualizado.
        """
        for col_name, tipo in conversiones.items():
            logger.info(f"Convirtiendo columna: '{col_name}' en tipo: '{tipo}'")
            self.df[col_name] = self.df[col_name].astype(tipo)

        return self

    def cambiar_tipo_fecha(
        self, 
        col_fecha: list = None
    ):
        """ 
            Convierte columnas de fecha a tipo timestamp en el DataFrame.
            Args:
                col_fecha (list): Lista de nombres de columnas que contienen fechas
                    y que se desean convertir a tipo timestamp. Las fechas deben estar
                    en formato "%m/%d/%Y". Los valores que no se puedan convertir
                    se marcarán como NaT.

            Returns:
                self: Devuelve la instancia del objeto con el DataFrame actualizado.
        """
        logger.info(f"Cambiando columnas: {col_fecha} a tipo timestamp")
    
        for col_ in col_fecha:
            self.df[col_] = pd.to_datetime(self.df[col_], format="%m/%d/%Y", utc=True, errors="coerce")

        return self
    
    def renombrar_columnas(
        self, 
        col_renom: dict
    ):
        """ 
            Renombra columnas específicas del DataFrame según el diccionario proporcionado.
            Args:
                col_renom (dict): Diccionario donde las claves son los nombres actuales
                    de las columnas y los valores son los nuevos nombres deseados, 
                    por ejemplo {'col_antigua': 'col_nueva'}.

            Returns:
                self: Devuelve la instancia del objeto con el DataFrame actualizado.
        """
        logger.info(f"Renombrando columnas: {col_renom}")
            
        self.df = self.df.rename(columns=col_renom)

        return self


    def resultado(
        self
    ) -> pd.DataFrame:
        """ 
            Devuelve el DataFrame resultante de la clase después de aplicar las transformaciones.
        Returns:
            pd.DataFrame: El DataFrame final con todas las modificaciones realizadas,
                útil para encadenamiento de métodos o análisis posterior.
        """
        return self.df