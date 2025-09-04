from dataclasses import dataclass

@dataclass
class RunningStats:
    """
        Clase para mantener estadísticas en tiempo real de una serie de valores numéricos.

            Attributes:
            -----------
            count : int
                Número de elementos agregados.
            mean : float
                Media de los valores.
            min : float
                Valor mínimo registrado.
            max : float
                Valor máximo registrado.

        Metodos:
        --------
            update_one(price: float)
                Actualiza las estadísticas agregando un único valor de price.

            merge_batch(cnt: int, mean: float, mn: float, mx: float)
                Combina un resumen de batch con las estadísticas actuales sin recorrer el 
                historial completo.
    """
    count: int = 0
    mean: float = 0.0
    min: float = float("inf")
    max: float = float("-inf")

    def update_one(self, price: float):
        """
            Actualiza las estadísticas realizadas con un único valor.

            Args:
            -----
                price : float 
                    Nuevo valor a agregar a las estadísticas.
            
            Actualizaciones:
            --------
                - count (contador de la nueva inserción) incrementado en 1 cada vez que 
                    se inserta un nuevo registro

                - mean (actualizado con la nueva media incremental) dado por la formula:
                        mean' = mean + (price - mean) / n'
                    Donde mean es la media, price es el precio, n' es el contador actualizado 

                - min : Se actualiza si price es un numero menor al que se tenia registro
                - max : Se actualiza si price es un numero mayor al que se tenia registrado
        """
        # Actualizamos el contador basados en que: n' = n + 1
        n1 = self.count + 1
        # Actualizamos la media basados en la formula: mean' = mean + (price - mean) / n'
        new_mean = self.mean + (price - self.mean) / n1
        self.count = n1
        self.mean = new_mean
        if price < self.min:
            self.min = price
        if price > self.max:
            self.max = price

    def merge_batch(self, cnt: int, mean: float, min: float, max: float):
        """
        Combina un batch de estadísticas con las estadísticas globales actuales.
        Para esta parte se tiene en cuenta la ponderacion de elementos, es decir, que se basa
        en la formula:
            
            m = (m1*n1 + m2*n2)/(n1+n2)
        
        Donde m seria la nueva media, m1 seria la media registrada anteriormente, n1 el conteo 
        registrado anteriormente, m2 la media otorgada en el batch, n2 el numero de elementos en 
        el batch. De manera que obtendriamos:

            combined_mean = (self.mean * self.count + mean * cnt) / (self.count + cnt)

        Args:
        -----
            cnt : int
                Número de elementos en el batch.
            mean : float
                Media de los valores en el batch.
            min : float
                Valor mínimo del batch.
            max : float
                Valor máximo del batch.
        
        Notes:
        ------
            - Se combina la media por ponderación usando count (contador general) 
                y cnt que es el numeor de elementos a ingresar.
            - No se recorren los valores individuales del batch.
            - Si cnt es 0, no se realiza ninguna actualización, esto para prevenir 
                los chunks que toman dataframes vacios.
        """
        if cnt == 0:
            return
        # Establecemos n1 + n2 (denominador de la formula) 
        total = self.count + cnt
        if total == 0:
            return
        # Nos basamos en la formula para combinar medias por pesos: m = (m1*n1 + m2*n2)/(n1+n2)
        # Para no tener errores en la ejecución, devolvemos 0.0 si n1 + n2 es igual a 0.
        combined_mean = (self.mean * self.count + mean * cnt) / total if total > 0 else 0.0 
        self.mean = combined_mean
        self.count = total
        self.min = min(self.min, min)
        self.max = max(self.max, max)