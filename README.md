
# RETO PRAGMA SAS (Prueba de ingeniería de datos)
#### Autor: Luis Carlos Sanchez Monroy - luiska1803@gmail.com
## Solución al planteamiento del reto:

Este proyecto implementa un **pipeline de ingesta de datos** desde archivos CSV hacia una base de datos PostgreSQL, con el cálculo incremental de estadísticas (`count`, `mean`, `min`, `max`) sobre el campo `price`.  

**Data:** Los archivos a procesar son:

   - `2012-1.csv`
   - `2012-2.csv`
   - `2012-3.csv`
   - `2012-4.csv`
   - `2012-5.csv`
   - `validation.csv` (Este archivo se utiliza solo al final para la validación).

Esta data cuenta con los campos `timestamp`, `price` y `user_id`; la data consta de registros de compra de los usuarios en una fecha especifica. Como buena practica, se debe normalizar la data para la subida a la base de datos, de igual manera se hara un proceso de limpieza de la data, donde se eliminaran datos nulos y repetidos, finalmente se añadira un campo de `updated_by` con el fin de saber el usuario que actualizo la data. 

**Particionamiento planteado:** Dado que los archivos estan en una forma con una estructura de fecha, el particionamiento se toma por fecha en forma lineal, esto hace que el pipeline sea secuencial y se puedan observar los datos mas facilmente. 

**Bases de datos escogidas:** Se escoge la base de datos de postgreSQL ya que es un motor que soporta ANSI SQL, lo cual hace que se puedan usar funciones de agregación, ademas dado que es open source no necesitamos de una licencia para su uso. De igual forma tambien se coloca pgadmin con el fin de facilitar la lectura en la base de datos.

**Modelo propuesto:** El modelo propuesto es un modelo estrella, ya que separa la tabla de hechos `events` y la tabla de estadisticas `running_stats`, esto facilita consultas y es muy usado en entornos Data Warehosue orientado a consultas. 

**Diagrama de Flujo:**

![alt text](<diagrama de flujo pipeline.png>)

**Agregaciones extras:**

De manera extra, este proyecto tambien establace un entrenamiento de modelo LLM, con el que se puede interactuar y realizar preguntas libres sobre la data presentada. 

## 📂 Estructura del proyecto

      ├── data /                          # Carpeta donde estan los datos "brutos" 
      |   ├── 2012-1.csv
      |   ├── 2012-2.csv
      |   ├── 2012-3.csv
      |   ├── 2012-4.csv
      |   ├── 2012-5.csv
      |   └── validation.csv 
      ├── config /
      |   ├── sql/
      |   |    └── schema.sql             # Archivo SQL con el schema propuesto.
      |   ├── config.yaml                 # Archivo yaml con la configuracion general del proyecto.
      |   ├── load_config.yaml            # Archivo para cargar configuración del archivo config.yaml.
      |   └── logging_utils.py            # Archivo de logger para creacion de logs.
      ├── src /
      |   ├── modulos/
      |   |   ├── db.py                   # Conexión a la DB, creación de tablas, queries
      |   |   ├── ingesta.py              # Archivo de ingesta y proceso del workflow.
      |   |   ├── limpieza.py             # Archivo de limpieza del(os) Dataframe(s)
      |   |   └── stats.py                # Archivo de cálculo incremental de estadísticas
      |   └── submodulos/
      |   |   ├── csv_reader.py           # Archivo lector de CSV en filas o chunks
      |   |   └── llm.py                  # Archivo de configuracion de LLM.
      |   └── proceso.py                  # Archivo que contiene elproceso en forma CLI (con click) del proyecto.
      ├── test /
      |   ├── csv_reader_test.py          # Archivo test para csv_reader
      |   ├── ingesta_test.py             # Archivo test para ingesta
      |   └── limpieza_test.py            # Archivo test para limpieza
      ├── docker-compose.yaml             # Servicios de Postgres y PgAdmin
      ├── .env                            # Archivo con las variables de entorno necesarias.
      ├── requirements.txt                # Archivo txt con las librerias necesarias para el proyecto.
      ├── Makefile                        # Archivo Makefile con los comandos CLI "principales" para ejecutar el proyecto.
      ├── main.py                         # Archivo Main (llamado al CLI principal)
      ├── diagrama de flujo pipeline.png  # Diagrama de flujo del proyecto.
      └── README.md                       # Archivo README

## ⚙️ Requerimientos

Los requerimientos necesarios para este proyecto se pueden encontrar en el archivo `requirements.txt`, sin embargo, es necesario el tener:
   - Python 3.12 + 
   - Docker y Docker Compose (seguir la guía de instalacion desde el link https://docs.docker.com/engine/install/ ), pueden asegurarse de que tienen docker y docker Compose instalados con:
```bash
docker --version
docker compose version
```
   - Libreria `pip` para instalar librerias necesarias, instalar con:

```bash
   pip install -r requirements.txt
```
   - Ollama **(Opcional si se quiere acceder a la funcion llm)**, se puede descargar desde https://ollama.com/download y seguir las instrucciones segun el sistema operativo que se tenga, una vez se tenga instalado, se puede verificar con:

```bash
   ollama --version
```
   - Una vez se tenga ollama descargado, se requiere descargar los modelos necesarios (`mxbai-embed-large` y `llama3.2`), se pueden usar otros modelos, sin embargo es recomendable usar estos ya que son modelos free y no son muy pesados para su uso:

```bash
   ollama pull mxbai-embed-large
   ollama pull llama3.2
``` 

   - Puedes observar si tienes descargados los modelos con:

```bash
   ollama list
```   
   - Si se decide utilizar otro tipo de modelo, se deberia hacer el cambio en el archivo `config.yaml`

## 🚀 Ejecución del Proyecto

Para la ejecución del proyecto, puedes usar las funciones directas de python y de docker compose para levantar los servicios, o usar las funciones definidas en el archivo `Makefile`, te recomiendo usar `Makefile` por simplicidad. Si se decide ejecutar `python main.py` se mostrara los comandos disponibles y las opciones para ejecutarlos. 

### 1. Levantar servicios de base de datos

Primero se hace el levantamiento de servicios de base de datos, para este proyecto, se decidio utilizar postgreSQL y para su visualización pgadmin.
```bash
   # bash
   # Función directa de docker compose
   docker compose up -d
   # Funcion predeterminada con Makefile
   make init_docker
```
Al levantar los servicios de docker, se puede acceder a las bases de datos desde localhost:8080 y usar las credenciales para el acceso, normalmente estas credenciales no se comparten debido a fugas de seguridad pero debido a que se trata de un reto, estas credenciales se encuentran en el archivo `.env`, a su vez estas credenciales estaran a continuación, cabe aclarar que estas credenciales son variables de entorno, por lo que si se cambian tambien deberan acceder con las nuevas credenciales a las bases de datos, sin embargo no es encesario modificar ninguna parte del pipeline ya que todo el codigo actuara con esas credenciales. 
   
   - PGADMIN_EMAIL : admin@example.com
   - PGADMIN_PASSWORD : admin123
   - POSTGRES_USER : pragma_admin
   - POSTGRES_PASSWORD : pass123
   - POSTGRES_DB : db_pragma

Si quieres detener todos los servicios de docker, lo puedes hacer con el sigueinte codigo: 

```bash
   # bash
   # Detener todos los servicios de Docker.
   docker stop $(docker ps -q)
   # Detener uno a uno los servicios de docker
   docker stop <id_imagen_docker>
```


### 2. Creación de schema en PostgreSQL:

Para que el proyecto funcione se debe realizar el levantamiento del schema, en donde se crearan las tablas necesarias en postgreSQL.
``` bash
   # bash
   # Función directa de python, 
   python main.py initdb
   # Funcion predeterminada con Makefile
   make db_init
```

Luego de esto se podra acceder a pgadmin a traves del localhost:8080 y observar las tablas creadas.
``` sql
   -- SQL
   SELECT table_schema, table_name
   FROM information_schema.tables
   WHERE table_type = 'BASE TABLE'
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
   ORDER BY table_schema, table_name;
```
### 3. Inserción de los archivos CSV :
En este paso, se carga los archivos CSV, dependiendo de como se quiera la ingesta de los datos, puede ser excluyendo o incluyendo el archivo validation.csv; tambien puedes cargar archivo por archivo seleccionando cada archivo. Este proceso mostrara por consola todo el proceso de estadistica, mostrando el valor minimo, maximo, media y el conteo de filas que se han insertado. 

#### 3.1 Inserción de los archivos CSV: 
   - Excluyendo `validation.csv`:
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load
   # Función directa de python
   python main.py load
```
   - Incluyendo `validation.csv`:
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load_full
   # Función directa de python
   python main.py load --include-validation 
```
   - Excluyendo `validation.csv`, con parseo `chunk` incremental (Cambiar chunksize si se quiere probar otro valor):
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load_chunk
   # Función directa de python
   python main.py load --mode chunk --chunksize 5
```
   - Incluyendo `validation.csv`, con parseo `chunk` incremental (Cambiar chunksize si se quiere probar otro valor):
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load_full_chunk
   # Función directa de python
   python main.py load --mode chunk --chunksize 5 --include-validation 
```
   - Cargar el archivo `validation.csv`, (Cambiar el nombre del archivo si se quiere cargar uno es especifico):
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load_val
   # Función directa de python
   python main.py load --mode row --single validation.csv 
```
#### 3.2. Observar las estadisticas en ejecución almacenadas:
Para esta parte, se mostraran las estadisticas de ejecución almacenadas por consola, las cuales indicaran el conteo de registros, el valor medio, minimo y maximo registrado y finalmente la fecha y hora en la que se realizo la ultima ejecución.
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make stats
   # Función directa de python
   python main.py print-stats
```
#### 3.3. Observar las estadisticas en ejecución almacenadas:
Este proceso consulta en la base de datos, los valores registrados, devolviendo el conteo de los registros insertados en la base de datos junto a la media, el valor minimo y el valor maximo del precio, los calores se podran observar por consola. 
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make db_stats
   # Función directa de python
   python main.py db-stats
```	
#### 3.4. Observar las estadisticas en la base de datos
De igual forma, una vez que se ejecuta todo el proceso, se puede acceder a la base de datos con pgadmin y hacer las consultas que se deseen, a continuacion se muestra algunas queries que podrian ser interesantes para observar:
``` sql
   -- SQL
   -- Conteo, precio promedio, minimo y maximo. 
   SELECT COUNT(*) AS total_rows, AVG(price)::float8 AS avg_price, MIN(price)::float8 AS min_price, MAX(price)::float8 AS max_price FROM events;
   -- Observar las estadisticas en running_stats. 
   SELECT count, mean, min, max, updated_at FROM running_stats WHERE id = 1;
   -- Precio total gastado por usuario 
   SELECT user_id, SUM(price) AS gasto_total FROM events GROUP BY user_id ORDER BY gasto_total DESC;
   -- Precio promedio por usuario 
   SELECT user_id, AVG(price) AS promedio_gasto FROM events GROUP BY user_id ORDER BY promedio_gasto DESC;
```
### 4. Consulta por llm:
Este proceso se agrega con el fin de otorgar una función extra, como lo es las preguntas a un modelo de LLM, el cual se entrenara con la data (proporcionada en la base de datos) y respondera preguntas de acuerdo con esa data.
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make llm
   # Función directa de python
   python main.py llm
```	
Acá se presentara unas sugerencias de preguntas para el modelo LLM:

   - Cual fue el promedio de gastos por mes y dame un listado de los 5 primeros
   - Cual fue el usuario que tiene el mayor promedio de gastos y dame sus compras mas altas
   - Cual fue el usuario que menos gasto
   - En que fecha se obtuvieron los mayores y menores ingresos

### 5. Test.
Este proceso simplemente ejecuta los test de las funciones realizadas en el proyecto. Este proceso se realiza con pytest, por lo que si no se tiene instalado, lo pueden instalar con `pip intall pytest` o instalarlo con el archivo de `requirements.txt`. Para que los test funcionen de manera correcta se recomienda estar en la carpeta principal del proyecto. 

``` bash
   # bash
   # Funcion predeterminada con Makefile
   make test
   # Función directa de python
   pytest
```

## 📖⭐ Comprobación de resultados:
Esta parte del proyecto establecera el como se puede acceder a las respuestas propuestas a partir del pipeline establecido, se colocara la pregunta / acción a responder y la forma de ejecutarlo para que se pueda observar en consola o en la base de datos.
   
   - Imprime el valor actual de las estadísticas en ejecución.
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make stats
   # Función directa de python
   python main.py print-stats
```

   - Realiza una consulta en la base de datos del: recuento total de filas, valor promedio, valor mínimo y valor máximo para el campo “price”.
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make db_stats
   # Función directa de python
   python main.py db-stats
```	

   - Ejecuta el archivo “validation.csv” a través de todo el pipeline y muestra el valor de las estadísticas en ejecución.
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load_val
   # Función directa de python
   python main.py load --mode row --single validation.csv 
   # Si se quiere hacer por parseo (cambiar chunksize si se desea)
   python main.py load --mode chunk --single validation.csv --chunksize 5
```

   - Realice una nueva consulta en la base de datos después de cargar “validation.csv”, para observar cómo cambiaron los valores del: recuento total de filas valor promedio, valor mínimo y valor máximo para el campo “price”
``` bash
   # bash
   # Funcion predeterminada con Makefile
   make load_val
   make db_stats
   # Función directa de python
   python main.py load --mode row --single validation.csv 
   # Si se quiere hacer por parseo (cambiar chunksize si se desea)
   python main.py load --mode chunk --single validation.csv --chunksize 5 
   python main.py db-stats
```

## 📊 Posibles mejoras futuras.

Como futuras posibles mejoras para este proyecto, se podrian hacer:
   - Usar procesos en paralelo en la carga para hacer mas eficiente el proceso de ingesta. 
   - Particionamiento de las tablas (ej. `año/mes` en la tabla `events`) para mejorar las consultas temporales.
   - Utilizar una nube (AWS, GCP, Azure, etc) para el almacenamiento de los datos, si es que la data llegara a ser mucho mas grande. 
   - Hacer que el modelo LLM no solo responda preguntas de los usuarios, sino que interactue con el pipeline y ejecute ciertas funciones y que actualice la bd en lugar de solo leerla. 
   
