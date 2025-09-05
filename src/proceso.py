import click
from pathlib import Path
from config.load_config import cargar_config

from src.modulos.db import init_db, fetch_db_stats, get_running_stats, db_query
from src.modulos.ingesta import ingest_file, iter_csv_files
from src.submodulos.llm import VectorStoreLLM


@click.group()
@click.option("--config", default="config/config.yaml", show_default=True, help="Ruta al archivo de configuración YAML.")
@click.pass_context
def cli(ctx, config):
    """
        CLI del reto Pragma: inicializa DB, ingesta CSV, muestra estadísticas.
    """
    #Esta funcion crea el grupo de comandos para la ejecucion del pipeline, teniendo en cuenta el archivo config.yaml
    ctx.ensure_object(dict)
    ctx.obj["config"] = cargar_config(config)

@cli.command()
@click.pass_context
def initdb(ctx):
    """
        Crea tablas y la fila inicial de la tabla 'running_stats'.
    """
    # Esta función trabaja con la ruta al archivo schema.slq que se describe en el archivo config.yaml
    config = ctx.obj["config"]
    init_db(config)
    click.echo("DB inicializada.")


@cli.command()
@click.option("--mode", type=click.Choice(["row", "chunk"]), default="row", show_default=True)
@click.option("--chunksize", type=int, default=1, show_default=True, help="Tamaño de procesamiento del CSV, default = 1, por row")
@click.option("--include-validation", is_flag=True, help="Ingresa también validation.csv")
@click.option("--single", type=str, default=None, help="Procesa solo un archivo por nombre (opcional)")
@click.pass_context
def load(ctx, mode, include_validation, single, chunksize):
    """
        Carga los archivos CSV (y opcionalmente validation.csv); 
        tambien puede cargar el archivo CSV individualmente si se le indica.  
        Todo lo hace con estadísticas en ejecución.
    """
    # Esta es la funcion principal del pipeline, en donde ejecuta el proceso de ingesta y realiza las estadisticas 
    config = ctx.obj["config"]
    if single:
        path_ = Path(config['CSV'].get("CSV_DIR")) / single
        ingest_file(path_, mode, chunksize, config)
        return
    for path_ in iter_csv_files(config, include_validation):
        ingest_file(path_, mode, chunksize, config)

@cli.command()
@click.pass_context
def print_stats(ctx):
    """
        Imprime las estadísticas en ejecución almacenadas.
    """
    # Funcion para imprimir todas las estadisticas almacenadas en la ejecucion 
    config = ctx.obj["config"]
    rs = get_running_stats(config)
    click.echo(
    f"RunningStats → Conteo={rs['count']} Promedio: {rs['mean']:.2f} Minimo={rs['min']:.2f} Maximo={rs['max']:.2f} Actualizado en: {rs['updated_at']}"
    )

@cli.command()
@click.pass_context
def db_stats(ctx):
    """
        Consulta en DB: count/avg/min/max calculados por SQL (para verificación).
    """
    # Esta funcion si entra en la BD y realiza la consulta indicada. 
    config = ctx.obj["config"]
    stats = fetch_db_stats(config)

    avg_price = f"{stats['avg_price']:.2f}" if stats['avg_price'] is not None else "nan"
    min_price = f"{stats['min_price']:.2f}" if stats['min_price'] is not None else "nan"
    max_price = f"{stats['max_price']:.2f}" if stats['max_price'] is not None else "nan"

    click.echo(
        f"DB Stats → Total de registros: {stats['total_rows']} "
        f"Promedio de precio: {avg_price} precio minimo: {min_price} precio maximo: {max_price}"
    )

@cli.command()
@click.option("--limit_rows", type=int, default=1000, help="limite de rows que tomara para trabajar con LLM")
@click.pass_context
def llm(ctx, limit_rows):
    """
        Carga la info que tengas en la DB y la usa para entrenar un LLM y poder hacer preguntas
    """
    # Esta funcion establece un LLM para interacion con lenguaje natural
    config = ctx.obj["config"]
    df = db_query(config, limit_rows)
    vector = VectorStoreLLM(df, config)
    while True:
        print("\n\n-------------------------------")
        pregunta = input("Hasme tu pregunta (q para salir): ")
        print("\n\n")
        if pregunta == "q":
            break
        
        respuesta = vector.get_pregunta(pregunta)
        print(respuesta)