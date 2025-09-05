init_docker: ## Levanta servicios en segundo plano de Docker
	docker compose up -d

docker_down: ## Detiene y elimina los servicios docker
	docker compose down

db_init: ## Carga el schema a la base de datos
	python main.py initdb

load: ## Proceso de carga de CSV sin validation.csv
	python main.py load

load_full: ## Proceso de carga de CSV con validation.csv
	python main.py load --mode row --include-validation 

load_chunk: ## Proceso de carga parseada de CSV sin validation.csv
	python main.py load --mode chunk --chunksize 5

load_full_chunk: ## Proceso de parseada carga de CSV con validation.csv
	python main.py load --mode chunk --chunksize 5 --include-validation 

load_val: ## Proceso de carga de archivo validation.csv
	python main.py load --mode row --single validation.csv 

stats: ## imprimir estadisticas acumuladas
	python main.py print-stats

db_stats: ## Imprimir estadisticas de la base de datos
	python main.py db-stats

unit_test: ## Realizar test a las funciones del pipeline
	pytest

llm: ## Carga los datos al llm e interactua con lenguaje natural 
	python main.py llm --limit_rows 1000