init_docker:
	docker compose up -d

docker_stop:
	docker stop $(docker ps -q)

db_init:
	python main.py initdb

load: 
	python main.py load

load_full:
	python main.py load --mode row --include-validation 

load_chunk:
	python main.py load --mode chunk --chunksize 5

load_full_chunk:
	python main.py load --mode chunk --chunksize 5 --include-validation 

load_val:
	python main.py load --mode row --single validation.csv 

stats:
	python main.py print-stats

db_stats:
	python main.py db-stats

unit_test:
	pytest

llm:
	python main.py llm --limit_rows 1000