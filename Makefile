init_docker:
	docker compose up -d

docker_stop:
	docker stop $(docker ps -q)

db_init:
	python main.py initdb

load: 
	python main.py load

load_chunk:
	python main.py load --mode chunk --chunksize 5

load_full:
	python main.py load --mode row --include-validation 

load_full_chunk:
	python main.py load --mode chunk --chunksize 5 --include-validation 

load_val:
	python main.py load --mode row --single validation.csv 

db_stats:
	python main.py db-stats

stats:
	python main.py print-stats

unit_test:
	pytest