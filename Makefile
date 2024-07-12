docker-spin-up:
	docker compose up --build -d

perms:
	sudo mkdir -p airflow dbt images terraform && sudo chmod -R u=rwx,g=rwx,o=rwx airflow dbt images terraform

up: perms docker-spin-up

down:
	docker compose down --volumes --rmi all

########################################################################################
pytest:
	docker exec airflow_webserver pytest -p no:warnings -v /opt/airflow/tests

format:
	docker exec airflow_webserver black .

isort:
	docker exec airflow_webserver isort .

lint:
	docker exec airflow_webserver flake8 /opt/airflow/dags --max-line-length 88

ci: isort format lint

####################################################################################################################
# Set up cloud infrastructure

tf-init:
	terraform -chdir=./terraform init

infra-up:
	terraform -chdir=./terraform apply

infra-down:
	terraform -chdir=./terraform destroy

infra-config:
	terraform -chdir=./terraform output