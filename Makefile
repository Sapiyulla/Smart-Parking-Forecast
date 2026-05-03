init:
	python -c "open('.env', 'a').close()"
	python -c "import shutil; shutil.copy('.env.example', '.env')"
	docker compose up --build -d
	echo -c "Service runned." \
		"Open http://localhost:8080/ for show Airflow DAG`s." \
		"Start DAG`s as showed from doc."