init:
	python -c "open('.env', 'a').close()"
	python -c "import shutil; shutil.copy('.env.example', '.env')"
	docker compose up --build -d