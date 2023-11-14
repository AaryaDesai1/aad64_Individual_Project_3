install:
	pip install --upgrade pip && pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:
	black *.py 

lint:
	ruff check *.py mylib/*.py


all: install lint test format deploy

job:
	python pipeline.py