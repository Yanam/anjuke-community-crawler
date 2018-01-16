default:
	./ve python main.py

install:
	python3 -m venv .virtualenv
	./ve pip install --upgrade pip
	./ve pip install -r requirements.txt