deps:
	python -m venv pyenv
	pyenv/bin/pip install --upgrade -r requirements.txt

test: deps
	python -m unittest discover
