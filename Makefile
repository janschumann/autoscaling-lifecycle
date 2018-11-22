deps:
	python3.6 -m venv pyenv
	pyenv/bin/pip install --upgrade -r requirements.txt

test: deps
	python3.6 -m unittest discover
