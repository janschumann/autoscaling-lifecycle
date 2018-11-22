deps:
	python3.6 -m venv pyenv
	pyenv/bin/pip install --upgrade -r requirements.txt

test: deps
	pyenv/bin/python -m unittest discover
