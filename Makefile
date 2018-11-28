deps:
	python3.6 -m venv pyenv
	pyenv/bin/pip install --upgrade -r requirements.txt

test: deps
	pyenv/bin/python -m unittest discover

show-version:
	@cat setup.py | grep version | sed 's/.*version = "//' | sed 's/",//'

update-version:
	@sed -i "" "s/    version = \".*\"/    version = \"$(VERSION)\"/" setup.py
