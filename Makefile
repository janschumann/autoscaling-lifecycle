deps:
	python3.6 -m venv .venv
	.venv/bin/pip install --upgrade -r requirements.txt

test: deps
	.venv/bin/python -m unittest discover

show-version:
	@cat setup.py | grep version | sed 's/.*version = "//' | sed 's/",//'

update-version:
	@sed -i "" "s/    version = \".*\"/    version = \"$(VERSION)\"/" setup.py
