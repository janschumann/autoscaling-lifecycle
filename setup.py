import setuptools

with open("README.md", "r") as fh:
	long_description = fh.read()

setuptools.setup(
	name = "AutoscalingLifecycle",
	version = "0.4.2",
	author = "Jan Schumann",
	author_email = "js@schumann-it.com",
	description = "A library to handle aws autoscaling lifecycle events",
	long_description = long_description,
	long_description_content_type = "text/markdown",
	url = "https://github.com/7NXT/infrastructure-autoscaling-lifecycle",
	packages = setuptools.find_packages(),
	classifiers = (
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
	),
)
