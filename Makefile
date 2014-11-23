SHELL := /bin/bash

html:
	(cd docs && $(MAKE) html)

test:
	nosetests -s
