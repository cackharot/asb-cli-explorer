.PHONY: all install clean build release

# coverage related
SRC = asb_tour
COV = --cov=$(SRC) --cov-branch --cov-report=term-missing

all:
	@grep -E "^\w+:" Makefile | cut -d: -f1

install:
	pip install -r requirements.txt -e .
	pip install -r requirements-test.txt .

clean: clean-build clean-pyc

clean-build:
	rm -rf build dist

clean-pyc:
	find . -type f -name *.pyc -delete

build: clean
	python setup.py sdist bdist_wheel

release: build
	twine upload dist/*

test:
	pytest $(COV)

cov-report:
	coverage report -m
