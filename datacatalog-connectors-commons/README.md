# datacatalog-connectors-commons

Common resources for Data Catalog connectors.

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Installable file build process](#1-installable-file-build-process)
  * [1.1. Get the code](#11-get-the-code)
  * [1.2. Virtualenv](#12-virtualenv)
      - [1.2.1. Install Python 3.5](#121-install-python-35)
      - [1.2.2. Create and activate a *virtualenv*](#122-create-and-activate-a-virtualenv)
  * [1.3. Generate a *wheel* file](#13-generate-a-wheel-file)
- [2. Developer environment](#2-developer-environment)
  * [2.1. Install and run YAPF formatter](#21-install-and-run-yapf-formatter)
  * [2.2. Install and run Flake8 linter](#22-install-and-run-flake8-linter)
  * [2.3. Install the package in editable mode (i.e. setuptools “develop mode”)](#23-install-the-package-in-editable-mode-ie-setuptools-develop-mode)
  * [2.4. Run the unit tests](#24-run-the-unit-tests)

<!-- tocstop -->

-----

## 1. Installable file build process

### 1.1. Get the code

````bash
git clone https://.../datacatalog-custom-type-ingestor.git
cd datacatalog-custom-type-ingestor
````

### 1.2. Virtualenv

Using *virtualenv* is optional, but strongly recommended.

##### 1.2.1. Install Python 3.5

##### 1.2.2. Create and activate a *virtualenv*

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

### 1.3. Generate a *wheel* file

```bash
python setup.py bdist_wheel
```

> The wheel file can used to install the package as a local pip dependency to
> other projects while it's not published to The Python Package Index (PyPI).

## 2. Developer environment

### 2.1. Install and run YAPF formatter

```bash
pip install --upgrade yapf

# Auto update files
yapf --in-place --recursive src tests

# Show diff
yapf --diff --recursive src tests

# Set up pre-commit hook
# From the root of your git project.
curl -o pre-commit.sh https://raw.githubusercontent.com/google/yapf/master/plugins/pre-commit.sh
chmod a+x pre-commit.sh
mv pre-commit.sh .git/hooks/pre-commit
```

### 2.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 2.3. Install the package in editable mode (i.e. setuptools “develop mode”)

```bash
pip install --editable .
```

### 2.4. Run the unit tests

```bash
pip install ./lib/datacatalog_connectors_commons_test-1.0.0-py2.py3-none-any.whl
pip install pytest mock
python setup.py test
```
