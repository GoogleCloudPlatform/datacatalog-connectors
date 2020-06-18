# google-datacatalog-connectors-commons

Common resources for Data Catalog connectors.

[![Python package][1]][1] [![PyPi][2]][3] [![License][4]][4] [![Issues][5]][6]

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
      - [1.2.1. Install Python 3.6](#121-install-python-36)
      - [1.2.2. Create and activate a *virtualenv*](#122-create-and-activate-a-virtualenv)
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
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors
cd datacatalog-connectors/google-datacatalog-connectors-commons
````

### 1.2. Virtualenv

Using *virtualenv* is optional, but strongly recommended.

##### 1.2.1. Install Python 3.6

##### 1.2.2. Create and activate a *virtualenv*

```bash
pip3 install --upgrade virtualenv
python3 -m virtualenv --python python3.6 env
source ./env/bin/activate
```

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
python setup.py test
```

[1]: https://github.com/GoogleCloudPlatform/datacatalog-connectors/workflows/Python%20package/badge.svg?branch=master
[2]: https://img.shields.io/pypi/v/google-datacatalog-connectors-commons.svg
[3]: https://pypi.org/project/google-datacatalog-connectors-commons/
[4]: https://img.shields.io/github/license/GoogleCloudPlatform/datacatalog-connectors.svg
[5]: https://img.shields.io/github/issues/GoogleCloudPlatform/datacatalog-connectors.svg
[6]: https://github.com/GoogleCloudPlatform/datacatalog-connectors/issues
