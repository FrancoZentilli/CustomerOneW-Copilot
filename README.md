# CustomerOneW-Copilot (MVP)

Create tools in order to make easier develop the CustomerOneW asset.

# Installation

## Clone the git repository

```shell
git clone https://github.com/FrancoZentilli/CustomerOneW-Copilot.git
```

## Create a new anaconda environment

```shell
conda create -n <env_name> python=3.8
conda activate
```

## Install the library requirements
```shell
pip install -r requirements.txt
```

## Run all unitary test (optional)

```shell
python -m unittest discover -p "test_*.py"
```

## How to use

### Catalog Reader

```python
# your_script.py

# (1) import package
from tools.catalog_reader import CatalogReader

# (2) Instantiate Catalog Reader class with respective inputs
path_repo =  "path/to/customerone-w"
path_catalog = "realm/<uc_name>/conf/base/uc/.../catalog.yaml"
catalog_reader = CatalogReader(path_catalog=path_catalog, path_repo=path_repo)

# (3) Get elements names found (by default is tables)
all_keys = catalog_reader.get_elements_keys(labels_subset=["tables", "pickle"])

# (4) Get elements
for key in all_keys:
    print(catalog_reader.get_element(elem_name=key))
```


