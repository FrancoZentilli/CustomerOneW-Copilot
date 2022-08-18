import pandas as pd
from pathlib import Path
import os
import yaml
from kedro.extras.datasets.pickle import PickleDataSet

FILE_TYPE_DICT = {"pandas.ParquetDataSet": {"reader": pd.read_parquet,
                                            "label": "table"},
                  "spark.SparkDataSet": {"reader": pd.read_parquet,
                                         "label": "table"},
                  "pickle.PickleDataSet": {"reader": PickleDataSet,
                                           "label": "pickle"}}


class CatalogReader:

    @staticmethod
    def get_globals_paths(path_repo, global_dir="globals", globals_file="dev.yaml", base_path_name="base_path",
                          use_case=None):
        globals_dict = {}
        yaml_file = {}
        path_uc = path_repo / "realm/{}".format(use_case)
        for (root, dirs, files) in os.walk(path_uc):
            if root.endswith(global_dir):
                path_globals = Path(root) / globals_file
                with open(path_globals) as file:
                    yaml_file = yaml.load(file, Loader=yaml.FullLoader)

        base_path = yaml_file[base_path_name]
        var_suffix = "${.%s}" % base_path_name
        for k in yaml_file:
            if k.endswith("path"):
                globals_dict[k] = path_uc / yaml_file[k].replace(var_suffix, base_path)

        return globals_dict

    @staticmethod
    def get_use_case(path_catalog, realm_name="realm"):
        split_path = path_catalog.parts
        for i, c in enumerate(split_path):
            if c == realm_name:
                return split_path[i + 1]

    @staticmethod
    def get_catalog_paths(path_catalog, globals_paths, file_type_dict):
        catalog_paths = {}

        with open(path_catalog) as file:
            yaml_file = yaml.load(file, Loader=yaml.FullLoader)

        for elem in yaml_file:
            if elem.startswith("_"):
                continue
            final_path = ""
            name = elem.split("@")[0] if "@" in elem else elem
            file_path = yaml_file[elem]["filepath"]
            split_path = file_path.split("/")
            suffix = split_path[0]
            # TODO: starts with suffix always?
            for g in globals_paths:
                if g in suffix:
                    final_path = globals_paths[g] / file_path.replace(suffix, "")[1:]

            file_type = yaml_file[elem]["type"]
            label = file_type_dict[file_type]["label"]
            reader = file_type_dict[file_type]["reader"]

            if label == "pickle":
                final_path = str(final_path)

            catalog_paths[name] = {"file_path": final_path,
                                   "reader": reader,
                                   "label": label}

        return catalog_paths

    def __init__(self, path_repo, path_catalog, global_dir="globals", globals_file="dev.yaml",
                 base_path_name="base_path", file_type_dict=None):
        """
        Manage the tables at catalog level
        :param path_repo: path to repository
        :rtype: str
        :param path_catalog: path from repository to catalog.yaml (inclusive).
        :rtype: str
        """
        if file_type_dict is None:
            file_type_dict = FILE_TYPE_DICT
        self.file_type_dict = file_type_dict
        self.path_repo = Path(path_repo)
        self.path_catalog = self.path_repo / path_catalog
        self.use_case = self.get_use_case(path_catalog=self.path_catalog)
        self.globals_paths = self.get_globals_paths(path_repo=self.path_repo, global_dir=global_dir,
                                                    globals_file=globals_file, base_path_name=base_path_name,
                                                    use_case=self.use_case)
        self.catalog_paths = self.get_catalog_paths(path_catalog=self.path_catalog, globals_paths=self.globals_paths,
                                                    file_type_dict=self.file_type_dict)

    def get_element(self, elem_name):

        reader = self.catalog_paths[elem_name]["reader"]
        elem = reader(self.catalog_paths[elem_name]["file_path"])

        return elem

    def get_elements_keys(self, labels_subset):
        return [k for k in self.catalog_paths.keys() if self.catalog_paths[k]["label"] in labels_subset]

