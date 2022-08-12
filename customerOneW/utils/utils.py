import pandas as pd
from pathlib import Path
import os
import yaml


class CatalogReader:
    # TODO: extend for all type of files (not only parquet)

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
    def get_catalog_paths(path_catalog, globals_paths):
        catalog_paths = {}
        with open(path_catalog) as file:
            yaml_file = yaml.load(file, Loader=yaml.FullLoader)

        for elem in yaml_file:
            for sub_elem in yaml_file[elem]:
                if sub_elem == "filepath":
                    split_path = yaml_file[elem][sub_elem].split("/")
                    suffix = split_path[0]
                    # TODO: starts with suffix always?
                    for g in globals_paths:
                        if g in suffix:
                            catalog_paths[elem.split("@")[0]] = globals_paths[g] / yaml_file[elem][sub_elem].replace(
                                suffix, "")[1:]

        return catalog_paths

    def __init__(self, path_repo, path_catalog, global_dir="globals", globals_file="dev.yaml",
                 base_path_name="base_path"):
        """
        Manage the tables at catalog level
        :param path_repo: path to repository
        :rtype: str
        :param path_catalog: path from repository to catalog.yaml (inclusive).
        :rtype: str
        """
        self.path_repo = Path(path_repo)
        self.path_catalog = self.path_repo / path_catalog
        self.use_case = self.get_use_case(path_catalog=self.path_catalog)
        self.globals_paths = self.get_globals_paths(path_repo=self.path_repo, global_dir=global_dir,
                                                    globals_file=globals_file, base_path_name=base_path_name,
                                                    use_case=self.use_case)
        self.catalog_paths = self.get_catalog_paths(path_catalog=self.path_catalog, globals_paths=self.globals_paths)

    def get_table(self, table_name):

        table = pd.read_parquet(self.catalog_paths[table_name])

        return table
