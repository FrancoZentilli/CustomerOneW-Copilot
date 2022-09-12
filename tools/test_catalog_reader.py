import unittest
from tools.catalog_reader import CatalogReader
from pathlib import Path
from configs import PATH_MAIN
import os


class TestCatalogReader(unittest.TestCase):
    path_catalog = "realm/rb/conf/base/uc/churn/model/catalog.yaml"
    path_globals = None
    path_repo = PATH_MAIN.parent / "customerone-w"
    use_case = "rb"
    catalog_reader = CatalogReader(path_catalog=path_catalog,
                                   path_repo=path_repo,
                                   path_globals=path_globals)

    def test_CatalogReader(self):
        self.assertEqual(self.catalog_reader.path_catalog,
                         self.path_repo / self.path_catalog)
        self.assertEqual(self.catalog_reader.path_repo, Path(self.path_repo))

    def test_get_globals_paths(self):
        globals_paths = self.catalog_reader.get_globals_paths(
            path_repo=Path(self.path_repo), use_case=self.use_case)
        for k in globals_paths:
            self.assertEqual(os.path.isdir(globals_paths[k]), True)

    def test_get_use_case(self):
        self.assertEqual(
            self.catalog_reader.get_use_case(path_catalog=Path(self.path_catalog)),
            self.use_case)

    def test_get_catalog_paths(self):
        globals_paths = self.catalog_reader.get_globals_paths(
            path_repo=Path(self.path_repo),
            use_case=self.use_case)
        catalog_paths = self.catalog_reader.get_catalog_paths(
            path_catalog=self.catalog_reader.path_catalog,
            globals_paths=globals_paths,
            file_type_dict=self.catalog_reader.file_type_dict)
        for k in catalog_paths:
            self.assertEqual(
                os.path.isdir(catalog_paths[k]["file_path"]) or os.path.isfile(
                    catalog_paths[k]["file_path"]), True)

        self.assertEqual(self.catalog_reader.globals_paths, globals_paths)
        self.assertEqual(self.catalog_reader.catalog_paths, catalog_paths)

    def test_get_element(self):
        table = self.catalog_reader.get_element("filtered_model_input_data")
        self.assertEqual(table.shape[0] > 0, True)
        pickle = self.catalog_reader.get_element("selected_features_list")
        self.assertEqual(len(pickle)>0, True)

    def test_get_elements_keys(self):
        labels = [None, "table", ["table", "pickle"]]
        for ls in labels:
            elements_keys = self.catalog_reader.get_elements_keys(ls)
            self.assertEqual(len(elements_keys) > 0, True)


if __name__ == '__main__':
    unittest.main()
