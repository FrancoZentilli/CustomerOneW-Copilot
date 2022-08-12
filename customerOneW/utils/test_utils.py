import unittest

import pandas as pd

from utils import CatalogReader
from pathlib import Path
from configs import PATH_MAIN
import os


class TestUtils(unittest.TestCase):

    def test_CatalogReader(self):
        path_catalog = "realm/rb/conf/base/uc/churn/model/catalog.yaml"
        path_repo = PATH_MAIN.parent / "customerone-w"
        use_case = "rb"
        catalog_reader = CatalogReader(path_catalog=path_catalog, path_repo=path_repo)
        self.assertEqual(catalog_reader.path_catalog, path_repo / path_catalog)
        self.assertEqual(catalog_reader.path_repo, Path(path_repo))
        self.assertEqual(catalog_reader.get_use_case(path_catalog=Path(path_catalog)), use_case)

        globals_paths = catalog_reader.get_globals_paths(path_repo=Path(path_repo), use_case=use_case)
        for k in globals_paths:
            # print(globals_paths[k])
            self.assertEqual(os.path.isdir(globals_paths[k]), True)

        catalog_paths = catalog_reader.get_catalog_paths(path_catalog=catalog_reader.path_catalog,
                                                         globals_paths=globals_paths)
        for k in catalog_paths:
            # print(catalog_paths[k])
            self.assertEqual(os.path.isdir(catalog_paths[k]) or os.path.isfile(catalog_paths[k]), True)

        self.assertEqual(catalog_reader.globals_paths, globals_paths)
        self.assertEqual(catalog_reader.catalog_paths, catalog_paths)

        table = catalog_reader.get_table("filtered_model_input_data")
        self.assertEqual(table.shape, (4015,194))


if __name__ == '__main__':
    unittest.main()
