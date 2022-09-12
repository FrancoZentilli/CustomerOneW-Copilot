import pandas as pd
from utils.utils import pickle_reader

FILE_TYPE_DICT = {"pandas.ParquetDataSet": {"reader": pd.read_parquet,
                                            "label": "table"},
                  "spark.SparkDataSet": {"reader": pd.read_parquet,
                                         "label": "table"},
                  "pickle.PickleDataSet": {"reader": pickle_reader,
                                           "label": "pickle"}}
