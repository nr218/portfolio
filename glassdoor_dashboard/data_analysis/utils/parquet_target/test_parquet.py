import os
from tempfile import TemporaryDirectory
from unittest import TestCase

from dask import dataframe as dd
from pandas import DataFrame

from data_analysis.utils.parquet_target import ParquetTarget


class ParquetTargetTest(TestCase):
    def setUp(self):
        test_df = DataFrame(
            {"test_1": [1, 2, 3, 4], "test_2": [5, 6, 7, 8], "test_3": [9, 10, 11, 12],}
        )
        self.ddf = dd.from_pandas(test_df, npartitions=3)

    def test_write_parquet(self):
        dask_df = self.ddf
        with TemporaryDirectory() as tmp:
            t = ParquetTarget(os.path.join(tmp + "/"))
            t.write_dask(collection=dask_df, compute=True)
            assert os.path.exists(os.path.join(tmp, "_SUCCESS"))
            for n in range(2):
                assert os.path.exists(os.path.join(tmp, "part.{}.parquet".format(n)))
