import tempfile

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    # Fresh warehouse dir per session: leftover table paths from a previous
    # run would make saveAsTable fail with "path already exists".
    s = (SparkSession.builder.master("local[2]")
         .appName("sas2dbx-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="sas2dbx-wh-"))
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()
