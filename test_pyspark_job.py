import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark_job import clean_data

SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_pyspark_job")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Bob", 50.0)], SCHEMA)
    result = clean_data(df)
    names = sorted(row["name"] for row in result.collect())
    assert names == ["Alice", "Bob"]


def test_amount_less_than_or_equal_zero_removed(spark):
    df = spark.createDataFrame(
        [("Alice", 100.0), ("Bob", 0.0), ("Carol", -5.0)], SCHEMA
    )
    result = clean_data(df)
    names = [row["name"] for row in result.collect()]
    assert names == ["Alice"]


def test_null_names_removed(spark):
    df = spark.createDataFrame([("Alice", 100.0), (None, 50.0)], SCHEMA)
    result = clean_data(df)
    names = [row["name"] for row in result.collect()]
    assert names == ["Alice"]


def test_amount_with_tax_calculated_correctly(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Bob", 50.0)], SCHEMA)
    result = clean_data(df)
    taxed = {row["name"]: row["amount_with_tax"] for row in result.collect()}
    assert taxed["Alice"] == pytest.approx(120.0)
    assert taxed["Bob"] == pytest.approx(60.0)
