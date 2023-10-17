import pytest
import os
import sys
from pyspark.sql import SparkSession
from project import get_distinct_count, sum_of_column, rename_column, add_literal_string_column


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()
    return spark


@pytest.fixture
def example_df(spark):
    data = [("Finance", 10),
            ("Marketing", 20),
            ("Sales", 30),
            ("Sales", 30),
            ("IT", 40),
            ("IT", 10)
            ]
    cols = ["dept_name", "dept_id"]

    return spark.createDataFrame(data, cols)


def test_get_distinct_count(spark, example_df):
    test_count = get_distinct_count(example_df)
    assert test_count == 5


def test_sum_of_column(spark, example_df):
    assert sum_of_column(example_df, 'dept_id') == 140


def test_rename_column(spark, example_df):
    renamed_df = rename_column(example_df, "dept_name", "dept_title")
    assert renamed_df.columns == ["dept_title", "dept_id"]


def test_add_literal_string_column_string(spark, example_df):
    test_df = add_literal_string_column(example_df, 'string_works', 'hello')

    data = [
        ("Finance", 10, 'hello'),
        ("Marketing", 20, 'hello'),
        ("Sales", 30, 'hello'),
        ("Sales", 30, 'hello'),
        ("IT", 40, 'hello'),
        ("IT", 10, 'hello')
    ]
    cols = ["dept_name", "dept_id", "string_works"]

    expected_df = spark.createDataFrame(data, cols)

    assert test_df.columns == expected_df.columns
    assert dict(test_df.dtypes)["string_works"] == 'string'
