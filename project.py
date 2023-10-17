import os
import sys

from pyspark.sql import SparkSession

from pyspark.sql import DataFrame

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# CODE BELOW

import pyspark.sql.functions as SF

def get_distinct_count(df: DataFrame) -> int:
    return df.distinct().count()


def sum_of_column(df: DataFrame, column_name: str) -> int:
    sum_col = df.select(SF.sum(SF.col(column_name)).alias("sum")).collect()[0].sum
    return sum_col


def rename_column(df: DataFrame, old_column_name: str, new_column_name: str) -> DataFrame:
    return df.withColumnRenamed(
        old_column_name,
        new_column_name
    )


def add_literal_string_column(df: DataFrame, new_column_name: str, literal_string: str) -> DataFrame:
    return df.withColumn(new_column_name, SF.lit(literal_string))
