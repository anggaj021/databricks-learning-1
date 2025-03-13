import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from etl.transformation.silver_transformation import clean_data

spark = SparkSession.builder.appName("TestSilver").getOrCreate()

def test_clean_data():
    data = [(1, "2024-03-10"), (2, "2024-03-11")]
    df = spark.createDataFrame(data, ["transaction_id", "transaction_date"])
    df_cleaned = clean_data(df)

    assert df_cleaned.select("transaction_date").dtypes[0][1] == "date"

test_clean_data()