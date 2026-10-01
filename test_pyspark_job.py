import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data
@pytest.fixture(scope="module")
def spark_local():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-customer-orders")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
def test_clean_data(spark_local):
    data = [
        ("Alice", 100.0),   
        ("Bob", -50.0),    
        ("Charlie", 0.0),   
        (None, 200.0),   
    ]
    df = spark_local.createDataFrame(
        data,
        ["name", "amount"]
    )
    transformed_df = clean_data(df)
    results = transformed_df.collect()
    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    assert results[0]["amount_with_tax"] == pytest.approx(120.0)











    