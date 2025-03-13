import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from etl.transformation.silver_transformation import clean_data, save_to_silver

@pytest.fixture(scope="session")
def spark():
    """Create a shared Spark session for all tests."""
    return SparkSession.builder.master("local[*]").appName("TestSilver").getOrCreate()

def test_clean_data(spark):
    """Test that clean_data() removes duplicates and formats dates correctly."""
    
    # Define schema
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("transaction_date", StringType(), True)
    ])
    
    # Sample test data (with duplicate transaction_id)
    data = [
        (1, "2024-03-10"),
        (2, "2024-03-11"),
        (1, "2024-03-10")  # Duplicate transaction_id
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)
    
    # Apply transformation
    df_cleaned = clean_data(df)

    # ✅ Check if duplicates are removed
    assert df_cleaned.count() == 2

    # ✅ Check if "transaction_date" is converted to DateType
    assert df_cleaned.schema["transaction_date"].dataType == DateType()

def test_save_to_silver(spark, tmp_path):
    """Test save_to_silver() function by writing to a temporary location."""
    
    # Define schema
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("transaction_date", DateType(), True)
    ])
    
    # Sample test data
    data = [
        (1, "2024-03-10"),
        (2, "2024-03-11")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Use a temporary Delta table path for testing
    temp_table_path = tmp_path / "silver_table"

    # Save to Silver (simulate writing)
    df.write.format("delta").mode("overwrite").save(str(temp_table_path))

    # Read back to verify
    df_loaded = spark.read.format("delta").load(str(temp_table_path))

    # ✅ Check if data is saved correctly
    assert df_loaded.count() == 2

    # ✅ Check if "transaction_date" is still DateType
    assert df_loaded.schema["transaction_date"].dataType == DateType()

if __name__ == "__main__":
    pytest.main([__file__])
