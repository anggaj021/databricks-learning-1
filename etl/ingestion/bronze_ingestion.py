from pyspark.sql import SparkSession
from config.settings import RAW_DATA_PATH, CATALOG_NAME, SCHEMA_NAME, BRONZE_TABLE

# Initialize Spark Session
spark = SparkSession.builder.appName("Bronze Ingestion").getOrCreate()

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

def load_raw_data():
    """Load raw CSV data from local storage (DBFS)"""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_DATA_PATH)
    return df

def save_to_bronze(df):
    """Save raw data as Bronze Delta Table in the specified catalog and schema"""
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{BRONZE_TABLE}"
    
    try:
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        print(f"✅ Successfully saved data to {full_table_name}")
    except Exception as e:
        print(f"❌ Error saving data: {str(e)}")

if __name__ == "__main__":
    df_bronze = load_raw_data()
    save_to_bronze(df_bronze)
