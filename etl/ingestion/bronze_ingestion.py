from pyspark.sql import SparkSession
from config.settings import RAW_DATA_PATH, BRONZE_TABLE

# Initialize Spark Session
spark = SparkSession.builder.appName("Bronze Ingestion").getOrCreate()

def load_raw_data():
    """Load raw CSV data from local storage (DBFS)"""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_DATA_PATH)
    return df

def save_to_bronze(df):
    """Save raw data as Bronze Delta Table"""
    df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)

if __name__ == "__main__":
    df_bronze = load_raw_data()
    save_to_bronze(df_bronze)