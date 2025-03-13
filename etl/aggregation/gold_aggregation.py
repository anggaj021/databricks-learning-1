from pyspark.sql.functions import sum, count
from pyspark.sql import SparkSession
from config.settings import CATALOG_NAME, SCHEMA_NAME, SILVER_TABLE, GOLD_TABLE

# Initialize Spark Session
spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# Fully qualified table names
SILVER_TABLE_FQN = f"{CATALOG_NAME}.{SCHEMA_NAME}.{SILVER_TABLE}"
GOLD_TABLE_FQN = f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE}"

def aggregate_data(df):
    """Aggregate total amount spent per customer"""
    return df.groupBy("customer_id").agg(
        sum("amount").alias("total_spent"),
        count("transaction_id").alias("total_transactions")
    )

def save_to_gold(df):
    """Save aggregated data as Gold Delta Table"""
    try:
        df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_FQN)
        print(f"✅ Successfully saved data to {GOLD_TABLE_FQN}")
    except Exception as e:
        print(f"❌ Error saving data: {str(e)}")

if __name__ == "__main__":
    try:
        df_gold = spark.read.table(SILVER_TABLE_FQN)
        df_gold = aggregate_data(df_gold)
        save_to_gold(df_gold)
    except Exception as e:
        print(f"❌ Error reading Silver table: {str(e)}")
