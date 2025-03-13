from pyspark.sql.functions import sum, count
from pyspark.sql import SparkSession
from config.settings import SILVER_TABLE, GOLD_TABLE

# initialize Spark Session
spark = SparkSession.builder.appName("GoldAggregation").getOrCreate()

def aggregate_data(df):
    """Aggregate total amount spend per customer"""
    return df.groupBy("customer_id").agg(
        sum("amount").alias("total_spent"),
        count("transaction_id").alias("total_transactions")
    )

def save_to_gold(df):
    """Save aggregated data as Gold Delta Table"""
    df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)

if __name__ == "__main__":
    df_gold = spark.read.table(SILVER_TABLE)
    df_gold = aggregate_data(df_gold)
    save_to_gold(df_gold)