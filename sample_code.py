from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("ThirdTransaction").getOrCreate()

# Sample data
data = [
    (111, 100.50, "01/08/2022 12:00:00"),
    (111, 55.00, "01/10/2022 12:00:00"),
    (121, 36.00, "01/18/2022 12:00:00"),
    (145, 24.99, "01/26/2022 12:00:00"),
    (111, 89.60, "02/05/2022 12:00:00")
]

# Create DataFrame
df = spark.createDataFrame(data, ["user_id", "spend", "transaction_date"])

# Convert transaction_date to Timestamp
df = df.withColumn("transaction_date", col("transaction_date").cast("timestamp"))

# Define window to partition by user_id and order by transaction_date
window_spec = Window.partitionBy("user_id").orderBy("transaction_date")

# Add row number
df_with_ranks = df.withColumn("rn", row_number().over(window_spec))

# Filter for 3rd transaction
third_transaction_df = df_with_ranks.filter(col("rn") == 3).select("user_id", "spend", "transaction_date")

# Show result
third_transaction_df.show()