from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
# Create SparkSession
spark = SparkSession.builder.appName("task1").getOrCreate()

# Read Excel files into Pandas DataFrames
orders_pd_df = pd.read_excel("/home/ubuntu/Downloads/Data_set1/stg_zidplatform__orders.xlsx")
orders_history_pd_df = pd.read_excel("/home/ubuntu/Downloads/Data_set1/stg_zidplatform__order_histories.xlsx")

# Convert Pandas DataFrames to PySpark DataFrames
orders_df = spark.createDataFrame(orders_pd_df)
orders_df = orders_df.withColumnRenamed("created_at", "created_at_order_df")
orders_df = orders_df.withColumnRenamed("invocation_id", "invocation_id_order_df")
orders_history_df = spark.createDataFrame(orders_history_pd_df)


# Print schema of orders_df
# print("Schema of orders_df:")
orders_df.printSchema()

# print("Schema of orders_history_df:")
orders_history_df.printSchema()

joined_df = orders_df.join(orders_history_df, orders_df.id == orders_history_df.order_id, "left")


result_df = joined_df.select(
    col("order_id").alias("order_id"),
    col("created_at_order_df").alias("order_created_at"),
    lit("1").alias("order_status_id"),
    col("en_status").alias("order_status_status_en"),
    col("created_at").alias("order_previous_status_date"),
    col("order_status_id").alias("previous_order_status_id"),
    lit("New").alias("_order_previous_status_en"),
    col("log_updated_at").alias("log_updated_at"),
    col("invocation_id_order_df").alias("invocation_id"),

)


# result_df.show()

# Define the path where you want to save the CSV file
output_path = "/home/ubuntu/Downloads/Data_set1/result.csv"

# Write the result_df DataFrame to a CSV file
result_df.write.csv(output_path, header=True,mode="overwrite")
