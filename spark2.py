from pyspark.sql import SparkSession
import time

# Create a SparkSession
spark = SparkSession.builder.appName("Read CSV").getOrCreate()

# Get the SparkContext
sc = spark.sparkContext

# Define the path to the directory containing CSV files
csv_directory_path = "/home/ubuntu/Downloads/dataset/"

# Start measuring execution time
start_time = time.time()


csv_rdds = sc.textFile(csv_directory_path + "*.csv")

# csv_rdds = csv_rdds.repartition(800)
# num_partitions = csv_rdds.getNumPartitions()

# print(num_partitions)

# Function to split each row into columns and filter out rows with null values
def filter_null_rows(row):
    columns = row.split(",")
    if any(column is None or column == "" for column in columns):
        return False
    return True

# Remove rows with null values
filtered_rdd = csv_rdds.filter(filter_null_rows)

# Count the number of rows after removing null values
total_rows_rdd = filtered_rdd.count()

# Stop measuring execution time
end_time = time.time()

# Print the total number of rows
print("Total Number of Rows (RDD after removing null values):", total_rows_rdd)

# Calculate and print the execution time
execution_time = end_time - start_time
print("Execution Time:", execution_time, "seconds")

# Stop the SparkSession
spark.stop()
