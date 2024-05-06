from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col

def main():
    ''' 
        We have .csv files with same schema of around 14.8 gb.
        We are create a single df by doing union all, 
        After that cleaning the data and saving the data in different format  '''

    spark = SparkSession.builder.appName("task1").config("spark.executor.instances", "2").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").getOrCreate()

    # Path to the folder containing CSV files
    folder_path = "/home/ubuntu/Downloads/dataset"

    # List all CSV files in the folder
    csv_files = [file for file in os.listdir(
        folder_path) if file.endswith(".csv")]

    # Read each CSV file into a DataFrame
    dfs = []
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        dfs.append(df)

    # Union all DataFrames into one
    final_df = dfs[0]  # Start with the first DataFrame
    for df in dfs[1:]:
        final_df = final_df.union(df)

    final_df = final_df.repartition(2024)
    final_df.show(5,0)
    
    # rtc.show()

    # Show the number of partitions after repartitioning
    # print("Number of partitions after repartitioning:",
    #       final_df.rdd.getNumPartitions())

    # num_columns = len(final_df.columns)
    # print("Size of final DataFrame:", size_in_mb)
    # print("Number of columns in final DataFrame:", num_columns)
    final_df.printSchema()


if __name__ == "__main__":
    main()