#Repartition() vs colsec method()


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rep_and_collsec").getOrCreate()

def rdd_parallelize_example():
    '''We can create parallelism at the time of the creation of an "RDD" using parallelize(), 
        textFile() and wholeTextFiles() methods.'''

    rdd = spark.sparkContext.parallelize(range(0,20))
    num_partitions = rdd.getNumPartitions()
    print("Number of partitions:", num_partitions)# 4

    global rdd1
    rdd1 = spark.sparkContext.parallelize(range(0,20),6)

    num_partitions = rdd1.getNumPartitions()
    print("Number of partitions:", num_partitions) # 6
    # rdd1.saveAsTextFile("/home/ubuntu/Downloads/tmp/repartitioned")

def rdd_repartitioned_example():
    '''Spark RDD repartition() method is used to increase or decrease the partitions.
       Repatition is costly because it shuffle all the partition'''
    global rdd2
    rdd2 = rdd1.repartition(4)
    num_partitions = rdd2.getNumPartitions()
    print("Number of partitions:", num_partitions)# 4

def rdd_coalesce_example():
    '''Spark RDD coalesce() is used only to reduce the number of partitions.
       this method is more optimized then repartition as shuffling between the partitions is less as compare to repartition method.'''
    rdd3 = rdd1.coalesce(5)
    num_partitions = rdd3.getNumPartitions()

# rdd_parallelize_example()
# rdd_repartitioned_example()
# rdd_coalesce_example()


def df_parallelize_example():
    ''' For df or dataset we don't have parallelize method like in rdd.
        In data frames by default use this method to decide no of partitions required.'''
    df = spark.range(0,20)
    print(df.rdd.getNumPartitions())# 4


def df_repartitioned_example():
    ''' Same as Rdd it is used for increase or decrease partitions.
        if we increase just 1 partition data movement will happen in all partitions'''
    df = spark.range(0,20)
    new_df = df.repartition(7)

    print(df.rdd.getNumPartitions())# 4
    print(new_df.rdd.getNumPartitions())# 7

def df_coalesce_exampl():
    ''' Same as rdd, It is used for decrease the partition only. data movement is very less as compare to repartitioned method'''
    df = spark.range(0,20)
    new_df = df.coalesce(3)

    print(df.rdd.getNumPartitions())# 4
    print(new_df.rdd.getNumPartitions())# 3
# df_parallelize_example()
# df_repartitioned_example()
df_coalesce_exampl()