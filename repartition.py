from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("repartitionApp").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("/home/xs354-riygup/TASKS/spark-task/data/sparkData.csv", header=True)

# print number of partition
print("Initially partition in dataframe")
print(df.rdd.getNumPartitions())

# performing repartitioning
partitioned_df = df.repartition(4)

print("After Applying repartition() method")
print(partitioned_df.rdd.getNumPartitions())
print("Repartitioned memory distribution")
partitioned_df.withColumn("partitionedID", spark_partition_id()).groupBy("partitionedID").count().show()
print("Repartitioned memory distribution with gender column")
partitioned_df_2 = df.repartition(2, "gender")
partitioned_df_2.withColumn("partitionedID", spark_partition_id()).groupBy("partitionedID").count().show()


# coalesce
print("*************COALESCE**************")
print("Initial dataframe partition")
print(df.rdd.getNumPartitions())
print("After Repartition()")
coalesce_df = df.repartition(5)
print(coalesce_df.rdd.getNumPartitions())
coalesce_df.withColumn("PartitionedID", spark_partition_id()).groupBy("PartitionedID").count().show()
print("After coalesce(2): ")
coalesce_df.bytwo = coalesce_df.coalesce(2)
coalesce_df.bytwo.withColumn("Partitioned_id", spark_partition_id()).groupBy("Partitioned_id").count().show()

spark.stop()