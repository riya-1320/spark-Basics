# persist data as CSV, JSON or Parquet using repartition by key or coalesce

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("sparkPersistence").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Load data from CSV, JSON, Parquet
dfCsv = spark.read.csv("/home/xs354-riygup/TASKS/spark-task/data/sparkData.csv", header=True)
dfJson = spark.read.json("/home/xs354-riygup/TASKS/spark-task/data/sparkData.json")
# df_parquet_write = dfJson.write.parquet("/home/xs354-riygup/TASKS/spark-task/data/people.parquet")
dfParquet = spark.read.parquet("/home/xs354-riygup/TASKS/spark-task/data/people.parquet")

# persisting CSV, JSON, Parquet file
dfCsv.persist(StorageLevel.MEMORY_AND_DISK)
dfJson.persist(StorageLevel.MEMORY_AND_DISK)
dfParquet.persist(StorageLevel.MEMORY_AND_DISK)

# repartitioning csv
print('\nInitial Partition:')
print(dfCsv.rdd.getNumPartitions())
print('\nPartition after repartition():')
repartitioned_df_csv = dfCsv.repartition(2, "gender")
repartitioned_df_csv.withColumn("PartitionedID", spark_partition_id()).groupBy("PartitionedID").count().show()

# by coalesce
df_coalesce_csv = dfCsv.repartition(5)
print('\nPartition after Coalesce():')
coalescecsv_by_three = df_coalesce_csv.coalesce(3)
coalescecsv_by_three.withColumn("PartitionedID", spark_partition_id()).groupBy("partitionedID").count().show()
dfCsv.unpersist()

# repartitioning json
print('\nInitial Partition:')
print(dfJson.rdd.getNumPartitions())
print('\nPartition after repartition():')
repartitioned_df_json = dfJson.repartition(3, "ZipCodeType")
repartitioned_df_json.withColumn("PartitionedID", spark_partition_id()).groupBy("PartitionedID").count().show()
repartitioned_df_json.show()

# by coalesce
df_coalesce_json = dfJson.repartition(5)
print('\nPartition after Coalesce():')
coalescejson_by_three = df_coalesce_json.coalesce(3)
coalescejson_by_three.withColumn("PartitionedID", spark_partition_id()).groupBy("partitionedID").count().show()
dfJson.unpersist()

# repartitioning parquet
print('\nInitial Partition:')
print(dfParquet.rdd.getNumPartitions())
dfParquet.show()
print('\nPartition after repartition():')
repartitioned_df_parquet = dfParquet.repartition("Zaxis")
repartitioned_df_parquet.show()

# by coalesce
df_coalesce_parquet = dfParquet.repartition(5)
print('\nPartition after Coalesce():')
df_coalesceparquet_by_three = df_coalesce_parquet.coalesce(3)
df_coalesceparquet_by_three.withColumn("PartitionedID", spark_partition_id()).groupBy("partitionedID").count().show()
dfParquet.unpersist()

spark.stop()
