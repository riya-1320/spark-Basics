# Load Data from CSV,  JSON, Parquet with InferSchema and perform basic spark SQL operations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import ceil

spark = SparkSession.builder.appName("infer-schemaApp").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load data from CSV with inferred schema
df_csv = spark.read.option("inferSchema", "true").csv("/home/xs354-riygup/TASKS/spark-task/data/sparkData.csv", header=True)

# Load data from JSON with inferred schema
df_json = spark.read.option("inferSchema", "true").json("/home/xs354-riygup/TASKS/spark-task/data/sparkData.json")

# Load data from Parquet with inferred schema
# df_parquet_write = df_json.write.parquet("/home/xs354-riygup/PycharmProjects/spark-task/people.parquet")
df_parquet = spark.read.option("inferSchema", "true").parquet("/home/xs354-riygup/TASKS/spark-task/data/people.parquet")

# Perform Spark SQL operations on CSV DataFrame

# Select columns
print("CSV data:")
df_csv.select("*").show()

# Filter data
print("filtered data: show only healthy records: ")
df_csv.filter(df_csv.healthStatus == "healthy").show()

# Aggregations
print("aggregate function : print maximum number of hospital in a state")
df_csv.groupBy("state").agg(F.max("hospitalInCIty")).alias("MaxHospital").show()

# Perform Spark SQL operations on JSON DataFrame
# Select columns
print("JSON data:")
df_json.select("*").show()

# Filter data
print("filtered data: show only FT WORTH city records: ")
df_json.filter(df_json.City == "FT WORTH").show()

# Aggregations
print("aggregate function : print maximum people in a state")
df_json.groupBy("State").agg(F.max("People").alias("MaxPeople")).show()

# Perform Spark SQL operations on Parquet DataFrame
# Select columns
print("parquet data:")
df_parquet.select("*").show()

# Filter data
print("filtered data: show only PO BOX records: ")
df_parquet.filter(df_parquet.ZipCodeType == "PO BOX").show()

# Aggregations
print("aggregate function : print average number of people in a state")
dfParquetRound = df_parquet.groupBy("State").agg(F.avg("People").alias("AvgPeople"))
dfParquetRound.select(ceil(dfParquetRound['AvgPeople'].alias('AvgPeople'))).show()

spark.stop()
