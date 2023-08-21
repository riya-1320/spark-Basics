# import
from pyspark.sql import SparkSession
from pyspark.sql.functions import ceil
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SqlApp").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# load data from source and crete dataframe
data_frame = spark.read.csv("/home/xs354-riygup/TASKS/spark-task/data/sparkData.csv", header=True)

# Sql operations to be performed
print("Complete dataframe : ")
data_frame.select("*").show()

# selecting specific columns
print("Specific columns : ")
data_frame.select("RecordNumber", "State", "gender").show()

# based on specific condition
print("specific conditions : gender is male and state is PR")
data_frame.filter((data_frame.gender == "male") & (data_frame.State == "PR")).show()

# Group by column and calculate aggregation
print("Number of hospital in particular state")
dataFrameRound = data_frame.groupBy("State").agg(F.sum("hospitalInCity").alias("numOfHospital"))
dataFrameRound.select(ceil(dataFrameRound['numOfHospital']).alias("numOfHospital")).show()

# Sort rows by a column
print("Sorted gender")
data_frame.sort("gender").show()

# Rename Column
print("Rename RecordNumber as RecNumber")
data_frame.withColumnRenamed("RecordNumber", "RecNumber").show()

# create dataframe
column = StructType([
    StructField("ID", IntegerType(), nullable=False),
    StructField("Name", StringType(), nullable=False),
    StructField("Age", IntegerType(), nullable=False),
    StructField("Occupation", StringType(), nullable=False)
    ])

# enter data in dataframe
data = [(1, "Riya", 21, "Doctor"),
        (2, "Aman", 22, "Cricketer"),
        (3, "Gagan", 23, "Engineer"),
        (4, "Anshu", 32, "Data Analyst")]

# Create the DataFrame
df1 = spark.createDataFrame(data, column)

# Show the contents of the DataFrame
print("Dataframe1: ")
df1.show()

# second dataframe
# column2 = StructType([
#     StructField("ID", IntegerType(), nullable=False),
#     StructField("place", StringType(), nullable=False)
#     ])

data2 = [(2, "Delhi"),
         (3, "Noida"),
         (5, "Kolkata")]
df2 = spark.createDataFrame(data2, ["ID", "Place"])
print("Dataframe2: ")
df2.show()

# Inner join
innerJoin = df1.join(df2, on="ID", how="inner")
print("Output after inner join")
innerJoin.show()

# Left join
leftJoin = df1.join(df2, on="ID", how="left")
print("Output after left join")
leftJoin.show()

# Right join
rightJoin = df1.join(df2, on="ID", how="right")
print("Output after right join")
rightJoin.show()

# Full outer join
fullOuterJoin = df1.join(df2, on='ID', how='full')
print('Output after full join')
fullOuterJoin.show()

# Cross join
crossJoin = df1.crossJoin(df2)
print("Output after cross join")
crossJoin.show()

# stop spark session
spark.stop()
