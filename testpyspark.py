from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, trim
 
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Transaction Data") \
    .getOrCreate()
 
# Define the schema for the DataFrame
schema = StructType([
    StructField("TransactionDate", StringType(), True),
    StructField("AccountNumber", IntegerType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("Amount", IntegerType(), True)
])
 
# Create data in list of tuples
data = [
    ("2023-01-01", 100, "Credit", 1000),
    ("2023-01-02", 100, "Credit", 1500),
    ("2023-01-03", 100, "Debit", 1000),
    ("2023-01-02", 200, "Credit", 3500),
    ("2023-01-03", 200, "Debit", 2000),
    ("2023-01-04", 200, "Credit", 3500),
    ("2023-01-13", 300, "Credit", 4000),
    ("2023-01-14", 300, "Debit", 4500),
    ("2023-01-15", 300, "Credit", 1500)
]
 
# Create DataFrame
df = spark.createDataFrame(data, schema)
 
# Trim any extra spaces in the 'TransactionType' column
df = df.withColumn("TransactionType", trim(col("TransactionType")))
 
# Show DataFrame
df.show()
 
 
from pyspark.sql import Window
from pyspark.sql.functions import col, when, sum as _sum
 
# Sort the DataFrame 
window_spec = Window.partitionBy("AccountNumber").orderBy("TransactionDate")
 
# added a new temprary column 'TransactionAmount' in dataframe to make debits as negative and credits as positive
df = df.withColumn("TransactionAmount", 
                   when(col("TransactionType") == "Credit", col("Amount"))
                   .otherwise(-col("Amount")))
#df.show()
# Calculate the cumulative balance for each account after each transaction
df = df.withColumn("CurrentBalance", _sum("TransactionAmount").over(window_spec))
 
#df.show()
 
#df.select([col for col in df.columns if col != "TransactionAmount"]).show()
df.select([col for col in df.columns if col != "TransactionAmount"]).display()