# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

# Sample dataset
Dataset = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

# Defining schema for the dataset
Schema = StructType([
    StructField("card_number", StringType())
])

# Creating Spark session
spark = SparkSession.builder.appName('credit_card').getOrCreate()

# Creating DataFrame from sample dataset
credit_card_df = spark.createDataFrame(data=Dataset, schema=Schema)

# Displaying the DataFrame
credit_card_df.show()

# Reading a CSV file with inferred schema
credit_card_df1 = spark.read.csv("../resource/card_number.csv", header=True, inferSchema=True)
credit_card_df1.show()
credit_card_df1.printSchema()

# Reading a CSV file with a custom schema
credit_card_df_schema = spark.read.format("csv").option("header", "true").schema(Schema).load("../resource/card_number.csv")
credit_card_df_schema.show()
credit_card_df_schema.printSchema()

# Printing number of partitions
number_of_partition = credit_card_df_schema.rdd.getNumPartitions()
print("Number of partitions before repartitioning:", number_of_partition)

# Increasing the number of partitions by 5
increase_partition = credit_card_df_schema.rdd.repartition(number_of_partition + 5)
increase_partition1 = increase_partition.getNumPartitions()
print("Increased partition count: ", increase_partition1)

# Decreasing the number of partitions back to its original size
decrease_partition = credit_card_df_schema.rdd.repartition(increase_partition1 - 5)
decrease_partition1 = decrease_partition.getNumPartitions()
print("Back to original partition count: ", decrease_partition1)

# Defining a function to mask card numbers
def masked_card_number(card_number):
    masked_characters = len(card_number) - 4
    masked_number = ('*' * masked_characters) + card_number[-4:]
    return masked_number

# Registering the function as a user-defined function (UDF)
credit_card_udf = udf(masked_card_number, StringType())

# Adding a masked card number column using the UDF
credit_card_df_udf = credit_card_df_schema.withColumn("masked_card_number", credit_card_udf(credit_card_df_schema['card_number']))
credit_card_df_udf.show()
