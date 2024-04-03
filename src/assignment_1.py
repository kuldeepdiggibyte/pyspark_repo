from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import countDistinct
spark = SparkSession.builder.appName("assignment_1").getOrCreate()
# 1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data
purchase_data = [
    (1, "iphone13"),

    (1, "dell i5 core"),

    (2, "iphone13"),

    (2, "dell i5 core"),

    (3, "iphone13"),

    (3, "dell i5 core"),

    (1, "dell i3 core"),

    (1, "hp i5 core"),

    (1, "iphone14"),

    (3, "iphone14"),

    (4, "iphone13")
]
purchase_data_schema = ["customer","product_model"]
purchase_data_df = spark.createDataFrame(data=purchase_data,schema=purchase_data_schema)

product_data = [
    ("iphone13",),

    ("dell i5 core",),

    ("dell i3 core",),

    ("hp i5 core",),

    ("iphone14",)
]

product_data_schema = ["product_model"]


product_data_df = spark.createDataFrame(data=product_data,schema=product_data_schema)
purchase_data_df.show()
product_data_df.show()
#2.Find the customers who have bought only iphone13
upgrade_customers = purchase_data_df.filter(F.col("product_model") == "iphone13")\
    .select("customer").show()

#3.Find customers who upgraded from product iphone13 to product iphone14

upgrade_customers = purchase_data_df.alias("p1").join(
    purchase_data_df.alias("p2"),
    (F.col("p1.customer") == F.col("p2.customer")) &
    (F.col("p1.product_model") == "iphone13") &
    (F.col("p2.product_model") == "iphone14"),
    "inner"
).select(F.col("p1.customer")).distinct()

upgrade_customers.show()

#4.Find customers who have bought all models in the new Product Data

customer_models_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("model_count"))
all_models = customer_models_count.filter(customer_models_count.model_count == product_data_df.count()).select("customer")
all_models.show()


