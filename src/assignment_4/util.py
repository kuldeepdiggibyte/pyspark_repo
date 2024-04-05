from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode, explode_outer, posexplode_outer, current_date, year, month, day

spark = SparkSession

def read_json_file(spark, file_path, schema):

    json_data_frame = spark.read.json(file_path, multiLine=True, schema=schema)
    return json_data_frame

def flatten_dataframe(df):

    flatten_df = df.select("*", "properties.name", "properties.storeSize").drop("properties") \
        .select("*", explode("employees").alias("new_employees")).drop("employees") \
        .select("*", "new_employees.empId", "new_employees.empName").drop("new_employees")
    return flatten_df

def filter_dataframe(df, condition):

    filtered_df = df.filter(condition)
    return filtered_df

def convert_to_snake_case(data_frame):

    for column in data_frame.columns:
        snake_case_col = ''.join(['_' + char.lower() if char.isupper() else char for char in column]).lstrip('_')
        data_frame = data_frame.withColumnRenamed(column, snake_case_col)
    return data_frame

def add_load_date_column(df):

    load_date_df = df.withColumn("load_date", current_date())
    return load_date_df

def add_date_columns(df, date_column="load_date"):

    date_df = df.withColumn("year", year(df[date_column])) \
                .withColumn("month", month(df[date_column])) \
                .withColumn("day", day(df[date_column]))
    return date_df

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()

    # Define schema for JSON data
    json_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("properties", StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ]), True),
        StructField("employees", ArrayType(
            StructType([
                StructField("empId", LongType(), True),
                StructField("empName", StringType(), True)
            ])
        ), True)
    ])

    # Path to JSON file
    json_file_path = "../resource/nested_json_file.json"

    # Read JSON file
    json_data_frame = read_json_file(spark, json_file_path, json_schema)

    # 1. Flatten the DataFrame
    flatten_df = flatten_dataframe(json_data_frame)

    # 2. Filter DataFrame
    filtered_df = filter_dataframe(flatten_df, flatten_df["empId"] == 1001)

    # 3. Convert column names to snake_case
    snake_case_df = convert_to_snake_case(flatten_df)

    # 4. Add a new column named 'load_date' with the current date
    load_date_df = add_load_date_column(snake_case_df)

    # 5. Add separate year, month, and day columns from the 'load_date' column
    year_month_day_df = add_date_columns(load_date_df)

    # Show DataFrame
    year_month_day_df.show()
