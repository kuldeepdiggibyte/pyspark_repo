<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PySpark Assignment</title>
</head>
<body>
  <h1>PySpark Assignment</h1>
  <h2>Question 1</h2>
  <h3>Create Dataframes:</h3>
  <p>Develop dataframes for both purchase data and product data as specified in the queries.</p>
  <h3>Identify Customers Exclusively Purchasing iPhone13:</h3>
  <p>Find customers who have solely purchased the "iPhone13" product model.</p>
  <h3>Determine Customers Upgrading from iPhone13 to iPhone14:</h3>
  <p>Identify customers who have transitioned from the "iPhone13" product model to the "iPhone14" product model.</p>
  <h3>Find Customers with Purchases for All Models in New Product Data:</h3>
  <p>Locate customers who have made purchases for all product models listed in the new product data.</p>
</body>
</html>


<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PySpark Assignment</title>
</head>
<body>
  <h1>PySpark Assignment</h1>
  <h2>Question 2</h2>
  <h3>Initialize SparkSession:</h3>
  <p>Set up a SparkSession for PySpark utilization.</p>
  <h3>Method 1: DataFrame Creation via createDataFrame Function:</h3>
  <p>Generate a DataFrame from provided data using the createDataFrame function.</p>
  <h3>Method 2: CSV File Reading:</h3>
  <p>Read credit card data from a CSV file using the credit_cards.csv function.</p>
  <h3>Method 3: JSON File Reading:</h3>
  <p>Read credit card data from a JSON file using the credit_cards.json function.</p>
  <h3>Partitioning Operations:</h3>
  <ul>
    <li>Determine the total number of partitions in the DataFrame using getNumPartitions.</li>
    <li>Increase the partition size by 5 partitions using repartition.</li>
    <li>Restore the partition size to its original state.</li>
  </ul>
  <h3>Masking Credit Card Numbers:</h3>
  <ul>
    <li>Define a UDF named masked_card_number to mask the credit card numbers, revealing only the last 4 digits.</li>
    <li>Apply the UDF to the DataFrame column containing credit card numbers.</li>
    <li>Exhibit the DataFrame with masked credit card numbers.</li>
  </ul>
</body>
</html>


<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PySpark Assignment</title>
</head>
<body>
  <h1>PySpark Assignment</h1>
  <h2>Question 3</h2>
  <h3>Column Names Modification:</h3>
  <p>A custom function has dynamically modified the DataFrame's column names to 'log_id', 'user_id', 'user_activity', and 'time_stamp'.</p>
  <p>The function iteratively renames the existing column names based on the specified new column names.</p>
  <h3>Action Calculation Query:</h3>
  <p>A query has been devised to calculate the count of actions performed by each user within the last 7 days.</p>
  <p>The DataFrame is filtered to include only data from the past 7 days, then grouped by user_id to count the actions.</p>
  <h3>Timestamp Conversion:</h3>
  <p>The timestamp column has been converted into a new column named 'login_date' with the format YYYY-MM-DD and a Date data type.</p>
  <p>This conversion enables easier handling and analysis of login date information.</p>
</body>
</html>


