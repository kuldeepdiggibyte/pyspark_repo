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



<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PySpark Assignment</title>
</head>
<body>
  <h1>PySpark Assignment</h1>
  <h2>Question 4</h2>
  <h3>Read JSON File:</h3>
  <p>A dynamic function has been used to read the JSON file, allowing flexibility in handling various JSON file structures.</p>
  <p>The DataFrame schema is printed and displayed to understand the data structure.</p>
  <h3>Flatten DataFrame:</h3>
  <p>The DataFrame has been flattened into a customized schema by utilizing the explode function on nested arrays.</p>
  <p>The resulting DataFrame contains columns for each nested array element, offering a structured view of the data.</p>
  <h3>Record Count Analysis:</h3>
  <p>An analysis of the record count before and after flattening the DataFrame has been conducted to identify any disparities.</p>
  <p>This analysis helps understand the impact of flattening on the overall record count.</p>
  <h3>Explode and PosExplode Functions:</h3>
  <p>The explode, explode outer, and posexplode functions have been applied to a sample DataFrame to demonstrate their differences.</p>
  <p>Each function is illustrated with examples, and the resulting DataFrames are presented.</p>
  <h3>Filtering by ID:</h3>
  <p>Records with a specific ID value (1001) have been filtered from the DataFrame.</p>
  <p>This filtering operation retrieves specific rows based on the provided condition.</p>
  <h3>Convert Column Names:</h3>
  <p>Column names in camel case have been converted to snake case for consistency and readability.</p>
  <p>A custom function has been executed to perform this conversion, and the DataFrame with updated column names is shown.</p>
  <h3>Add Load Date Column:</h3>
  <p>A new column named 'load_date' has been added to the DataFrame, containing the current date for each record.</p>
  <p>This column provides information about when the data was loaded into the DataFrame.</p>
  <h3>Create Year, Month, and Day Columns:</h3>
  <p>From the 'load_date' column, three new columns ('year', 'month', 'day') have been generated to extract the corresponding date components.</p>
  <p>These columns facilitate further analysis and filtering based on specific date attributes.</p>
</body>
</html>


<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PySpark Assignments</title>
</head>
<body>
  <h1>PySpark Assignments</h1>
  <h2>Question 5</h2>
  <h3>Create DataFrames:</h3>
  <p>Three DataFrames - employee_df, department_df, and country_df - have been created with dynamically defined custom schemas.</p>
  <p>Each DataFrame corresponds to employee data, department data, and country data, respectively.</p>
  <h3>Average Salary of Each Department:</h3>
  <p>The average salary of each department has been calculated using the employee_df DataFrame, providing insights into salary distribution.</p>
  <h3>Employees Whose Names Start with 'M':</h3>
  <p>Employees whose names start with the letter 'M' have been identified along with their respective department names, aiding in specific employee record retrieval.</p>
  <h3>Bonus Calculation:</h3>
  <p>A new 'bonus' column has been added to the employee_df DataFrame by multiplying the employee's salary by 2, representing the bonus amount for each employee.</p>
  <h3>Reordering Column Names:</h3>
  <p>The column names of the employee_df DataFrame have been reordered as per the specified sequence, enhancing data organization and readability.</p>
  <h3>Join Operations:</h3>
  <p>Inner join, left join, and right join operations have been dynamically performed between the employee_df and department_df DataFrames, yielding different results based on the specified join type.</p>
  <h3>Update State to Country Name:</h3>
  <p>The 'State' column in the employee_df DataFrame has been updated to display country names instead, enhancing geographical information clarity.</p>
  <h3>Lowercase Column Names and Add Load Date:</h3>
  <p>All column names in the DataFrame resulting from Question 7 have been converted to lowercase. Additionally, a new column named 'load_date' has been added with the current date, denoting data loading timestamps.</p>
  <h3>About</h3>
  <p>This repository contains all my PySpark assignments.</p>
  <h3>Resources</h3>
  <ul>
    <li><a href="Readme">Readme</a></li>
    <li><a href="Activity">Activity</a></li>
  </ul>
  <p>© 2024 GitHub, Inc.</p>
</body>
</html>

