"""
employee_setup_and_transformations.py

This script initializes a SparkSession, defines a schema, loads employee data from a CSV file,
displays the schema, creates a temporary view, and performs multiple transformations and queries
on employee data using PySpark.

Note:
- If running in a Jupyter notebook, install PySpark using: !pip install pyspark
- If running this script from the terminal, install PySpark first with: pip install pyspark

Tasks covered:
- Task 1: Generate DataFrame from CSV data
- Task 2: Define a schema for the data
- Task 3: Display schema of DataFrame
- Task 4: Create a temporary view
- Task 5–15: Transformations, filters, aggregations, and joins
"""

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, avg, max as spark_max, sum as spark_sum, count

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Final Project - Data Analysis using Spark") \
    .getOrCreate()

# Define schema for the employees CSV file
schema = StructType([
    StructField("Emp_No", IntegerType(), True),
    StructField("Emp_Name", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Department", StringType(), True)
])

# Load CSV data using defined schema
employees_df = spark.read.csv("data/employees.csv", header=True, schema=schema)

# Display the schema of the DataFrame
employees_df.printSchema()

# Create a temporary view named "employees" to enable SQL queries
employees_df.createOrReplaceTempView("employees")

# ---------------------- Transformations and Queries ----------------------

# Task 5: Execute SQL query to fetch employees older than 30
print("Task 5: Employees older than 30")
spark.sql("SELECT * FROM employees WHERE Age > 30").show()

# Task 6: Calculate Average Salary by Department
print("Task 6: Average salary by department")
spark.sql("""
    SELECT ROUND(AVG(Salary), 2) AS AVG_Salary, Department 
    FROM employees 
    GROUP BY Department
""").show()

# Task 7: Filter and Display IT Department Employees
print("Task 7: Employees in the IT department")
employees_df.filter(employees_df["Department"] == "IT").show()

# Task 8: Add 10% Bonus to Salaries
print("Task 8: Add 10% bonus to salaries")
employees_df = employees_df.withColumn(
    "SalaryAfterBonus", col("Salary") + col("Salary") * 0.1
)

# Task 9: Find Maximum Salary by Age
print("Task 9: Max salary by age")
employees_df.groupBy("Age").agg(spark_max("Salary").alias("Max_Salary")).show()

# Task 10: Self-Join on Employee Data
print("Task 10: Self-join on Emp_No")
employees_df.join(employees_df, "Emp_No", "inner").show()

# Task 11: Calculate Average Employee Age
print("Task 11: Average employee age")
employees_df.agg(avg("Age").alias("AVG_Age")).show()

# Task 12: Calculate Total Salary by Department
print("Task 12: Total salary by department")
employees_df.groupBy("Department").agg(spark_sum("Salary").alias("Total_Salary")).show()

# Task 13: Sort Data by Age and Salary
print("Task 13: Sort by Age (asc), Salary (desc)")
employees_df.sort(["Age", "Salary"], ascending=[True, False]).show()

# Task 14: Count Employees in Each Department
print("Task 14: Number of employees per department")
employees_df.groupBy("Department").agg(count("Emp_No").alias("Emp_Count")).show()

# Task 15: Filter Employees with the letter 'o' in the name
print("Task 15: Employees with 'o' in their name")
employees_df.filter(employees_df["Emp_Name"].contains("o")).show()

# Stop the Spark session
spark.stop()