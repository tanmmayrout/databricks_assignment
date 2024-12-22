# Databricks notebook source
# MAGIC %run ../source_to_bronze/utils
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Read Silver Layer Table
silver_df = spark.read.format("delta").load("/silver/Employee_info/dim_employee")

silver_df.show()

# COMMAND ----------

#The salary of each department in descending order

salary_df = silver_df.groupBy("department").sum("salary").orderBy("sum(salary)", ascending=False)

salary_df.show()


# COMMAND ----------

#Number of employees in each department and country

employee_count_df = silver_df.groupBy("department","country").count()

employee_count_df.show()


# COMMAND ----------

# Check the schema of employee_df to understand its structure
silver_df.printSchema()


# COMMAND ----------

#Department and country names:
department_df = spark.read.csv("/source_to_bronze/department_df.csv", header=True, inferSchema=True)
country_df = spark.read.csv("/source_to_bronze/country_df.csv", header=True, inferSchema=True)

department_df.show()

country_df.show()


# COMMAND ----------

# Join employee_df with department_df to get department names
joined_with_department = silver_df.join(department_df, silver_df.department == department_df.DepartmentID, "inner")

# Join the resulting DataFrame with country_df to get country names
final_df = joined_with_department.join(country_df, silver_df.country == country_df.CountryCode, "inner")

result = final_df.select("DepartmentName", "CountryName")
result = result.distinct()

result.show()


# COMMAND ----------

#List the department names along with their corresponding country names.
employee_with_department = silver_df.join(department_df, silver_df.department == department_df.DepartmentID, "inner")

employee_with_details = employee_with_department.join(country_df, silver_df.country == country_df.CountryCode, "inner")

employee_final = employee_with_details.select(
    "employee_i_d", "name", "DepartmentName", "CountryName", "salary", "age", "load_date"
)

# Show the updated employee DataFrame
employee_final.show()


# COMMAND ----------


from pyspark.sql.functions import year, col, current_date, avg

# Group by department_id and calculate the average age
avg_age_df = silver_df.groupBy("department").agg(avg("age").alias("average_age"))

# Show the result
avg_age_df.show()



# COMMAND ----------


final_df = employee_final.withColumn("at_load_date", current_date())

final_df.show()


# COMMAND ----------

final_df.write.format("delta").mode("overwrite").save("/gold/employee/fact_employee")

# Verify the paths
display(dbutils.fs.ls("/gold/employee/fact_employee"))