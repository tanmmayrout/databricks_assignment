# Databricks notebook source
#Driver notebook for the first step

from pyspark.sql import SparkSession

%run ./utils


# COMMAND ----------


#Read the 3 datasets as Dataframe in employee_source_to_bronze
display(dbutils.fs.ls("/FileStore/tables/")) #Verify the paths 

country_df = spark.read.csv("/FileStore/tables/Country_Q1.csv", header=True, inferSchema=True)
department_df = spark.read.csv("/FileStore/tables/Department_Q1.csv", header=True, inferSchema=True)
employee_df = spark.read.csv("/FileStore/tables/Employee_Q1.csv", header=True, inferSchema=True)

# Show the first few records of each DataFrame to ensure they're loaded correctly
country_df.show()
department_df.show()
employee_df.show()


# COMMAND ----------

#Write to a location in DBFS
employee_df.write.csv("/source_to_bronze/employee.csv", header=True, mode="overwrite")
department_df.write.csv("/source_to_bronze/department_df.csv", header=True, mode="overwrite")
country_df.write.csv("/source_to_bronze/country_df.csv", header=True, mode="overwrite")

display(dbutils.fs.ls("/source_to_bronze/")) #Verify the paths 
