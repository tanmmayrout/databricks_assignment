# Databricks notebook source
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
import re

%run ../source_to_bronze/utils



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


#Read the file located in DBFS location source_to_bronze with as data frame different read methods using custom schema

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True),
])

employee_df = spark.read.schema(employee_schema).csv("/source_to_bronze/employee.csv", header=True)

employee_df.show()

# COMMAND ----------

#Convert CamelCase to SnakeCase:

def camel_to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

camel_to_snake_udf = udf(camel_to_snake, StringType())

employee_snake_case_df = employee_df.toDF(*[camel_to_snake(col) for col in employee_df.columns])

employee_snake_case_df.show()


# COMMAND ----------

#Add load date

from pyspark.sql.functions import current_date

employee_snake_case_df = employee_snake_case_df.withColumn("load_date", current_date())

employee_snake_case_df.show()


# COMMAND ----------

# Write as Delta Table with schema overwrite enabled
employee_snake_case_df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("/silver/Employee_info/dim_employee")

# Verify the paths
display(dbutils.fs.ls("/silver/Employee_info/dim_employee"))