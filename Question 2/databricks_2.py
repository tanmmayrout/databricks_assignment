# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# Import necessary libraries
import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to obtain data from the API
def fetch_data(url):
    """Fetch data from the API and return JSON response."""
    try:
        response = requests.get(url)
        # Check if the response status code is not 200
        if response.status_code != 200:
            logger.error(f"Request failed with status code: {response.status_code} - {response.text}")
            return None
          # Save response to a JSON file
        try:
            with open('data.json', 'w') as json_file:
                json.dump(response.json(), json_file, indent=4)
            logger.info("Data successfully saved to data.json")
        except Exception as e:
            logger.error(f"Failed to save data to JSON file: {str(e)}")
        
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}") 
        return None
# API 
API_URL = "https://reqres.in/api/users?page=2"

#Calling function to fetch data 
data = fetch_data(API_URL)
print(f"Fetched {data} records")

# Extract relevant data
fetched_data = data['data']  # Extract the data key 

# Print for debugging
print(type(fetched_data))  # Should print <class 'list'>
print(fetched_data)


# COMMAND ----------

# Filter out unnecessary keys (e.g., 'page')
relevant_data = [
    {key: record[key] for key in ['id', 'email', 'first_name', 'last_name', 'avatar']}
    for record in fetched_data
]


# Define custom schema
custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])
#Dataframe with custom schema 
data_df = spark.createDataFrame(fetched_data, schema=custom_schema)


data_df.show()


# COMMAND ----------

from pyspark.sql.functions import lit, current_date

# Add site_address and load_date columns
final_df = data_df.withColumn("site_address", lit("reqres.in")) \
                  .withColumn("load_date", current_date())

# Transformed dataframe 
final_df.show()


# COMMAND ----------

# Write the data frame to location in DBFS as /db_name /table_name
final_df.write.format("delta") \
              .mode("overwrite") \
              .save("/site_info/person_info")

print("Data successfully written to DBFS at /site_info/person_info")


# COMMAND ----------

# Data validation read data from Delta format
read_df = spark.read.format("delta").load("/site_info/person_info")

# Show the data
read_df.show()
