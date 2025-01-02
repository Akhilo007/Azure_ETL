# Databricks notebook source
# MAGIC %md
# MAGIC Mounting data from ADLS

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "d063c078-dc05-4cae-be21-2ade955b1818",
"fs.azure.account.oauth2.client.secret": 'dw78Q~8x-xfcidetX~jpOEIDa2pSXkiiOx.Yvasu',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/70de1992-07c6-480f-a318-a1afcba03983/oauth2/token"}


dbutils.fs.mount(
source = "abfss://rawdata@azureproj.dfs.core.windows.net",
mount_point = "/mnt/rawdata",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/rawdata"

# COMMAND ----------

# MAGIC %md
# MAGIC Loading Data

# COMMAND ----------

cars_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/rawdata/Cars/Car.csv")
cosmetics_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/rawdata/Cosmetics/Cosmetics.csv")
delhiclimate_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/rawdata/DelhiClimate/DailyDelhiClimateTest.csv")
transaction_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/rawdata/Transaction/transaction.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC Transformations on Cars data

# COMMAND ----------

cars_df.show()

# COMMAND ----------

cars_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Casting Cars data DataType

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType
for col_name, col_type in cars_df.dtypes:
    if col_type == "double":
        cars_df = cars_df.withColumn(col_name, cars_df[col_name].cast(IntegerType()))

# Print the updated schema
print("Updated Schema:")
cars_df.printSchema()

# Show the DataFrame with updated column types
cars_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformations on Cosmetics data

# COMMAND ----------

cosmetics_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering and renaming required columns

# COMMAND ----------

#selecting columns and renaming required column
required_columns = ["product_id", "product_name", "brand_name", "rating", "reviews", "price_usd"]
cosmetics_df = cosmetics_df.select(*[col for col in required_columns],cosmetics_df.primary_category.alias("category"))
cosmetics_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Transformations on Delhi_climate_data

# COMMAND ----------

delhiclimate_df.printSchema()

# COMMAND ----------

delhiclimate_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Round the double values to 2 decimal points

# COMMAND ----------

from pyspark.sql.functions import round
columns_to_round = ["meantemp", "humidity","wind_speed","meanpressure"]

# Apply rounding to the specified columns
for col_name in columns_to_round:
    delhiclimate_df = delhiclimate_df.withColumn(col_name, round(delhiclimate_df[col_name], 2))

#print the rounded DataFrame
delhiclimate_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation on Transactions data

# COMMAND ----------

transaction_df.show()

# COMMAND ----------

from pyspark.sql.functions import max, min

# Calculate max and min transaction dates
max_date = transaction_df.select(max("date")).collect()[0][0]
min_date = transaction_df.select(min("date")).collect()[0][0]

# Print results
print(f"Min Transaction Date: {min_date}")
print(f"Max Transaction Date: {max_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC need transactions only for year 2015

# COMMAND ----------

from pyspark.sql.functions import year

# Filter transactions for the year 2015
transaction_df = transaction_df.filter(year("date") == 2015)

# Show the resulting DataFrame
transaction_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Storing transformed Data Back into ADLS account

# COMMAND ----------

cars_df.write.mode("append").option("header","true").parquet("/mnt/rawdata/Transformed_Data/Cars")
cosmetics_df.write.mode("append").option("header","true").parquet("/mnt/rawdata/Transformed_Data/Cosmetics")
delhiclimate_df.write.mode("append").option("header","true").parquet("/mnt/rawdata/Transformed_Data/DelhiClimate")
transaction_df.write.mode("append").option("header","true").parquet("/mnt/rawdata/Transformed_Data/Transaction")


# COMMAND ----------

#unmounnt adls
dbutils.fs.unmount("/mnt/rawdata")