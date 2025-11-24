# Databricks notebook source
spark

# COMMAND ----------

storage_account = "olistdatastorageaccnt"
application_id = "2a50ea48-4396-4735-a4ca-ef144e14377b"
directory_id = "3f7f845a-f4da-4116-96d0-a735adea06f6"


spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

customer_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", True)
    .load("abfss://olistdata@olistdatastorageaccnt.dfs.core.windows.net/bronze/olist_customers_dataset.csv")
)
display(customer_df)


# COMMAND ----------

base_path = "abfss://olistdata@olistdatastorageaccnt.dfs.core.windows.net/bronze/"
orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").load(payments_path)
reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mongo DB Data For Enrichment 

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

hostname = "991lxy.h.filess.io"
database = "olistDataNoSQL_stovedrive"
port = "27018"
username = "olistDataNoSQL_stovedrive"
password = "5dc5be2d703cc01388e9090d94eb8432f8695f4b"

# Creating the MongoDB URI string
uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"

# Connect to MongoDB
client = MongoClient(uri)

# Accessing the database
mydb = client[database]

print("Connected successfully!")

# COMMAND ----------

import pandas as pd
collection = mydb['product_categories']

mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

products_df.display()

# COMMAND ----------

mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning the Data

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def clean_data(df, name):
    print("Cleaned " + name)
    return df.dropDuplicates().na.drop("all")
# means drop rows where ALL columns are NULL.


# COMMAND ----------

orders_df = clean_data(orders_df, "Orders")
display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

# Convert String Columns → Date Columns
orders_df = orders_df \
    .withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp"))) \
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))

orders_df.display()

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

# calculate delivery and delay time
orders_df = orders_df.withColumn("actual_delivery_time", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
orders_df = orders_df.withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp"))
orders_df = orders_df.withColumn("Delay_Time", col("actual_delivery_time") - col("estimated_delivery_time"))

orders_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Data

# COMMAND ----------

orders_cutomers_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id, "left")

orders_payments_df = orders_cutomers_df.join(payments_df, orders_cutomers_df.order_id == payments_df.order_id, "left")

orders_items_df = orders_payments_df.join(items_df, "order_id", "left")

orders_items_products_df = orders_items_df.join(products_df, orders_items_df.product_id == products_df.product_id, "left")

final_df = orders_items_products_df.join(sellers_df, orders_items_products_df.seller_id == sellers_df.seller_id, "left")

# COMMAND ----------

final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Enrichment Via Mongo DB

# COMMAND ----------

# Remove the MongoDB _id field (since Spark cannot infer its type)
mongo_data.drop('_id', axis=1, inplace=True)

# Convert the Pandas DataFrame into a Spark DataFrame
mongo_sparf_df = spark.createDataFrame(mongo_data)

# Display the Spark DataFrame
display(mongo_sparf_df)

# COMMAND ----------

final_df = final_df.join(
    mongo_sparf_df,
    "product_category_name",
    "left"
)


# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns
    
    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)

    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned


# Apply the function to final_df
final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastorageaccnt.dfs.core.windows.net/silver")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

