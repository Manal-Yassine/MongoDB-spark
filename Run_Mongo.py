import time
import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient
import numpy as np
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, when
import logging

# Configure logging
logging.basicConfig(filename='pyspark_output.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Suppress PySpark logging on the console
logger = logging.getLogger('py4j')
logger.setLevel(logging.ERROR)

# MongoDB connection string
mongo_uri = "mongodb+srv://Mona:Mongo-23-job@cluster0.rsxcmkb.mongodb.net/My_test_database"#"mongodb://192.168.1.44:27017"
client = None

# Retry loop with increasing delay to wait for MongoDB to be ready
max_retries = 5
retries = 0
connected = False


connection_string = "mongodb+srv://Mona:Mongo-23-job@cluster0.rsxcmkb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    
    # Connect to MongoDB Atlas
client = MongoClient(connection_string)
    
    
# Once connected to MongoDB, proceed with your operations
db = client["My_test_database"]
collection = db["my_test_collection"]

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MongoDBSparkConnector") \
    .config("spark.mongodb.input.uri", mongo_uri + ".My_test_database.my_test_collection") \
    .config("spark.mongodb.output.uri", mongo_uri + ".My_test_database.output") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Retry loop for Spark to ensure MongoDB is fully ready
spark_retries = 3
for _ in range(spark_retries):
    try:
      #  df_all = spark.read.format("mongo").load()
        df_all = spark.read.format("mongo").option("database", "My_test_database").option("collection", "my_test_collection").load()
        total_rows = df_all.count()
        print("Total number of rows loaded into Spark:", total_rows)
        break  # Exit retry loop if successful
    except Exception as e:
        logging.error(f"Error connecting to MongoDB from Spark: {e}")
        print(f"Error connecting to MongoDB from Spark: {e}")
        time.sleep(10)  # Wait 10 seconds before retrying

# Check if Spark DataFrame is successfully loaded
if total_rows is None:
    logging.error("Failed to load data from MongoDB into Spark DataFrame. Exiting.")
    print('Error: Failed to load data from MongoDB into Spark DataFrame.')
    exit(1)

# Perform Spark operations
df = df_all.limit(total_rows // 100)

# Group by InvoiceNo
grouped_by_invoice = df.groupBy("InvoiceNo").agg(sum("Quantity").alias("TotalQuantity"))
grouped_by_invoice.show()

# Product with the most sales
product_sales = df.groupBy("StockCode").agg(sum("Quantity").alias("TotalQuantity"))
max_sold_product = product_sales.orderBy(col("TotalQuantity").desc()).first()
print("Product with the most sales:", max_sold_product)

# Customer who spent the most money
dfCost = df.withColumn("TotalCost", col("Quantity") * col("UnitPrice"))
dfCost_Wo_Na = dfCost.na.drop(subset=["CustomerID"])
product_money = dfCost_Wo_Na.groupBy("CustomerID").agg(sum("TotalCost").alias("TotalSpent"))
max_spending_customer = product_money.orderBy(col("TotalSpent").desc()).first()
print("Customer who spent the most money:", max_spending_customer)

# Product distribution by country
product_distribution = df.groupBy("Country", "StockCode").agg(sum("Quantity").alias("TotQuantity"))
product_distribution_ps = product_distribution.toPandas()

# Plot distribution
plt.figure(figsize=(12, 8))
sns.barplot(data=product_distribution_ps, x="Country", y="TotQuantity", hue="StockCode")
plt.title("Distribution of Each Product for Each Country")
plt.savefig('product_distribution.png')  # Save the figure as PNG
#plt.show()

# Average unit price
average_unit_price = df.agg(avg("UnitPrice")).first()
print("Average unit price:", average_unit_price)

# Distribution of unit prices
unit_prices = df.select("UnitPrice").dropna().filter(col("UnitPrice").isNotNull()).rdd.flatMap(lambda x: x).collect()
unit_prices = np.array(unit_prices, dtype=np.float64)

plt.figure(figsize=(10, 6))
plt.hist(unit_prices, bins=50, color='blue', edgecolor='black')
plt.title('Distribution of Unit Prices')
plt.xlabel('Unit Price')
plt.ylabel('Frequency')
plt.grid(True)
plt.savefig('price_distribution.png')  # Save the figure as PNG
#plt.show()

# Ratio between UnitPrice and Quantity
dfprice = df.withColumn("PriceQuantityRatio", when(col("Quantity") != 0, col("UnitPrice") / col("Quantity")).otherwise(None))
dfprice.show()

