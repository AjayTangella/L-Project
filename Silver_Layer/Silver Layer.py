# Databricks notebook source
#importing libraries

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##read files for AdventureWorks_Calendar

# COMMAND ----------

from datetime import datetime, timedelta
#current_date = datetime.now() current date
current_date = datetime.now() - timedelta(days=1)
new_file_loaction = '/mnt/bronze/Full/' + current_date.strftime("%Y/%m/%d") + '/AdventureWorks_Calendar/'
#print(new_file_loaction)
df_Calendar=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
display(df_Calendar.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ###read file from bronze layer for  AdventureWorks_Customers

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now() -timedelta(days=1)
new_file_loaction='/mnt/bronze/Full/'+current_date.strftime("%Y/%m/%d")+'/AdventureWorks_Customers/'
# print(new_file_loaction)
df_Customers=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
#df_Customers.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### read file from bronze layer for AdventureWorks_Product_Categories

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now() -timedelta(days=1)
new_file_loaction='/mnt/bronze/Full/'+current_date.strftime("%Y/%m/%d")+'/AdventureWorks_Product_Categories/'
# print(new_file_loaction)
df_Product_Categories=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
#df_Product_Categories.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###read file from bronze layer for AdventureWorks_Products

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now() -timedelta(days=1)
new_file_loaction='/mnt/bronze/Full/'+current_date.strftime("%Y/%m/%d")+'/AdventureWorks_Products/'
# print(new_file_loaction)
df_Products=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
#display(df_Products.limit(2))
#df_Products.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###read file from broze layer for AdventureWorks_Returns table

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now() -timedelta(days=1)
new_file_loaction='/mnt/bronze/Full/'+current_date.strftime("%Y/%m/%d")+'/AdventureWorks_Returns/'
# print(new_file_loaction)
df_Returns=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
#display(df_Returns.limit(2))


# COMMAND ----------

# MAGIC %md
# MAGIC ###read file from broze layer for  AdventureWorks_Sales

# COMMAND ----------

from datetime import datetime, timedelta
current_date = datetime.now() - timedelta(days=1)
base_path = '/mnt/bronze/Full/' + current_date.strftime("%Y/%m/%d") + '/AdventureWorks_Sales_*'
# folders = ['AdventureWorks_Sales_2015', 'AdventureWorks_Sales_2016', 'AdventureWorks_Sales_2017']
# file_locations = [base_path + folder for folder in folders]
df_Sales = spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(base_path)
display(df_Sales.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###read file from bronze layer for AdventureWorks_Territories

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now() -timedelta(days=1)
new_file_loaction='/mnt/bronze/Full/'+current_date.strftime("%Y/%m/%d")+'/AdventureWorks_Territories/'
# print(new_file_loaction)
df_Territories=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
display(df_Territories.limit(2))


# COMMAND ----------

# MAGIC %md
# MAGIC ###read files from bronze layer for Product_Subcategories

# COMMAND ----------

from datetime import datetime,timedelta
current_date=datetime.now() -timedelta(days=1)
new_file_loaction='/mnt/bronze/Full/'+current_date.strftime("%Y/%m/%d")+'/Product_Subcategories/'
# print(new_file_loaction)
df_Product_Subcategories=spark.read.format("CSV").option("header", "true").option("inferSchema", "true").load(new_file_loaction)
display(df_Product_Subcategories.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calender  dataframe

# COMMAND ----------

#need to separeate month and year
df_Calendar=df_Calendar.withColumn("Month",month(col("date")))\
                        .withColumn("Year",year(col("date")))
#display(df_Calendar.limit(1))


df_Calendar.write.format("delta").mode("overwrite").save("/mnt/silver/Calendar")
#df_Calendar.write.format("parquet").mode("overwrite").save("/mnt/silver/Calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cutomer Df

# COMMAND ----------

display(df_Customers.limit(2))

# COMMAND ----------


#for separtor for multipule time use the lit 
# df_Customers=df_Customers.withColumn("FullName",concat(col("prefix"),lit(' '),col("FirstName"),lit(' '),col("LastName")))

#advance function for separator one time without using lit for multiple times
df_Customers=df_Customers.withColumn("FullName",concat_ws(' ',col("prefix"),col("FirstName"),col("LastName")))
# display(df_Customers.limit(2))
df_Customers.write.format("delta").mode("overwrite").save("/mnt/silver/Customers")
#df_Customers.write.format("parquet").mode("overwrite").save("dbfs:/mnt/silver/Customers1/")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Product_Subcategories df
# MAGIC

# COMMAND ----------

# display(df_Product_Subcategories.limit(2))

df_Product_Subcategories.write.format("delta").mode("overwrite").save("/mnt/silver/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Products

# COMMAND ----------

#1st word from ProductSKU and ProductName
df_Products=df_Products.withColumn("ProductSKU",split(col("ProductSKU"),"-")[0])\
                       .withColumn("ProductName",split(col("ProductName"),",")[0])


#load the data in silver layer
df_Products.write.format("delta").mode("overwrite").save("/mnt/silver/Products")


# COMMAND ----------

# MAGIC %md
# MAGIC ##returns

# COMMAND ----------

df_Returns.write.format("delta").mode("overwrite").save("/mnt/silver/Returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Territories

# COMMAND ----------

df_Territories.write.format("delta").mode("overwrite").save("/mnt/silver/Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales

# COMMAND ----------

display(df_Sales.limit(1))

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
df_Sales=df_Sales.withColumn("StockDate",to_timestamp(col("StockDate")))\
                 .withColumn("OrderNumber",regexp_replace(col("OrderNumber"),"S","T"))\
                 .withColumn("Multiply",col("OrderLineItem")*col("OrderQuantity"))
#display(df_Sales.limit(1))

#load the data into silver layer
df_Sales.write.format("delta").mode("overwrite").save("/mnt/silver/Sales")

# COMMAND ----------


