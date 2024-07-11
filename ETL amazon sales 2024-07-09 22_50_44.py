# Databricks notebook source
from pyspark.sql import SparkSession

# Iniciar uma sessão Spark
spark = SparkSession.builder.appName("AmazonSalesData").getOrCreate()

# Carregar o arquivo CSV do DBFS para um DataFrame Spark
amazonSales_df = spark.read.csv("dbfs:/FileStore/tables/AmazonSalesData.csv", sep= ',', header=True, inferSchema=True)

# Mostrar as primeiras linhas do DataFrame para verificar os dados
amazonSales_df.show()

# COMMAND ----------

amazonSales_df.printSchema()

# COMMAND ----------

# Criar a dimensão de localização, adaptando os nomes de colunas reais
location_df = amazonSales_df.select("Region", "Country").distinct()

location_df = location_df.withColumnRenamed("Region", "region")

# Salvar a dimensão de localização no Hive Metastore
location_df.write.mode("overwrite").saveAsTable("dim_location")


# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth

# Selecionar colunas de data
date_df = amazonSales_df.select("Order date").distinct()
date_df = date_df.withColumn("data", year("Order date"))
date_df = date_df.withColumn("month", month("Order date"))
date_df = date_df.withColumn("day", dayofmonth("Order date"))

date_df = date_df.withColumnRenamed("Order date", "Order_date")

# Salvar a dimensão de data no Hive Metastore
date_df.write.mode("overwrite").saveAsTable("dim_date")


# COMMAND ----------

# Selecionar colunas de orders
orders_columns = [
    "Sales Channel",
    "Order Priority",
    "Order ID",
    "Ship Date",
    "Item Type",
    "Unit Price",
    "Unit Cost",
    "Total Revenue",
    "Total Cost",
    "Units Sold",
    "Total Profit"
]

orders_df = amazonSales_df.select(orders_columns).distinct()

orders_df = orders_df.withColumnRenamed("Sales Channel", "Sales_Channel")
orders_df = orders_df.withColumnRenamed("Order Priority", "Order_Priorit")
orders_df = orders_df.withColumnRenamed("Order ID", "Order_ID")
orders_df = orders_df.withColumnRenamed("Ship Date", "Ship_Date")
orders_df = orders_df.withColumnRenamed("Item Type", "Item_Type")
orders_df = orders_df.withColumnRenamed("Unit Price", "Unit_Price")
orders_df = orders_df.withColumnRenamed("Unit Cost", "Unit_Cost")
orders_df = orders_df.withColumnRenamed("Total Revenue", "Total_Revenue")
orders_df = orders_df.withColumnRenamed("Total Cost", "Total_Cost")
orders_df = orders_df.withColumnRenamed("Total Profit", "Total_Profit")
orders_df = orders_df.withColumnRenamed("Units Sold", "Units_Sold")

# Salvar a dimensão de medições no Hive Metastore
orders_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_orders")

# COMMAND ----------

# Criar a tabela de fato
fact_df = amazonSales_df.select(
    "Region",
    "Country",
    "Order date",
    "Unit Price",
    "Unit Cost",
    "Total Revenue",
    "Total Cost",
    "Units Sold",
    "Total Profit",
    "Sales Channel",
    "Order Priority",
    "Order ID",
    "Ship Date",
    "Item Type"
)

# Juntando com dimensões para obter as chaves estrangeiras
fact_df = fact_df.join(date_df, fact_df["Order date"] == date_df["Order_date"])
fact_df = fact_df.join(location_df, fact_df["Country"] == location_df["Country"]) 
fact_df = fact_df.join(orders_df, fact_df["Order ID"] == orders_df["Order_ID"]) 

# Agora, fact_df contém a tabela de fato com chaves estrangeiras das dimensões


# Selecionar as chaves estrangeiras e as medidas
from pyspark.sql.functions import col



# Selecionando as colunas da tabela de fato
fact_df = amazonSales_df.select(
    col("region"), 
    col("Country"),
    col("Order date"),
    col("Unit Price"),
    col("Unit Cost"),
    col("Total Revenue"),
    col("Total Cost"),
    col("Units Sold"),
    col("Total Profit"),
    col("Sales Channel"),
    col("Order Priority"),
    col("Order ID"),
    col("Ship Date"),
    col("Item Type")
)

fact_df = fact_df.withColumnRenamed("Order date", "Order_date")
fact_df = fact_df.withColumnRenamed("Unit Price", "Unit_Price")
fact_df = fact_df.withColumnRenamed("Unit Cost", "Unit_Cost")
fact_df = fact_df.withColumnRenamed("Total Revenue", "Total_Revenue")
fact_df = fact_df.withColumnRenamed("Total Cost", "Total_Cost")
fact_df = fact_df.withColumnRenamed("Total Profit", "Total_Profit")
fact_df = fact_df.withColumnRenamed("Units Sold", "Units_Sold")
fact_df = fact_df.withColumnRenamed("Sales Channel", "Sales_Channel")
fact_df = fact_df.withColumnRenamed("Order Priority", "Order_Priorit")
fact_df = fact_df.withColumnRenamed("Order ID", "Order_ID")
fact_df = fact_df.withColumnRenamed("Ship Date", "Ship_Date")
fact_df = fact_df.withColumnRenamed("Item Type", "Item_Type")



# Salvar a tabela de fato no Hive Metastore
fact_df.write.mode("overwrite").saveAsTable("fact_amazon_sales")

