# Databricks notebook source
dbutils.widgets.text("filename","","")
file_name=dbutils.widgets.get("filename")
file=file_name.split(".")
file1=file[0]

# COMMAND ----------

url=dbutils.secrets.get("airflow","datalake-url")
key=dbutils.secrets.get("airflow","datalake-key")
spark.conf.set(
  url,
  key
)
# data lake file location:
file_location = "abfss://landingzone@datalake1407.dfs.core.windows.net/file/"+file_name
 
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "false").option("header", "false").option("sep",",").load(file_location)
 
#display the dataframe
display(df)

#Saving dataframe as delta Table
df.write.mode("append").format("delta").saveAsTable(file1)



# COMMAND ----------

c=spark.sql(f"select count(*) from {file1};")
print("no of record inserted")
print(abs(c.count()-df.count()))
print(f"total no of record after insertion in table")
c.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


