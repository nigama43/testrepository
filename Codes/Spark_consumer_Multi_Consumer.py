# Databricks notebook source
# DBTITLE 1,#Creating and Initializing Widgets
# Creating and Initializing Widgets
dbutils.widgets.text("eventhub_name","","");
dbutils.widgets.text("eventhub_namespace","","");
dbutils.widgets.text("access_key_name", "","");
dbutils.widgets.text("access_key","","");
dbutils.widgets.text("Target_Table_Name","","");
dbutils.widgets.text("primary_key","","");


eventhub_name = dbutils.widgets.get("eventhub_name");
eventhub_namespace = dbutils.widgets.get("eventhub_namespace");
access_key_name = dbutils.widgets.get("access_key_name");
access_key = dbutils.widgets.get("access_key");
Target_Table_Name = dbutils.widgets.get("Target_Table_Name");
primary_key = dbutils.widgets.get("primary_key");

# COMMAND ----------

# DBTITLE 1,#importing packages
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json
import json
from urllib.parse import unquote
from pyspark.sql.functions import udf
from delta.tables import *
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import datetime as dt
from pyspark.sql.functions import when,col
from delta.tables import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,#Connection to EventHub
#Connection to eventhub for streaming data



connection_template = f"Endpoint=sb://{eventhub_namespace}.servicebus.windows.net/;SharedAccessKeyName={access_key_name};SharedAccessKey={access_key};EntityPath={eventhub_name}"
# Start from beginning of stream
startOffset = "-1"

# End at the current time. This datetime formatting creates the correct string format from a python datetime object
endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

ehConf = {}
# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

endingEventPosition = {
  "offset": None,           #not in use
  "seqNo": -1,              #not in use
  "enqueuedTime": endTime,
  "isInclusive": True
}


# Put the positions into the Event Hub config dictionary
#ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
#ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_template)

# COMMAND ----------

dict1={}
List1 =["Person.Address","Person.Person"]
for table in List1:
    Target_table_schema=spark.sql(f"select * from {table}").schema
    dict1.update({table:Target_table_schema})

# COMMAND ----------

# Function to insert ,update ,delete microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
    deltaTable.alias("t").merge(\
    microBatchOutputDF.alias("s"),\
    f"s.{primarykey} = t.{primarykey}") \
    .whenMatchedDelete(condition = "s.op = 'd'")\
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll(condition = "s.op = 'c'") \
    .execute()

def processing(tablename ,primarykey):
    #Reading Streaming data from eventhub 
    read_df = spark.readStream.format("eventhubs").options(**ehConf).load()

    #Converting Streaming data from Binary format to String
    bodyString=read_df.withColumn("Body", col("body").cast("String")).select("Body")
    bodyString.createOrReplaceTempView("jsonStr")

    #Reading Schema from Target Delta Table
    Target_table_schema=spark.sql(f"select * from {tablename}").schema
    deltaTable = DeltaTable.forName(spark, f"{Target_Table_Name}")
    #Extracting payload Streaming json data in table foramt 
    json_payload=spark.sql(f"select Body:before ,Body:after,Body:op,concat(Body:source:schema,'.',Body:source:table) as tablename from jsonStr")

    #Identifying the data is insert, update or delete
    changetype = json_payload.withColumn("rawData", when(json_payload.op == "d",json_payload.before).otherwise(json_payload.after))
    primarykey=primarykey
    schema_type = changetype.withColumn("data",when(changetype.tablename == tablename,from_json("rawData",Target_table_schema)))
    final_data=schema_type.filter(f"tablename ='{tablename}'").select("op",col("data.*")).na.drop("all")
    # Write the output of a streaming  into Delta table
    final_data.writeStream \
    .format("delta") \
    .foreachBatch(upsertToDelta) \
    .outputMode("update") \
    .option("checkpointLocation", f"/checkpoint/{schemaname}/1")\
    .start()

# COMMAND ----------

processing("Person.Address","AddressID")

# COMMAND ----------

processing("Person.Person","BusinessEntityID")

# COMMAND ----------

# DBTITLE 1,#Converting Json data in table format
#Reading Streaming data from eventhub 
read_df = spark.readStream.format("eventhubs").options(**ehConf).load()

#Converting Streaming data from Binary format to String
bodyString=read_df.withColumn("Body", col("body").cast("String")).select("Body")
bodyString.createOrReplaceTempView("jsonStr")
from pyspark.sql.functions import when,col
#Reading Schema from Target Delta Table
#target_table = spark.sql("select concat(Body:source:schema,'.',Body:source:table) from jsonStr").collect
#Target_table_schema=spark.sql(f"select * from {Target_Table_Name}").schema

#Extracting payload Streaming json data in table foramt 
json_payload=spark.sql(f"select Body:before ,Body:after,Body:op,concat(Body:source:schema,'.',Body:source:table) as schemaname from jsonStr")

#Identifying the data is insert, update or delete
changetype = json_payload.withColumn("data", when(json_payload.op == "d",json_payload.before).otherwise(json_payload.after))

schema_type = changetype.withColumn("actual_data_person_address",when(changetype.schemaname == "Person.Address",from_json("data",dict1['Person.Address']))).withColumn("actual_data_person_Person",when(changetype.schemaname == "Person.Person",from_json("data",dict1['Person.Person'])))

# COMMAND ----------

display(schema_type)

# COMMAND ----------

# DBTITLE 1,#Final DataFrame contain Change Record
address_data=schema_type.filter("schemaname ='Person.Address'").select("op",col("actual_data_person_address.*")).na.drop("all")
person_data=schema_type.filter("schemaname='Person.Person'").select("op",col("actual_data_person_Person.*")).na.drop("all")

# COMMAND ----------

display(address_data)

# COMMAND ----------

display(person_data)

# COMMAND ----------

# DBTITLE 1,#Merge Changes to Target Table
from delta.tables import *
from pyspark.sql.functions import *
from delta.tables import *

#Reading taget Delta table in dataframe
deltaTable = DeltaTable.forName(spark, f"{Target_Table_Name}")

# Function to insert ,update ,delete microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
    deltaTable.alias("t").merge(\
    microBatchOutputDF.alias("s"),\
    f"s.{primary_key} = t.{primary_key}") \
    .whenMatchedDelete(condition = "s.op = 'd'")\
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll(condition = "s.op = 'c'") \
    .execute()

# Write the output of a streaming  into Delta table
address_data.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option("checkpointLocation", f"/checkpoint/{Target_Table_Name}/1")\
  .start()


# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from delta.tables import *

#Reading taget Delta table in dataframe
deltaTable_person = DeltaTable.forName(spark, "Person.Person")
primary_key_person='BusinessEntityID'
# Function to insert ,update ,delete microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
    deltaTable_person.alias("t").merge(\
    microBatchOutputDF.alias("s"),\
    f"s.{primary_key_person} = t.{primary_key_person}") \
    .whenMatchedDelete(condition = "s.op = 'd'")\
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll(condition = "s.op = 'c'") \
    .execute()

# Write the output of a streaming  into Delta table
person_data.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option("checkpointLocation", f"/checkpoint/Person/1")\
  .start()


# COMMAND ----------


