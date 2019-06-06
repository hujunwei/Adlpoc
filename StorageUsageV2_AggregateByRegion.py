# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.msuadlspocgen2.dfs.core.windows.net",
  dbutils.secrets.get(scope = "junweiscope", key = "msuadlsgen2pocaccesskey"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from dateutil import parser

# Parse pipeline parameter
dbutils.widgets.text("windowStartTime", "","")
dbutils.widgets.get("windowStartTime")
windowStartTime_str = getArgument("windowStartTime")
windowStartTime = parser.parse(windowStartTime_str)
year_str = windowStartTime.strftime("%Y")
month_str = windowStartTime.strftime("%m")
day_str = windowStartTime.strftime("%d")

# Create Path parameter
input_filename = "Data_" + year_str + "_" + month_str + "_" + "day_str" + ".csv"
input_directory = "abfss://storagedata@msuadlspocgen2.dfs.core.windows.net/CopiedData/StorageUsageV2Csv/" + year_str + "/" + month_str + "/" + day_str + "/"
input_path = input_directory + input_filename
output_filename = input_filename
output_directory = "abfss://storagedata@msuadlspocgen2.dfs.core.windows.net/ProcessedData/StorageUsageV2Csv/" + year_str + "/" + month_str + "/" + day_str + "/"
output_path = output_directory + output_filename

# Extract csv as dataframe with schema
schema = StructType([
  StructField("Date",StringType(),True),
  StructField("Cloud",StringType(),True),
  StructField("Cluster",StringType(),True),
  StructField("TenantSuffix",StringType(),True),
  StructField("ClusterType",StringType(),True),
  StructField("Geo",StringType(),True),
  StructField("Region",StringType(),True),
  StructField("DataCenter",StringType(),True),
  StructField("SubscriptionGuid",StringType(),True),
  StructField("AccountName",StringType(),True),
  StructField("IsSRP",BooleanType(),True),
  StructField("IsXIO",BooleanType(),True),
  StructField("AccountType",StringType(),True),
  StructField("ResourceType",StringType(),True),
  StructField("ResourceSubtype",StringType(),True),
  StructField("MaxUsageBytes",DoubleType(),False),
  StructField("AvgUsageBytes",DoubleType(),False),
  StructField("ProvisionedDisks",DoubleType(),False),
  StructField("TotalTransactions",LongType(),False),
  StructField("TotalIngress",LongType(),True),
  StructField("TotalEgress",LongType(),True),
  StructField("BytesHoursCaptured",DoubleType(),True),
  StructField("TransactionHoursCaptured",DoubleType(),True),
  StructField("IsManagedDisk",BooleanType(),True),
  StructField("IsImputed",BooleanType(),True),
  StructField("PhysicalAvailabilityZone",StringType(),True),
  StructField("AvailabilityZoneCode",StringType(),True),
  StructField("CloudCustomerGUID",StringType(),True),
  StructField("OfferType",StringType(),True),
  StructField("AccountTag",StringType(),True),
  StructField("GeoSetup",StringType(),True),
  StructField("IsV2Account",BooleanType(),True),
  StructField("IsReadyForCustomer",BooleanType(),True),
  StructField("IOps",DoubleType(),False)])

storageusagev2 = sqlContext.read.schema(schema).format('com.databricks.spark.csv').load(input_path)

storageusagev2_storage = storageusagev2.where(upper(storageusagev2.ClusterType) == "STORAGE")

storageusagev2_storage_grouped_by_region = (storageusagev2_storage
.groupBy('Region')
.agg(pyspark.sql.functions.sum('AvgUsageBytes').alias('Usage'))
.withColumn('Usage', col('Usage') / 2**50)
.select('Region','Usage'))




# COMMAND ----------

# Save as temp file in ADLS Gen2
(storageusagev2_storage_grouped_by_region.coalesce(1)
 .write
 .format("com.databricks.spark.csv")
 .option("header","true")
 .mode("overwrite")
 .save(output_directory))

# Save as temp file in ADLS Gen2, rename temp file, remove system files. Should be abstract as a func
files = dbutils.fs.ls(output_directory)
output_file = [x for x in files if x.name.startswith("part-")]
system_file = [x for x in files if x.name.startswith("_")]
dbutils.fs.mv(output_file[0].path, output_path)
for i in range (0, len(system_file)):
    file_name = system_file[i].name
    dbutils.fs.rm(output_directory + file_name, True)
    print ('Removed:' + file_name)
