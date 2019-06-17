# Databricks notebook source
# Mount ADLS GEN1 file system to Databricks - Onetime Job

# configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
#            "dfs.adls.oauth2.client.id": "SECRET",
#            "dfs.adls.oauth2.credential": dbutils.secrets.get(scope = "SECRET", key = "SECRET"),
#            "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# dbutils.fs.mount(
#   source = "adl://mcio-arp-prod-c11.azuredatalakestore.net",
#   mount_point = "/mnt/mcio-arp-prod-c11/",
#   extra_configs = configs)

# COMMAND ----------

# Set up ADLS Gen 2 storage account 

spark.conf.set(
  "fs.azure.account.key.msuadlspocgen2.dfs.core.windows.net",
  dbutils.secrets.get(scope = "SECRET", key = "SECRET"))

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
input_filename = "Data_" + year_str + "_" + month_str + "_" + day_str + ".ss"
input_directory = "/mnt/mcio-arp-prod-c11/local/PublishedData/StorageUsageV2/" + year_str + "/" + month_str + "/" + day_str + "/"
input_path = input_directory + input_filename
output_filename = "Data_" + year_str + "_" + month_str + "_" + day_str + ".csv"
output_directory = "abfss://storagedata@msuadlspocgen2.dfs.core.windows.net/ProcessedData/StorageUsageV2CsvFromSS/" + year_str + "/" + month_str + "/" + day_str + "/"
output_path = output_directory + output_filename

storageusagev2 = sqlContext.read.format("sstream").load(input_path)

storageusagev2_storage = storageusagev2.where(upper(storageusagev2.ClusterType) == "STORAGE")

storageusagev2_storage_grouped_by_region = (storageusagev2_storage
.groupBy('Region')
.agg(sum('AvgUsageBytes').alias('Usage'))
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
