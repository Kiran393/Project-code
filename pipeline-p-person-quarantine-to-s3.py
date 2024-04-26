import boto3
from datetime import datetime
import pandas 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import first
from pyspark.sql.types import StructType,StructField, StringType
import csv
import boto3
import json
## @params: [JOB_NAME]
today = datetime.now()
suffix = today.strftime("%m_%d_%Y")
print(suffix)



def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    outboundDF = glueContext.create_data_frame.from_catalog( 
    database = "seiubg-rds-b2bds", 
    table_name = "b2bds_staging_personquarantine", 
    transformation_ctx = "outboundDF")
    
    #new_df = spark.createDataFrame(new_df.rdd, doh_completed_Schema)
    outboundDF.printSchema()
    outboundDF = outboundDF.withColumn("hiredate",col("hiredate").cast("String")).withColumn("trackingdate",col("trackingdate").cast("String")).withColumn("person_unique_id",col("person_unique_id").cast("String")).withColumn("cdwaid",col("cdwaid").cast("String")).withColumn("dshsid",col("dshsid").cast("String")).withColumn("personid",col("personid").cast("String"))
    outboundDF.printSchema()
    pandas_df = outboundDF.toPandas()
    print(pandas_df.dtypes)
    pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/PersonQuarantine/Quarantined-Persons-"+suffix+".csv",header=True, index=None, sep=',',encoding='utf-8-sig',quoting=csv.QUOTE_ALL)
    #dataFrameWithOnlyOneColumn = outboundDF.select(F.concat(outboundDF.columns).alias('data'))
    #outboundDF.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","False").text("s3://seiu-dev-b2b-datafeed/Outbound/dshs/ 01250_APSSN_TPTOADSA_"+suffix+"_All.csv")
if __name__ == "__main__": 
    main()