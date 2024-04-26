import boto3
import csv
from datetime import datetime
import pandas 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import first

#suffix 
today = datetime.now()
suffix = today.strftime("%Y%m%d_%H%M%S")
print(suffix)

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    dyf = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_outbound_vw_dshsprovider",
    transformation_ctx="dyf",
)
    outboundDF = dyf.toDF()
    pandas_df = outboundDF.toPandas()
    pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/dshsproviders/01250_PROVIDERS TPTOADSA_"+suffix+"_All.txt", header=None, index=None, sep='|', mode='a')
    #dataFrameWithOnlyOneColumn = outboundDF.select(F.concat(outboundDF.columns).alias('data'))
    #outboundDF.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header","False").text("s3://seiu-dev-b2b-datafeed/Outbound/dshs/ 01250_APSSN_TPTOADSA_"+suffix+"_All.csv")

if __name__ == "__main__": 
    main()