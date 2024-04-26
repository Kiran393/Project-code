import boto3
from datetime import datetime
import pandas 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import csv
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import first

#suffix 
today = datetime.now()
suffix = today.strftime("%Y-%m-%d")
print(suffix)

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    dyf = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_outbound_vw_cdwa_errorfile",
    transformation_ctx="dyf",
)

    datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_cdwa",
    transformation_ctx="datasource0",
)


    new_df = datasource0.toDF()

    new_df.createOrReplaceTempView("temp1")
    new_df1 = spark.sql("SELECT DISTINCT SUBSTR(p.fileName,1,LENGTH(p.fileName) - 4) as filename FROM temp1 p WHERE p.filemodifieddate = ((SELECT MAX(t2.filemodifieddate) AS MAX FROM temp1 t2));")
    new_df1.show(truncate=False)
    pandas_df1 = new_df1.toPandas()
    newsuffix = pandas_df1.iat[0,0]
    print(newsuffix)
    outboundDF = dyf.toDF()
    
    

    #outboundDF.coalesce(1).write.mode("overwrite").format("csv").option("header","true").save("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/cdwa/errorlog/CDWA-O-BG-ProviderInfo-"+suffix+"-error")
    pandas_df = outboundDF.toPandas()
    pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/cdwa/errorlog/"+newsuffix+"-error.csv", header=True, index=None,quotechar='"',encoding='utf-8', sep=',',quoting=csv.QUOTE_ALL)


if __name__ == "__main__": 
    main()