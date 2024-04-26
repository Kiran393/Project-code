import boto3
from datetime import datetime
import pandas 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import first, lit,col
from awsglue.transforms import *
import csv


#suffix 
today = datetime.now()
suffix = today.strftime("%Y-%m-%d")
filename = "CDWA-I-BG-TrainingStatusUpdate-"+suffix+".csv"
print(suffix)

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    #spark.sql('set spark.sql.caseSensitive=true')

    dyf = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_outbound_cdwa_trainingstatus",
    transformation_ctx="dyf",
)
    
    outboundDF = dyf.toDF()
    outboundDF = outboundDF.withColumn("Employee ID",col("employee id").cast("String"))
    pandas_df = outboundDF.toPandas()
    pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/cdwa/Trainee_status_file/" + filename, index=None, sep=',', mode='w', encoding = 'utf-8', quoting=csv.QUOTE_ALL)
    
    outboudlogdf = outboundDF.withColumn("filename",lit(filename))
    outboudlogdf = DynamicFrame.fromDF(outboudlogdf, glueContext, "outboudlogdf")
    
        # Script generated for node ApplyMapping
    ApplyMapping_node2 = ApplyMapping.apply(
        frame=outboudlogdf,
        mappings=[
            ("employee id", "string", "employeeid", "string"),
            ("person id", "long", "personid", "long"),
            ("compliance status", "string", "compliancestatus", "string"),
            ("training exempt", "string", "trainingexempt", "string"),
            ("training program", "string", "trainingprogram", "string"),
            ("training program status", "string", "trainingstatus", "string"),
            ("class name", "string", "classname", "string"),
            ("dshs course code", "string", "dshscoursecode", "string"),
            ("credit hours", "decimal", "credithours", "decimal"),
            ("completed date", "string", "completeddate", "date"),
            ("filename", "string", "filename", "string")
        ],
        transformation_ctx="ApplyMapping_node2",
    )
    
    # Script generated for node PostgreSQL table
    PostgreSQLtable_node3 = glueContext.write_dynamic_frame.from_catalog(
        frame=ApplyMapping_node2,
        database="seiubg-rds-b2bds",
        table_name="b2bds_logs_traineestatuslogs",
        transformation_ctx="PostgreSQLtable_node3",
    )

if __name__ == "__main__": 
    main()
