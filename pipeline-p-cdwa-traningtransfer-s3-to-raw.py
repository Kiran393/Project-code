import sys
from awsglue.transforms import *
import numpy as np
import pandas as pd
import random
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name,to_date,regexp_extract
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import substring, expr, length,col, when
from pyspark.sql.functions import regexp_replace
import boto3
import json


client10 = boto3.client('secretsmanager')

response1 = client10.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)


database_secrets = json.loads(response1['SecretString'])

B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_NAME = 'b2bds'
s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')


objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/cdwa/trainingtransfer/'))
objects.sort(key=lambda o: o.last_modified)
print(objects)
print(objects[0].key)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket

dataframe1 = spark.read.format("csv").option("header","true").option("inferSchema","False").load("s3://seiubg-b2bds-prod-feeds-fp7mk/"+(objects[0].key)+"")
dataframe1.printSchema()
dataframe1.show()
dataframe1 = dataframe1.withColumn("filename", input_file_name())
dataframe1 = dataframe1.withColumn("filenamenew",expr("substring(filename, 69, length(filename))"))
df2=dataframe1.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in dataframe1.columns])
df2 = df2.withColumn("filemodifieddate",to_date(regexp_extract(col('filenamenew'), '(CDWA-O-BG-TrainingTrans-)(\d\d\d\d-\d\d-\d\d)(.csv)', 2),"yyyy-MM-dd"))
new_cols=(column.replace('"', '').strip() for column in df2.columns)
df3 = df2.toDF(*new_cols)
df3.show(truncate = False)
df3 = df3.withColumn("CreatedDatev", regexp_replace("CreatedDate",'"',''))
df3.select("CreatedDatev").show()

S3bucket_node2 = DynamicFrame.fromDF(df3, glueContext, "S3bucket_node2")
# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node2,
    mappings=[
        ("Employee Id", "string", "employeeid", "string"),
        ("Person ID", "string", "PersonID", "string"),
        ("training program", "string", "trainingprogram", "string"),
        ("Class Name", "string", "ClassName", "string"),
        ("DSHS Course Code", "string", "DSHSCourseCode", "string"),
        ("Credit Hours", "string", "CreditHours", "string"),
        ("Completed Date", "string", "CompletedDate", "string"),
        ("Training Entity", "string", "TrainingEntity", "string"),
        ("Reason for Transfer", "string", "ReasonforTransfer", "string"),
        ("EmployerStaff", "string", "EmployerStaff", "string"),
        ("CreatedDatev", "string", "CreatedDate", "string"),
        ("filenamenew", "string", "filename", "string"),
        ("filemodifieddate", "date", "filemodifieddate", "date")
    ],
    transformation_ctx="ApplyMapping_node2",
)
print("troublshooting")
ApplyMapping_node2.toDF().show()
ApplyMapping_node2.toDF().printSchema()
# Script generated for node PostgreSQL table


final_df = ApplyMapping_node2.toDF()

final_df.show()
print("final_df count")
print(final_df.count())
#final_df.select("filename").show()
final_df.createOrReplaceTempView("Tab")

spark.sql("select distinct filemodifieddate from Tab").show()

mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
final_df.write.option("truncate",True).jdbc(url=url, table="raw.trainingtransfers", mode=mode, properties=properties)

sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
print("file is successfully archived")

job.commit()

