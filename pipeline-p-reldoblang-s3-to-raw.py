import sys
from itertools import cycle
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import json
from awsglue.job import Job
from pyspark.sql.functions import sha2,md5,concat_ws,col,current_timestamp,input_file_name,expr
from pyspark.sql.functions import col,when,to_timestamp,regexp_extract,to_date
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as fun
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType,LongType,BinaryType
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

s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/reldob/'))
#df= list(objects.key.endswith('TXT'))
objects.sort(key=lambda o: o.last_modified)
print(objects)
print(objects[1].key)

'''for obj in bucket.objects.filter(Prefix='Inbound/raw/reldob/'):
    if obj.key.endswith('.TXT'):
        objects = list(bucket.objects.filter(Prefix='Inbound/raw/reldob/'))
        objects.sort(key=lambda o: o.last_modified)
        print(objects[0].key)'''



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "|",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://seiubg-b2bds-prod-feeds-fp7mk/"+objects[1].key+""],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

S3bucket_node1.toDF().show()
dataframe1 = S3bucket_node1.toDF()
if dataframe1.count() != 0:
    dataframe2 = dataframe1.withColumn("filename", input_file_name())
    dataframe3 = dataframe2.withColumn("filenamenew",expr("substring(filename, 55, length(filename))"))
    dataframe4 = dataframe3.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filenamenew'), '(01250_RelDOBLang_ADSAToTP_)(\d\d\d\d\d\d\d\d_\d\d\d\d\d\d)(.TXT)', 2),"yyyyMMdd_HHmmss"))
    df2=dataframe4.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in dataframe4.columns])
    dataframe4.select("filenamenew","filemodifieddate","filename").show(truncate=False)
    dyf = DynamicFrame.fromDF(df2, glueContext, "dyf")
    
    # Script generated for node ApplyMapping
    ApplyMapping_node2 = ApplyMapping.apply(
        frame=dyf,
        mappings=[
            ("provider id", "string", "provider_id", "int"),
            ("filenamenew", "string", "filename", "string"),
            ("filemodifieddate", "timestamp", "filemodifieddate", "timestamp"),
            ("student id", "string", "student_id", "long"),
            ("social security number", "string", "social_security_number", "int"),
            ("dob", "string", "dob", "string"),
            ("client relationship to ip", "string", "client_relationship_to_ip", "string"),
            ("authorization start date", "string", "authorization_start_date", "string"),
            ("authorization end date", "string", "authorization_end_date", "string"),
            ("preferred language", "string", "preferred_language", "string"),
            ("cumulative career hours", "string", "cumulative_career_hours", "decimal"),
            ("monthly hours worked", "string", "monthly_hours_worked", "decimal"),
            ("monthly hours worked month", "string", "monthly_hours_worked_month", "int"),
            ("last background check date", "string", "last_background_check_date", "string"),
            (
                "ip contract expiration date",
                "string",
                "ip_contract_expiration_date",
                "string",
            ),
            ("termination type", "string", "termination_type", "int"),
            ("termination date", "string", "termination_date", "string"),
        ],
        transformation_ctx="ApplyMapping_node2",
    )
    
    
    
    final_df = ApplyMapping_node2.toDF()
    print("final_df count")
    print(final_df.count())
    
    final_df.createOrReplaceTempView("Tab")
    
    spark.sql("select distinct filemodifieddate from Tab").show()
    
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
    properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
    final_df.write.option("truncate",True).jdbc(url=url, table="raw.reldoblang", mode=mode, properties=properties) 
    
    sourcekey = objects[1].key
    targetkey = objects[1].key.replace("/raw/", "/archive/")
    print(sourcekey)
    print(targetkey)
    copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
    bucket.copy(copy_source, targetkey)
    s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
else:
    sourcekey = objects[1].key
    targetkey = objects[1].key.replace("/raw/", "/archive/")
    print(sourcekey)
    print(targetkey)
    copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
    bucket.copy(copy_source, targetkey)
    s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()

job.commit()
