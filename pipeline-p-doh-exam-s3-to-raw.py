import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import input_file_name,regexp_extract,col,to_timestamp,when
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
import json

client10 = boto3.client('secretsmanager')

response = client10.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)


database_secrets = json.loads(response['SecretString'])

B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_NAME = 'b2bds'

s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')

#s3://seiubg-b2bds-prod-feeds-fp7mk/Inbound/raw/exam/01250_TPExam_ADSAToTP_20220411_055213.TXT

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/exam/'))
objects.sort(key=lambda o: o.last_modified)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read data from S3 Bucket
examcdf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": "|", "optimizePerformance": True},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://seiubg-b2bds-prod-feeds-fp7mk/"+(objects[0].key)+""],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)
examdf = examcdf.toDF().withColumn("inputfilename",input_file_name())


# Capturing the processing file name, date and marking empty columns as null's 
examdf = examdf.withColumn("filename",regexp_extract(col('inputfilename'), '(s3://)(.*)(/Inbound/raw/exam/)(.*)', 4))
examdf = examdf.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filename'), '(01250_TPExam_ADSAToTP_)(\d\d\d\d\d\d\d\d_\d\d\d\d\d\d)(.TXT)', 2),"yyyyMMdd_HHmmss"))
examdf = examdf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in examdf.columns])


# ApplyMapping to match the target table
ApplyMapping_node =  DynamicFrame.fromDF(examdf, glueContext, "ApplyMapping_node")
ApplyMapping_node2 = ApplyMapping.apply(
    frame=ApplyMapping_node,
    mappings=[
        ("studentid", "string", "studentid", "string"),
        ("taxid", "string", "taxid", "string"),
        ("filename", "string", "filename", "string"),
        ("filemodifieddate", "timestamp", "filemodifieddate", "timestamp"),
        ("credentialnumber", "string", "credentialnumber", "string"),
        ("examdate", "string", "examdate", "string"),
        ("examstatus", "string", "examstatus", "string"),
        ("examtitlefromprometrics", "string", "examtitlefromprometrics", "string"),
        ("testlanguage", "string", "testlanguage", "string"),
        ("testsite", "string", "testsite", "string"),
        ("sitename", "string", "sitename", "string"),
        ("rolesandresponsibilitiesofthehomecareaide","string","rolesandresponsibilitiesofthehomecareaide","string"),
        ("supportingphysicalandpsychosocialwellbeing","string","supportingphysicalandpsychosocialwellbeing","string"),
        ("promotingsafety", "string", "promotingsafety", "string"),
        ("handwashingskillresult", "string", "handwashingskillresult", "string"),
        ("randomskill1result", "string", "randomskill1result", "string"),
        ("randomskill2result", "string", "randomskill2result", "string"),
        ("randomskill3result", "string", "randomskill3result", "string"),
        ("commoncarepracticesskillresult","string","commoncarepracticesskillresult","string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)
examdf = ApplyMapping_node2.toDF()

# Truncating and loading the processed data
mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
examdf.write.option("truncate",True).jdbc(url=url, table="raw.exam", mode=mode, properties=properties)


# Moving the processed file to archive location
sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()


job.commit()

