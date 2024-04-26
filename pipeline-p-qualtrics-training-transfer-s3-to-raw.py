import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_date, when, regexp_extract, col

import boto3
import json

# Accessing the Secrets Manager from boto3 lib
secretsmangerclient = boto3.client('secretsmanager')


# Accessing the secrets value for S3 Bucket
s3response = secretsmangerclient.get_secret_value(
    SecretId='prod/b2bds/s3'
)
s3_secrets = json.loads(s3response['SecretString'])
S3_BUCKET = s3_secrets['datafeeds']

# Accessing the secrets target database
databaseresponse = secretsmangerclient.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)
database_secrets = json.loads(databaseresponse['SecretString'])
B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_DBNAME = 'b2bds'  # database_secrets['dbname']

# Reading files from s3 and sorting it by last modified date to read only the earliest file
s3resource = boto3.resource('s3')
s3bucket = s3resource.Bucket(''+S3_BUCKET+'')

# Default JOB arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Multiple files of the tranfers can be processed in same time
sourcepath = "s3://"+S3_BUCKET+"/Inbound/raw/qualtrics/transfers/"

# Script generated for node S3 bucket
qualtricstransfers = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [sourcepath],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
).toDF()


glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_qualtricstrainingtransfers"
).toDF().selectExpr("*").createOrReplaceTempView("qualtricstrainingtransfersprevious")


#filtering records nd extracting filename

qualtricstransfers = qualtricstransfers.withColumn("inputfilename", input_file_name())\
                                    .withColumn("filename", regexp_extract(col('inputfilename'), '(s3://)(.*)(/Inbound/raw/qualtrics/transfers/)(.*)', 4))\
                                    .withColumn("filemodifieddate",to_date(regexp_extract(col('filename'), '(Qualtricstrainingtransfers)(\d\d\d\d-\d\d-\d\d)(.csv)', 2),"yyyy-MM-dd"))

# marking empty columns as null's
qualtricstransfers = qualtricstransfers.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in qualtricstransfers.columns])


qualtricstransfers.createOrReplaceTempView("qualtricstransfers")

qualtricstransferswithdelta = spark.sql("""
  select *, true as isdelta from qualtricstransfers where trim(responseid) not in (select trim(responseid) from qualtricstrainingtransfersprevious)
    UNION 
  select *, false as isdelta from qualtricstransfers where trim(responseid) in (select trim(responseid) from qualtricstrainingtransfersprevious)
    """)
    
    
qualtricstransferscatalog = DynamicFrame.fromDF(qualtricstransferswithdelta, glueContext, "qualtricstransferscatalog")



#Mappping columns
# Script generated for node ApplyMapping
qualtricstransfers_applymapping = ApplyMapping.apply(
    frame=qualtricstransferscatalog,
    mappings=[
        ("StartDate", "string", "startdate", "timestamp"),
        ("EndDate", "string", "enddate", "timestamp"),
        ("Status", "string", "status", "string"),
        ("IPAddress", "string", "ipaddress", "string"),
        ("Progress", "string", "progress", "string"),
        ("`Duration (in seconds)`", "string", "duration_in_seconds", "string"),
        ("Finished", "string", "finished", "string"),
        ("RecordedDate", "string", "recordedDate", "timestamp"),
        ("ResponseId", "string", "responseid", "string"),
        ("RecipientLastName", "string", "recipientlastName", "string"),
        ("RecipientFirstName", "string", "recipientfirstName", "string"),
        ("RecipientEmail", "string", "recipientemail", "string"),
        ("ExternalReference", "string", "externalreference", "string"),
        ("LocationLatitude", "string", "locationlatitude", "string"),
        ("LocationLongitude", "string", "locationlongitude", "string"),
        ("UserLanguage", "string", "userlanguage", "string"),
        ("DistributionChannel", "string", "distributionchannel", "string"),
        ("id", "string", "personid", "bigint"),
        ("vertical", "string", "course_name", "string"),
        ("reason", "string", "reasonfortransfer", "string"),
        ("ce_1", "string", "continuing_education_information_1", "string"),
        ("ce_2", "string", "continuing_education_information_2", "string"),
        ("completion_date", "string", "completed_date" , "string"),
        ("entity_retired_0830_1", "string", "trainingentity_retired", "string"),
        ("entity2_1", "string", "trainingentity", "string"),
        ("hours", "string", "credithours", "string"),
        ("employer", "string", "employer", "string"),
        ("staff", "string", "employerstaff", "string"),
        ("non_seiu", "string", "non_seiu", "string"),
        ("comments", "string", "additional_comments", "string"),
        ("filename", "string", "filename", "string"),
        ("form_completed_date", "string", "form_completed_date", "string"),
        ("isdelta", "boolean", "isdelta", "boolean"),
        ("filemodifieddate", "date", "filemodifieddate", "timestamp")
        
    ],
    transformation_ctx="qualtricstransfers_applymapping",
)

# Coverting glue dynamic dataframe to spark dataframe
oandstransfersfinaldf = qualtricstransfers_applymapping.toDF()

# Truncating and loading the processed data
mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_DBNAME
properties = {"user": B2B_USER, "password": B2B_PASSWORD,
              "driver": "org.postgresql.Driver"}
oandstransfersfinaldf.write.option("truncate", True).jdbc(
    url=url, table="raw.qualtricstrainingtransfers", mode=mode, properties=properties)


#Archiving Processed files
#If we have Multiple files of the tranfers, then one file at time is moved to archive location
for object_summary in s3bucket.objects.filter(Prefix="Inbound/raw/qualtrics/transfers/"):
      if object_summary.key.endswith('csv'):
        filename = object_summary.key
        sourcekey = filename
        targetkey = sourcekey.replace("/raw/", "/archive/")
        copy_source = {'Bucket': S3_BUCKET, 'Key': sourcekey}
        s3bucket.copy(copy_source, targetkey)
        s3resource.Object(""+S3_BUCKET+"", sourcekey).delete()

job.commit()
