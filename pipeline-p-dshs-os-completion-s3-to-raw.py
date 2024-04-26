import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col,when,input_file_name,expr,to_date,regexp_extract,to_timestamp
import boto3
import json

s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/qualtrics/DSHS_O&S_Completed/'))
objects.sort(key=lambda o: o.last_modified)


print(objects[0].key)


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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# For Qualtrics DSHS O&S Completions, we always load the full file data foe every run, capturing the responseid of the pervious run to differentiate the old and new records . 
# isDelta flag  is marked as true for new responseid not existing in data store and false for otherwise 
dshspersonospreviousdf = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_dshs_os_qual_agency_person", transformation_ctx = "dshspersonospreviousdf")
dshspersonospreviousdf = dshspersonospreviousdf.toDF().select("responseid")
dshspersonospreviousdf.createOrReplaceTempView("dshspersonospreviousdf")


# Script generated for node S3 bucket
dshspersonosdf = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://seiubg-b2bds-prod-feeds-fp7mk/"+objects[0].key+""
        ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)
dshspersonosdf = dshspersonosdf.toDF().withColumn("filename", input_file_name())
dshspersonosdf = dshspersonosdf.withColumn("filenamenew",regexp_extract(col('filename'), '(s3://)(.*)(/Inbound/raw/qualtrics/DSHS_O&S_Completed/)(.*)', 4))
dshspersonosdf = dshspersonosdf.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filenamenew'), '(DSHS_O&S_Completed)(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)', 2),"yyyyMMddHHmmss"))
dshspersonosdf = dshspersonosdf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in dshspersonosdf.columns])

dshspersonosdf = DynamicFrame.fromDF(dshspersonosdf, glueContext, "dyf")


# df2.show(20, truncate=0)
# print('df2 schema')
# df2.printSchema()

# Script generated for node ApplyMapping
dshspersonosdf = ApplyMapping.apply(
    frame=dshspersonosdf,
    mappings=[
        ("startdate", "string", "startdate", "string"),
        ("filenamenew","string","filename","string"),
        ("filemodifieddate","timestamp","filemodifieddate","timestamp"),
        ("enddate", "string", "enddate", "string"),
        ("status", "string", "status", "string"),
        ("ipaddress", "string", "ipaddress", "string"),
        ("progress", "string", "progress", "string"),
        ("duration (in seconds)", "string", "duration_in_seconds", "string"),
        ("finished", "string", "finished", "string"),
        ("recordeddate", "string", "recordeddate", "string"),
        ("responseid", "string", "responseid", "string"),
        ("recipientlastname", "string", "recipientlastname", "string"),
        ("recipientfirstname", "string", "recipientfirstname", "string"),
        ("recipientemail", "string", "recipientemail", "string"),
        ("externalreference", "string", "externalreference", "string"),
        ("locationlatitude", "string", "locationlatitude", "string"),
        ("locationlongitude", "string", "locationlongitude", "string"),
        ("distributionchannel", "string", "distributionchannel", "string"),
        ("userlanguage", "string", "userlanguage", "string"),
        ("q_recaptchascore", "string", "q_recaptchascore", "string"),
        ("q2_1", "string", "agency_firstname", "string"),
        ("q2_2", "string", "agency_lastname", "string"),
        ("q2_3", "string", "agency_email", "string"),
        ("q4", "string", "employername", "string"),
        ("q5_1", "string", "person_firstname", "string"),
        ("q5_2", "string", "person_lastname", "string"),
        ("q5_3", "string", "person_dob", "string"),
        ("q5_4", "string", "person_phone", "string"),
        ("q5_5", "string", "dshs_id", "string"),
        ("q13", "string", "os_completion_date", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node PostgreSQL table


dshspersonosdf_clean = dshspersonosdf.toDF()

dshspersonosdf_clean.createOrReplaceTempView("dshspersonoscleandf")

spark.sql("select trim(responseid) from dshspersonospreviousdf").show()
spark.sql("select *, true as isdelta from dshspersonoscleandf where trim(responseid) not in (select trim(responseid) from dshspersonospreviousdf)").show()
spark.sql("select *, false as isdelta from dshspersonoscleandf where trim(responseid) in (select trim(responseid) from dshspersonospreviousdf)").show()

dshspersonosdf_clean = spark.sql("""
   select *, true as isdelta from dshspersonoscleandf where trim(responseid) not in (select trim(responseid) from dshspersonospreviousdf)
    UNION 
   select *, false as isdelta from dshspersonoscleandf where trim(responseid) in (select trim(responseid) from dshspersonospreviousdf)
    """)



mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
dshspersonosdf_clean.write.option("truncate",True).jdbc(url=url, table="raw.dshs_os_qual_agency_person", mode=mode, properties=properties)


sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()

print('done.')

job.commit()
