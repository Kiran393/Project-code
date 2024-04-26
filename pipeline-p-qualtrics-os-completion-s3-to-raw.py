import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col,when,input_file_name,expr,to_date,regexp_extract,to_timestamp
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

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/qualtrics/O&SCompletion/'))
objects.sort(key=lambda o: o.last_modified)


print(objects[0].key)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# For Qualtrics, we always load the full file data foe every run, capturing the responseid of the pervious run to differentiate the old and new records . 
# isDelta flag  is marked as true for new responseid not existing in data store and false for otherwise 
agencypersonospreviousdf = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_os_qual_agency_person", transformation_ctx = "agencypersonospreviousdf")
agencypersonospreviousdf = agencypersonospreviousdf.toDF().select("responseid")
agencypersonospreviousdf.createOrReplaceTempView("agencypersonospreviousdf")

agencypersonosdf = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="agencypersonosdf",
)
# Capturing the processing file name, date and marking empty columns as null's 
agencypersonosdf = agencypersonosdf.toDF().withColumn("filename", input_file_name())
agencypersonosdf = agencypersonosdf.withColumn("filenamenew",regexp_extract(col('filename'), '(s3://)(.*)(/Inbound/raw/qualtrics/O&SCompletion/)(.*)', 4))
agencypersonosdf = agencypersonosdf.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filenamenew'), '(O&SCompletion)(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)', 2),"yyyyMMddHHmmss"))
agencypersonosdf=agencypersonosdf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in agencypersonosdf.columns])

dyf = DynamicFrame.fromDF(agencypersonosdf, glueContext, "dyf")

# Mapping of the filename columns to the target table coumns with its datatypes
applymapping1 = ApplyMapping.apply(frame = dyf, mappings = [("RecipientFirstName", "string", "recipientfirstname", "string"), ("Q_RecaptchaScore", "string", "q_recaptchascore", "string"), ("ExternalReference", "string", "externalreference", "string"), ("RecipientEmail", "string", "recipientemail", "string"), ("DistributionChannel", "string", "distributionchannel", "string"), ("Duration (in seconds)", "string", "duration_in_seconds", "string"), ("Q5_1", "string", "person_firstname", "string"), ("Q2_1", "string", "agency_firstname", "string"), ("Progress", "string", "progress", "string"), ("Status", "string", "status", "string"), ("EndDate", "string", "enddate", "string"), ("Q5_8", "string", "person_id", "string"),("filenamenew", "string", "filename", "string"),("filemodifieddate", "timestamp", "filemodifieddate", "timestamp"),("Q13", "string", "os_completion_date", "string"), ("ResponseId", "string", "responseid", "string"), ("Q5_3", "string", "person_dob", "string"), ("RecordedDate", "string", "recordeddate", "string"), ("Finished", "string", "finished", "string"), ("LocationLatitude", "string", "locationlatitude", "string"), ("LocationLongitude", "string", "locationlongitude", "string"), ("IPAddress", "string", "ipaddress", "string"), ("Q2_3", "string", "agency_email", "string"), ("StartDate", "string", "startdate", "string"), ("Q5_4", "string", "person_phone", "string"), ("UserLanguage", "string", "userlanguage", "string"), ("Q2_2", "string", "agency_lastname", "string"), ("Q5_2", "string", "person_lastname", "string"), ("RecipientLastName", "string", "recipientlastname", "string"), ("Q4", "string", "employername", "string")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["ipaddress", "recipientemail", "recipientfirstname", "startdate", "userlanguage", "person_firstname", "agency_lastname", "person_lastname", "agency_email", "externalreference", "responseid", "person_id", "agency_firstname", "person_phone", "finished", "locationlongitude", "os_completion_date", "locationlatitude", "q_recaptchascore", "enddate", "distributionchannel", "person_dob", "recipientlastname", "employername", "progress", "recordeddate","filename", "status", "duration_in_seconds","filemodifieddate","filedate"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_os_qual_agency_person", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")


agencypersonosdf_clean = resolvechoice4.toDF()
print("agencypersonosdf_clean count")
print(agencypersonosdf_clean.count())
agencypersonosdf_clean.show()

agencypersonosdf_clean.createOrReplaceTempView("agencypersonoscleandf")


spark.sql("select trim(responseid) from agencypersonospreviousdf").show()
spark.sql("select *,  true as isdelta from agencypersonoscleandf where trim(responseid) not in (select trim(responseid) from agencypersonospreviousdf)").show()
spark.sql("select *, false as isdelta from agencypersonoscleandf where trim(responseid) in (select trim(responseid) from agencypersonospreviousdf)").show()

# isDelta flag  is marked as true for new responseid not existing in data store and false for otherwise 
agencypersonosdf_clean = spark.sql("""
   select *, true as isdelta from agencypersonoscleandf where trim(responseid) not in (select trim(responseid) from agencypersonospreviousdf)
    UNION 
   select *, false as isdelta from agencypersonoscleandf where trim(responseid) in (select trim(responseid) from agencypersonospreviousdf)
    """)



mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
agencypersonosdf_clean.write.option("truncate",True).jdbc(url=url, table="raw.os_qual_agency_person", mode=mode, properties=properties)


sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()

job.commit()