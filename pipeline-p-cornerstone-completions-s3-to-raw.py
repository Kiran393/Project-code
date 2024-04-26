import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import boto3
import json
import pandas as pd

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

# Multiple files of the completions can be processed in same time
sourcepath = "s3://"+S3_BUCKET+"/Inbound/raw/cornerstone/"

'''
##"s3://seiu-dev-b2b-datafeed/Inbound/raw/cornerstone/Cornerstone RAW file.csv"
# Script for S3 bucket to read file in format Cornerstone RAW file.csv
rawcornerstonecompletions = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="rawcornerstonecompletions",
).toDF()
'''


rawcornerstonecompletions = spark.read.format("csv").option("header","true").option("inferSchema","false").option("escape","\"").load(f"{sourcepath}")

#Removing the duplicates based on learner id and class id (course name) and marking eny blank values as nulls
rawcornerstonecompletions = rawcornerstonecompletions.dropDuplicates(['BG Person ID', 'Course Title'])

# marking empty columns as null's
rawcornerstonecompletions = rawcornerstonecompletions.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in rawcornerstonecompletions.columns])

# Check if the personid is has only digits, if not making them as null
rawcornerstonecompletions = rawcornerstonecompletions.withColumn("BG Person ID", when(col("BG Person ID").rlike("""^[0-9]+$"""), col("BG Person ID")).otherwise(None))

#rawcornerstonecompletions.show()
rawcornerstonecompletions.printSchema()


# Filtering the rows feilds where combination BG Person ID, Course Title,Completed Date is not empty
rawcornerstonecompletions = rawcornerstonecompletions.filter( col("BG Person ID").isNotNull()  & col("Course Title").isNotNull() & col("Completion Date").isNotNull() ).filter(col("Status") == "Passed")

#converting spark df to pandas
rawcornerstonecompletionsdf = rawcornerstonecompletions.toPandas()

#converting string to datetime
rawcornerstonecompletionsdf['Completed Date']=pd.to_datetime(rawcornerstonecompletionsdf['Completed Date'])
rawcornerstonecompletionsdf['Registration Date']=pd.to_datetime(rawcornerstonecompletionsdf['Registration Date'])
rawcornerstonecompletionsdf['Last Active Date']=pd.to_datetime(rawcornerstonecompletionsdf['Last Active Date'])
rawcornerstonecompletionsdf['Completion Date']=pd.to_datetime(rawcornerstonecompletionsdf['Completion Date'])
rawcornerstonecompletionsdf['Begin Date']=pd.to_datetime(rawcornerstonecompletionsdf['Begin Date'])



rawcornerstonecompletionsdf = rawcornerstonecompletionsdf.dropna(axis='columns', how='all')
rawcornerstonecompletions_df = spark.createDataFrame(rawcornerstonecompletionsdf)



rawcornerstonecompletions_df.show()


rawcornerstonecompletions_catalog = DynamicFrame.fromDF(rawcornerstonecompletions_df, glueContext, "rawcornerstonecompletions_catalog")
    
    
    
# Script Apply Mapping for Cornerstone completions as per target table
rawcornerstonecompletions_targetmapping = ApplyMapping.apply(
    frame=rawcornerstonecompletions_catalog,
    mappings=[
        #("Offering ID", "string", "offering_id", "string"),
        ("Offering Title", "string", "offering_title", "string"),
        #("Offering Summary", "string", "offering_summary", "string"),
        #("Offering Credits", "string", "offering_credits", "decimal(3,2)"),
        #("Course ID", "string", "course_id", "string"),
        ("Course Title", "string", "course_title", "string"),
        #("Instructor Name", "string", "instructor_name", "string"),
       # ("User ID", "string", "user_id", "string"),
        ("Student First Name", "string", "user_first_name", "string"),
        ("Student Last Name", "string", "user_last_name", "string"),
        ("Student Email", "string", "user_email", "string"),
        #("User Tags", "string", "user_tags", "string"),
        #("Teams", "string", "teams", "string"),
       # ("User Created Date", "timestamp", "user_created_date", "timestamp"),
        ("Last Active Date", "timestamp", "last_login_date", "timestamp"),
        #("License Agreement Date", "timestamp", "license_agreement_date", "timestamp"),
       # ("DSHS Approval Code", "string", "dshs_approval_code", "string"),
        ("Course Type", "string", "course_type", "string"),
        ("BG Person ID", "choice", "bg_person_id", "choice"),
        ("Date Of Birth", "date", "date_of_birth", "date"),
        ("Phone Number", "string", "phone_number", "string"),
        ("Street 1", "string", "street_1", "string"),
        ("Street 2", "string", "street_2", "string"),
        ("City", "string", "city", "string"),
        ("State/Province", "string", "state", "string"),
        ("Postal Code", "string", "postal_code", "string"),
       # ("Registration ID", "string", "registration_id", "string"),
        ("Registration Date", "timestamp", "registered_date", "timestamp"),
        ("Begin Date", "timestamp", "begin_date", "timestamp"),
        ("Completed Date", "timestamp", "completed_date", "timestamp"),
        ("Grade", "string", "grade_percentage", "string"),
        #("Ejected", "string", "ejected", "string"),
        #("Passed", "string", "passed", "string"),
        ("Status", "string", "status", "string"),
       # ("Transaction ID", "string", "transaction_id", "string"),
       # ("Affiliate Codes", "string", "affiliate_codes", "string"),
       # ("Applied Discount Codes", "string", "applied_discount_codes", "string"),
       ("Completion Percentage", "string", "completion_percentage", "string"),
       ("Completion Date", "timestamp", "completion_date", "timestamp")
    ],
    transformation_ctx="rawcornerstonecompletions_targetmapping",
    
)


# Coverting glue dynamic dataframe to spark dataframe
rawcornerstonecompletionsfinaldf = rawcornerstonecompletions_targetmapping.toDF()
#rawcornerstonecompletionsfinaldf.filter(col('user_first_name') == "RAELEENE").show()
rawcornerstonecompletionsfinaldf.printSchema()



# Truncating and loading the processed data
mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_DBNAME
properties = {"user": B2B_USER, "password": B2B_PASSWORD,
              "driver": "org.postgresql.Driver"}
rawcornerstonecompletionsfinaldf.write.option("truncate", True).jdbc(
    url=url, table="raw.cornerstone_completion", mode=mode, properties=properties)


#Archiving Processed files
#If we have Multiple files of the completions, then one file at time is moved to archive location
for object_summary in s3bucket.objects.filter(Prefix="Inbound/raw/cornerstone/"):
      if object_summary.key.endswith('csv'):
        filename = object_summary.key
        sourcekey = filename
        targetkey = sourcekey.replace("/raw/", "/archive/")
        copy_source = {'Bucket': S3_BUCKET, 'Key': sourcekey}
        s3bucket.copy(copy_source, targetkey)
        s3resource.Object(""+S3_BUCKET+"", sourcekey).delete()



job.commit()
