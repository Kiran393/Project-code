import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import random
from awsglue.dynamicframe import DynamicFrame
import time
from pyspark.sql.functions import concat, col, lit, coalesce, year, current_date, date_format, to_date,when
from pyspark.sql.functions import input_file_name,expr,regexp_extract,to_timestamp,regexp_replace
import boto3
import re
import pyspark.sql.functions as F
from pyspark.sql.types import StringType,BooleanType,DateType
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

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/smartsheets/curated/'))
objects.sort(key=lambda o: o.last_modified)

print(objects[0].key)



args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

sscompletionspreviousdf = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_ss_course_completion", transformation_ctx = "sscompletionspreviousdf")
sscompletionspreviousdf = sscompletionspreviousdf.toDF().selectExpr("md5(concat_ws('',learner_id, first_name, last_name, phone_number, learner_email, attendance, class_id, class_title, date, duration,when_enrolled, instructor, sheet_name)) as sscompletionshashid")
sscompletionspreviousdf.createOrReplaceTempView("sscompletionspreviousdf")


curated = glueContext.create_dynamic_frame.from_options(
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
            "s3://seiubg-b2bds-prod-feeds-fp7mk/"+(objects[0].key)+""
        ],
        "recurse": True,
    },
    transformation_ctx="curated",
)

rawcompletions = curated.toDF().dropDuplicates(['learner id', 'class id']).withColumn("date", when(col("date") == "" , None).otherwise(col("date")))

rawcompletions = rawcompletions.withColumn("learner id",col("learner id").cast(StringType()))

numex = """^[0-9]+$"""
rawcompletions = rawcompletions.withColumn("learner id", when(col("learner id").rlike(numex), col("learner id")).otherwise(None))

rawcompletions = rawcompletions.withColumn("learner id", when( F.length("learner id") <= 12, col("learner id")).otherwise(None))

rawcompletions = rawcompletions.filter( col("learner id").isNotNull() & col("class id").isNotNull() & col("class title").isNotNull() & col("date").isNotNull() & col("attendance").isNotNull() )

# rawcompletions.where(col("learner id")=='273631768346').show()

rawcompletions = rawcompletions.withColumn("date",to_date(col("date"),"MM/dd/yy")).withColumn("when enrolled",to_date(col("when enrolled"),"MM/dd/yy"))

rawcompletions = rawcompletions.withColumn("instructor",regexp_replace(col("instructor"), "[']", "’"))

# rawcompletions.where(col("learner id")=='273631768346').show()

datasource0 = DynamicFrame.fromDF(rawcompletions, glueContext, "datasource0")


applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("attendance", "string", "attendance", "string"), ("class id", "string", "class_id", "string"), ("class title", "string", "class_title", "string"), ("date", "date", "date", "date"), ("duration", "double", "duration", "decimal(3,2)"), ("first name", "string", "first_name", "string"), ("instructor", "string", "instructor", "string"), ("last name", "string", "last_name", "string"), ("learner email", "string", "learner_email", "string"), ("learner id", "string", "learner_id", "string"), ("phone number", "string", "phone_number", "string"), ("sheet name", "string", "sheet_name", "string"), ("when enrolled", "date", "when_enrolled", "date")], transformation_ctx = "applymapping1")

print("newmappnig applied to source\n")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["date", "learner_id", "class_id", "last_name", "class_title", "duration", "learner_email", "instructor", "when_enrolled", "phone_number", "sheet_name", "first_name", "attendance"], transformation_ctx = "selectfields2")
print("select fields\n")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_ss_course_completion", transformation_ctx = "resolvechoice3")
print("select choices \n")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
print("apply choices \n")

#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_raw_ss_course_completion", transformation_ctx = "datasink5")

sscompletionsdf_clean = resolvechoice4.toDF()
# print("final_df count")
# print(final_df.count())

sscompletionsdf_clean.createOrReplaceTempView("sscompletionscleandf")

sscompletionsdf_clean = spark.sql(""" select *, md5(concat_ws('',learner_id, first_name, last_name, phone_number, learner_email, attendance, class_id, class_title, date, duration, 
        when_enrolled, instructor, sheet_name)) as sscompletionshashid from sscompletionscleandf """)

sscompletionsdf_clean.createOrReplaceTempView("sscompletionscleandf")

spark.sql("select * from sscompletionspreviousdf").show()

spark.sql("""
  select *, false as isdelta from sscompletionscleandf where trim(sscompletionshashid) in (select trim(sscompletionshashid) from sscompletionspreviousdf)
    """).show()
    
spark.sql("""
  select *, true as isdelta from sscompletionscleandf where trim(sscompletionshashid) not in (select trim(sscompletionshashid) from sscompletionspreviousdf)
    """).show()

sscompletionsdf_clean = spark.sql("""
  select *, true as isdelta from sscompletionscleandf where trim(sscompletionshashid) not in (select trim(sscompletionshashid) from sscompletionspreviousdf)
    UNION 
  select *, false as isdelta from sscompletionscleandf where trim(sscompletionshashid) in (select trim(sscompletionshashid) from sscompletionspreviousdf)
    """).drop("sscompletionshashid")


mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}

sscompletionsdf_clean.write.option("truncate",True).jdbc(url=url, table="raw.ss_course_completion", mode=mode, properties=properties)


sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()

print("dataframe write is complete\n")
job.commit()