import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import boto3
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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


objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/doh/classified/'))
objects.sort(key=lambda o: o.last_modified)
print(objects)
#print(objects[1].key)

## @type: DataSource
## @args: [database = "doh_classified", table_name = "classified", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "doh_classified", table_name = "classified", transformation_ctx = "datasource0")
# Script generated for node S3 bucket



# doh_completed_Schema = StructType([ \
#     StructField("FileLastName",StringType(),True), \
#     StructField("FileFirstName",StringType(),True), \
#     StructField("FileReceivedDate",StringType(),True), \
#     StructField("DOHName", StringType(), True), \
#     StructField("Credential", StringType(), True), \
#     StructField("CredentialStatus", StringType(), True), \
#     StructField("ApplicationDate", StringType(), True), \
#     StructField("DateDSHSBenefitPaymentUDFUpdated", StringType(), True) \
#     ])
  
datasource0 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://seiubg-b2bds-prod-feeds-fp7mk/"+objects[0].key+""],
        "recurse": True,
    },
    transformation_ctx="datasource0",
)
new_df = datasource0.toDF()
#if new_df.count() != 0:
print("Schema:")
new_df.printSchema()
#new_df = spark.createDataFrame(new_df.rdd, doh_completed_Schema)
print("Schema change:")
new_df.printSchema()


new_df = new_df.withColumn("filename",input_file_name())
new_df.show()


new_df = new_df.coalesce(1).withColumn("Index",monotonically_increasing_id())
new_df = new_df.orderBy("Index")
new_df= new_df.filter(new_df.Index > 6)

new_df = new_df.withColumn("recordmodifieddate",current_timestamp())\
               .withColumn("filenamenew",expr("substring(filename, 69, length(filename))")).withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filename'), '( DSHS Benefit Classified File )(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)', 2),"yyyyMMddHHmmss"))\
               .withColumn("credentialnumber",translate(col("col4"),".","").substr(5,16))
#print("printing dataframe after filename substr")
#new_df.select("filename","credentialnumber").show(truncate = False)

print("count of df")
print(new_df.count())
df2=new_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in new_df.columns])

df2.show(10,truncate =False)
newdatasource0 = DynamicFrame.fromDF(df2, glueContext, "newdatasource0")

    
 #applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("col0", "string", "filefirstname", "string"), ("FileLastName", "string", "filelastname", "string"),("filemodifieddate", "timestamp", "filedate", "timestamp"), ("FileReceivedDate", "string", "filereceiveddate", "string"), ("DOHName", "string", "dohname", "string"), ("credentialnumber", "string", "credentialnumber", "string"), ("CredentialStatus", "string", "credentialstatus", "string"), ("ApplicationDate", "string", "applicationdate", "string"), ("DateDSHSBenefitPaymentUDFUpdated", "string", "datedshsbenefitpaymentudfupdated", "string"),("recordmodifieddate","timestamp","recordmodifieddate","timestamp"),("filenamenew","string","filename","string")], transformation_ctx = "applymapping1")

applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("col0", "string", "filelastname", "string"), ("col1", "string", "filefirstname", "string"), ("col2", "string", "filereceiveddate", "string"), ("col3", "string", "dohname", "string"), ("credentialnumber", "string", "credentialnumber", "string"), ("col5", "string", "credentialstatus", "string"), ("col6", "string", "applicationdate", "string"), ("col7", "string", "datedshsbenefitpaymentudfupdated", "string"),("filenamenew","string","filename","string"),("filemodifieddate", "timestamp", "filedate", "timestamp")], transformation_ctx = "applymapping1")

## @type: SelectFields
## @args: [paths = ["filefirstname", "datedshsbenefitpaymentudfupdated", "reconnotes", "filelastname", "dateuploaded", "reconciled", "credentialstatus", "filereceiveddate", "credentialnumber", "dohname", "applicationdate", "appfeepaid", "filename", "recordmodifieddate"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["filefirstname","filedate","datedshsbenefitpaymentudfupdated", "filelastname", "credentialstatus", "filereceiveddate", "credentialnumber", "dohname", "applicationdate", "filename"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_dohclassified", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_raw_dohclassified", transformation_ctx = "datasink5")
final_df = resolvechoice4.toDF()

final_df.show()
print("final_df count")
print(final_df.count())
final_df.select("filelastname").show()

mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
final_df.write.option("truncate",True).jdbc(url=url, table="raw.dohclassified", mode=mode, properties=properties)

sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
print("file is successfully archived")


job.commit()