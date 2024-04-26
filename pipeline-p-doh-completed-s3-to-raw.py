import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import functions as fun
from pyspark.sql.types import StructType,StructField, StringType
from awsglue.dynamicframe import DynamicFrame
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

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/doh/completed/'))
objects.sort(key=lambda o: o.last_modified)


print(objects[0].key)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "doh_classified", table_name = "classified", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "doh_completed", table_name = "completed", transformation_ctx = "datasource0")
#Create Schema

doh_completed_Schema = StructType([ \
    StructField("FileLastName",StringType(),True), \
    StructField("FileFirstName",StringType(),True), \
    StructField("FileReceivedDate",StringType(),True), \
    StructField("DOHName", StringType(), True), \
    StructField("Credential", StringType(), True), \
    StructField("CredentialStatus", StringType(), True), \
    StructField("ApplicationDate", StringType(), True), \
    StructField("DateApprovedInstructorCodeandNameUDFUpdated", StringType(), True), \
    StructField("ApprovedInstructorCodeandName", StringType(), True), \
    StructField("DateGraduatedfor70Hours", StringType(), True) \
    ])
  
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
        "paths": [
            "s3://seiubg-b2bds-prod-feeds-fp7mk/"+objects[0].key+""
        ],
        "recurse": True,
    },
    transformation_ctx="datasource0",
)

new_df = datasource0.toDF()
#if new_df.count() != 0:
#new_df = datasource0.toDF().withColumn("filename",input_file_name())
##
print("Schema:")
new_df.printSchema()
new_df.show(truncate = False)
new_df = spark.createDataFrame(new_df.rdd, doh_completed_Schema)

print("Schema change:")
new_df.printSchema()
new_df.show(truncate = False)
new_df = new_df.withColumn("filename",input_file_name())

#commenting this to debug
new_df = new_df.coalesce(1).withColumn("Index",monotonically_increasing_id())
new_df = new_df.orderBy("Index")

#converting to pandas to remove the top 6 rows in the df3
#pandas_df = new_df.toPandas()
#pandas_df.iloc[7:]
#print("Printin Pandas df")
#pandas_df.head()
#Convert to spark df
#new_df = spark.createDataFrame(pandas_df) 
print("After new_df orderby")
new_df.show(10,truncate =False)
    
new_df= new_df.filter(new_df.Index > 6)
    
#if new_df.count() != 0:
print("After filter:")
new_df.show(10,truncate =False)

new_df = new_df.withColumn("recordmodifieddate",current_timestamp())\
               .withColumn("filename",expr("substring(filename, 68, length(filename))"))\
               .withColumn("credentialnumber",translate(col("Credential"),".","").substr(5,16))
#df3 = df3.withColumn("Client ID", regexp_replace("Client ID",'/[\r\n\x0B\x0C\u0085\u2028\u2029]+/g',''))

new_df = new_df.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filename'), '(DSHS Benefit Training Completed File )(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)', 2),"yyyyMMddHHmmss"))
#dataframe4 = dataframe3.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filenamenew'), '(01250_RelDOBLang_ADSAToTP_)(\d\d\d\d\d\d\d\d_\d\d\d\d\d\d)(.TXT)', 2),"yyyyMMdd_HHmmss"))
###
print("Schema after filter:")
new_df.printSchema()
print("printing dataframe after filename substr")
new_df.select("filename","filemodifieddate").show(truncate = False)
new_df.show(10,truncate =False)

print("count of df")
print(new_df.count())
df2=new_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in new_df.columns])
for colname in df2.columns:
  df3 = df2.withColumn(colname, fun.trim(fun.col(colname)))
newdatasource0 = DynamicFrame.fromDF(df3, glueContext, "newdatasource0")
## @type: ApplyMapping
## @args: [mapping = [("col0", "string", "reconciled", "string"), ("col1", "string", "dohname", "string"), ("col2", "string", "reconnotes", "string"), ("col3", "string", "applicationdate", "string"), ("col4", "string", "credentialnumber", "string"), ("col5", "string", "credentialstatus", "string"), ("col6", "string", "filename", "string"), ("col7", "string", "filelastname", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("FileLastName", "string", "filelastname", "string"),("filemodifieddate", "string", "filedate", "timestamp"), ("FileFirstName", "string", "filefirstname", "string"), ("FileReceivedDate", "string", "filereceiveddate", "string"), ("DOHName", "string", "dohname", "string"), ("credentialnumber", "string", "credentialnumber", "string"),("CredentialStatus","string","credentialstatus","string"), ("ApplicationDate", "string", "applicationdate", "string"), ("DateApprovedInstructorCodeandNameUDFUpdated", "string", "dateapprovedinstructorcodeandnameudfupdated", "string"), ("ApprovedInstructorCodeandName", "string", "approvedinstructorcodeandname", "string"),("DateGraduatedfor70Hours","string","dategraduatedfor70hours","string"),("recordmodifieddate","timestamp","recordmodifieddate","timestamp"),("filename","string","filename","string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["filefirstname", "datedshsbenefitpaymentudfupdated", "reconnotes", "filelastname", "dateuploaded", "reconciled", "credentialstatus", "filereceiveddate", "credentialnumber", "dohname", "applicationdate", "appfeepaid", "filename", "recordmodifieddate"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["filefirstname","filedate","credentialstatus", "datedshsbenefitpaymentudfupdated", "filelastname", "filereceiveddate", "credentialnumber", "dohname", "applicationdate", "dateapprovedinstructorcodeandnameudfupdated","approvedinstructorcodeandname","dategraduatedfor70hours", "filename", "recordmodifieddate"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "postgresrds", table_name = "b2bdevdb_raw_dohclassified", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_dohcompleted", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "postgresrds", table_name = "b2bdevdb_raw_dohclassified", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_raw_dohcompleted", transformation_ctx = "datasink5")
final_df = resolvechoice4.toDF()
print("final_df count")
print(final_df.count())
#final_df.select("filefirstname").show()
mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
final_df.write.option("truncate",True).jdbc(url=url, table="raw.dohcompleted", mode=mode, properties=properties)


sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()

    
job.commit()