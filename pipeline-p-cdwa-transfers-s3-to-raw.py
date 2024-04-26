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


# Creating Temp View for person table in prod schema
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person"
).toDF().selectExpr("*").createOrReplaceTempView("person")

# Multiple files of the tranfers can be processed in same time
#s3://seiubg-b2bds-prod-feeds-fp7mk/Inbound/raw/cdwa/trainingtransfer/CDWA-O-BG-TrainingTrans-2022-09-06.csv
sourcepath = "s3://"+S3_BUCKET+"/Inbound/raw/cdwa/trainingtransfer/"

# Script for S3 bucket to read file in format CDWA-O-BG-TrainingTrans-_XX.csv
cdwatransfersdf = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ",","inferSchema": False},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [sourcepath],
        "recurse": True,
    }
).toDF()


if cdwatransfersdf.count() != 0:


    # Capturing the processing file name, date 
    cdwatransfersdf = cdwatransfersdf.withColumn("inputfilename", input_file_name())\
                            .withColumn("filename", regexp_extract(col('inputfilename'), '(s3://)(.*)(/Inbound/raw/cdwa/trainingtransfer/)(.*)', 4))\
                            .withColumn("filemodifieddate",to_date(regexp_extract(col('filename'), '(CDWA-O-BG-TrainingTrans-)(\d\d\d\d-\d\d-\d\d)(.csv)', 2),"yyyy-MM-dd"))
    
    # marking empty columns as null's
    cdwatransfersdf = cdwatransfersdf.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in cdwatransfersdf.columns])
    
    # Converting columns into appropriate type posible and renaming
    cdwatransfersdf = cdwatransfersdf.withColumn("completed_date", to_date(col("Completed Date"),"yyyyMMdd"))\
                            .withColumn("created_date", to_date(col("CreatedDate"),"yyyyMMdd"))\
                            .withColumnRenamed("Person ID","personid")\
    
    # Creating Temp View for Transfers
    cdwatransfersdf.createOrReplaceTempView("transfers")
    
    # Filtering out the CDWAID which are mapped to a personid in prod person table
    transfers_noerror = spark.sql( "select tt.*, \
     p.personid as personid_new \
    from transfers tt \
    left join (select distinct personid, cdwaid from person) p \
    on tt.personid = p.cdwaid \
    where p.personid is not null" )
    print("transfers_noerror")
    transfers_noerror.show()
    
    # Filtering out the CDWAID which cannot be mapped to a personid in prod person table
    transfers_error = spark.sql( "select tt.*, \
     p.personid as personid_new, \
     'CDWAID cannot be mapped to a personid' as error_msg, \
     'false' as isvalid \
    from transfers tt \
    left join (select distinct personid, cdwaid from person) p \
    on tt.personid = p.cdwaid \
    where p.personid is null" )
    print("transfers_error")
    transfers_error.show()
    
    
    transfers_errors_datacatalogtable = DynamicFrame.fromDF(transfers_error, glueContext, "transfers_errors_datacatalogtable")
    
    # Filtering for O&S Transfers from all transfers 
    cdwaoandstransfers =  transfers_noerror.filter((col("training program") == "Orientation & Safety") | (col("training program") == "Orientation & Safety Attest"))
    cdwatransfers_oands_datacatalog = DynamicFrame.fromDF(cdwaoandstransfers, glueContext, "cdwatransfers_oands_datacatalog")
    
    # ApplyMapping for O and S Transfers
    oands_mapping = ApplyMapping.apply(
        frame=cdwatransfers_oands_datacatalog,
        mappings=[
            ("employee id", "string", "employeeid", "bigint"),
            ("personid_new", "bigint", "personid", "bigint"),
            ("training program", "string", "trainingprogram", "string"),
            ("class name", "string", "classname", "string"),
            ("dshs course code", "string", "dshscoursecode", "string"),
            ("credit hours", "string", "credithours", "string"),
            ("completed_date", "date", "completeddate", "date"),
            ("training entity", "string", "trainingentity", "string"),
            ("reason for transfer", "string", "reasonfortransfer", "string"),
            ("employerstaff", "string", "employerstaff", "string"),
            ("created_date", "date", "createddate", "date"),
            ("filename", "string", "filename", "string"),
            ("filemodifieddate", "date", "filedate", "date")
        ],
        transformation_ctx="oands_mapping",
    )
    
    # Coverting glue dynamic dataframe to spark dataframe
    oandstransfersfinaldf = oands_mapping.toDF()
    
    # Truncating and loading the processed data
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_DBNAME
    properties = {"user": B2B_USER, "password": B2B_PASSWORD,
                  "driver": "org.postgresql.Driver"}
    oandstransfersfinaldf.write.option("truncate", True).jdbc(
        url=url, table="raw.cdwaoandstransfers", mode=mode, properties=properties)
    
    
    
    # Filtering for except O&S Transfers from all transfers 
    cdwaothertransfers =  transfers_noerror.filter((col("training program") != "Orientation & Safety") & (col("training program") != "Orientation & Safety Attest"))
    
    cdwatransfers_btandce_datacatalog = DynamicFrame.fromDF(cdwaothertransfers, glueContext, "cdwatransfers_btandce_datacatalog")
    
    # Script Apply Mapping for BT & CE Transfers
    btandce_mapping = ApplyMapping.apply(
        frame=cdwatransfers_btandce_datacatalog,
        mappings=[
            ("Employee Id", "string", "employeeid", "bigint"),
            ("personid_new", "bigint", "personid", "bigint"),
            ("training program", "string", "trainingprogram", "string"),
            ("Class Name", "string", "classname", "string"),
            ("DSHS Course Code", "string", "dshscoursecode", "string"),
            ("Credit Hours", "string", "credithours", "string"),
            ("completed_date", "date", "completeddate", "date"),
            ("Training Entity", "string", "trainingentity", "string"),
            ("Reason for Transfer", "string", "reasonfortransfer", "string"),
            ("EmployerStaff", "string", "employerstaff", "string"),
            ("created_date", "date", "createddate", "date"),
            ("filename", "string", "filename", "string"),
            ("filemodifieddate", "date", "filedate", "date")
        ],
        transformation_ctx="btandce_mapping",
    )
    
    # Coverting glue dynamic dataframe to spark dataframe
    btandcetransfersfinaldf = btandce_mapping.toDF()
    
    # Truncating and loading the processed data
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_DBNAME
    properties = {"user": B2B_USER, "password": B2B_PASSWORD,
                  "driver": "org.postgresql.Driver"}
    btandcetransfersfinaldf.write.option("truncate", True).jdbc(
        url=url, table="raw.cdwatrainingtransfers", mode=mode, properties=properties)
    
    
    # Apply Mapping for Transfers Errors  
    transfers_error_mapping = ApplyMapping.apply(
        frame=transfers_errors_datacatalogtable,
        mappings=[
            ("employee id", "string", "employeeid", "bigint"),
            ("personid", "string", "personid", "bigint"),
            ("training program", "string", "trainingprogram", "string"),
            ("class name", "string", "classname", "string"),
            ("dshs course code", "string", "dshscoursecode", "string"),
            ("credit hours", "string", "credithours", "string"),
            ("completed_date", "date", "completeddate", "date"),
            ("training entity", "string", "trainingentity", "string"),
            ("reason for transfer", "string", "reasonfortransfer", "string"),
            ("employerstaff", "string", "employerstaff", "string"),
            ("created_date", "date", "createddate", "date"),
            ("filemodifieddate", "date", "filedate", "date"),
            ("filename", "string", "filename", "string"),
            ("isvalid", "string", "isvalid", "boolean"),
            ("error_msg", "string", "error_message", "string")
        ],
        transformation_ctx="transfers_error_mapping",
    )
    
    # Appending all error to training transfers logs
    glueContext.write_dynamic_frame.from_catalog(
        frame=transfers_error_mapping,
        database="seiubg-rds-b2bds",
        table_name="b2bds_logs_trainingtransferslogs",
    )
    
    
    #Archiving Processed files
    #If we have Multiple files of the tranfers, then one file at time is moved to archive location
    for object_summary in s3bucket.objects.filter(Prefix="Inbound/raw/cdwa/trainingtransfer/"):
          if object_summary.key.endswith('csv'):
            filename = object_summary.key
            sourcekey = filename
            targetkey = sourcekey.replace("/raw/", "/archive/")
            copy_source = {'Bucket': S3_BUCKET, 'Key': sourcekey}
            s3bucket.copy(copy_source, targetkey)
            s3resource.Object(""+S3_BUCKET+"", sourcekey).delete()
            
else:
     for object_summary in s3bucket.objects.filter(Prefix="Inbound/raw/cdwa/trainingtransfer/"):
          if object_summary.key.endswith('csv'):
            filename = object_summary.key
            sourcekey = filename
            targetkey = sourcekey.replace("/raw/", "/archive/")
            copy_source = {'Bucket': S3_BUCKET, 'Key': sourcekey}
            s3bucket.copy(copy_source, targetkey)
            s3resource.Object(""+S3_BUCKET+"", sourcekey).delete()

job.commit()
