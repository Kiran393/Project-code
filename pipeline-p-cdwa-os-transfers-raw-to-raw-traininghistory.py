import sys
import boto3
import pandas
from datetime import date
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#Importing tables and creating views

#Creating Temp View for cdwaoandstransfers table in prod schema														  
glueContext.create_dynamic_frame.from_catalog(
    database = "seiubg-rds-b2bds", 
    table_name = "b2bds_raw_cdwaoandstransfers"
    ).toDF().filter("isvalid == 'true'")\
    .createOrReplaceTempView("rawcdwaoandstransfers")

#Creating Temp View for person table in prod schema																																									   
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person"
).toDF().selectExpr("*").createOrReplaceTempView("person")

#Creating Temp View for trainingrequirement table in prod schema
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_trainingrequirement"
).toDF().selectExpr("*").createOrReplaceTempView("trainingrequirement")


#within the cdwaoandstransfers table on personid,trainingprogram  with earliest completed date.
spark.sql("""select *,
    'Orientation & Safety EN' as coursename ,
    5 as credithours ,
    '1000065ONLEN02' as courseid ,
    100 as trainingprogramcode ,
    'Orientation & Safety' as trainingprogramtype ,
    'EN' as courselanguage,
    'ONL' as coursetype,
    'CDWA' as trainingsource,
    'instructor' as instructorname,
    'instructor' as instructorid, ROW_NUMBER() OVER ( PARTITION BY personid, trainingprogram ORDER BY completeddate ASC ) completionsrank 
    from rawcdwaoandstransfers where isvalid = 'true' """).createOrReplaceTempView("cte_rawcdwaoandstransfers")
spark.sql("select * from cte_rawcdwaoandstransfers").show()


#Creating temp view by joining person table and training requirement table
spark.sql("""select distinct p.personid,p.cdwaid,t.status from person p join trainingrequirement t 
on p.personid = t.personid where t.trainingprogramcode= '100' and t.status = 'active' and p.cdwaid is NOT NULL  """).createOrReplaceTempView("cte_trainingrequirement")


#Filtering unique records to injest into training history table
traininghistory = spark.sql("""select * from cte_rawcdwaoandstransfers os left join cte_trainingrequirement tr on os.personid=tr.personid 
WHERE os.completionsrank = 1 and tr.personid is not null """)

#Filtering duplicate records to injest into training history log table
traininghistorylog = spark.sql("""select * from cte_rawcdwaoandstransfers os left join cte_trainingrequirement tr on os.personid=tr.personid 
WHERE os.completionsrank > 1 or tr.personid is null""") 
 

#creating dynaicframes from the dataframes
OnStransfers = DynamicFrame.fromDF(traininghistory, glueContext, "OnStransfers") 
traininglogs = DynamicFrame.fromDF(traininghistorylog, glueContext, "traininglogs")	
OnStransfers.show()
#Creating file and posting to S3 store the error out records 
today = date.today()
suffix = today.strftime("%Y-%m-%d")
filename = "Outbound/cdwa/errorlog/CDWA-O-BG-TrainingTrans-errorfile-"+suffix+".csv"

error_ds = traininglogs.toDF()
error_df = error_ds.toPandas()

#ingesting data into error file
error_df.to_csv("s3://"+S3_BUCKET+"/" + filename, index=None, sep=',', encoding = 'utf-8', doublequote = True)

#Applymapping for transfers history  									
transfers_mapping = ApplyMapping.apply(frame = OnStransfers, mappings = [
    ("personid", "long", "personid", "long"),
    ("dshsid", "long", "dshsid", "long"),
    ("courseid", "string", "courseid", "string"),
    ("completeddate","date","completeddate","date"),
    ("coursename","string","coursename","string"),
    ("credithours","int","credithours","double"),
    ("trainingprogramtype", "string", "trainingprogramtype", "string"),
    ("trainingprogramcode", "int", "trainingprogramcode", "long"),
    ("coursetype", "string", "coursetype", "string"),
    ("courselanguage", "string", "courselanguage", "string"),
    ("instructorname", "string", "instructorname", "string"),
    ("instructorid", "string", "instructorid", "string"),
    ("trainingsource", "string", "trainingsource", "string"),
    ("filedate", "date", "filedate", "date")
															   
    ], transformation_ctx = "transfers_mapping")

#Appending all valid records to training history 
glueContext.write_dynamic_frame.from_catalog(
    frame=transfers_mapping,
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_traininghistory",
)

#apply apping for transfers errors	
transfers_error_mapping = ApplyMapping.apply(frame = traininglogs, mappings = [
    ("personid", "long", "personid", "long"),
    ("dshsid", "long", "dshsid", "long"),
    ("courseid", "string", "courseid", "string"),
    ("completeddate","date","completeddate","date"),
    ("coursename","string","coursename","string"),
    ("credithours","string","credithours","int"),
    ("trainingprogramtype", "string", "trainingprogramtype", "string"),
    ("trainingprogramcode", "int", "trainingprogramcode", "long"),
    ("coursetype", "string", "coursetype", "string"),
    ("courselanguage", "string", "courselanguage", "string"),
    ("instructorname", "string", "instructorname", "string"),
    ("instructorid", "string", "instructorid", "string"),
    ("trainingsource", "string", "trainingsource", "string")
															   
    ], transformation_ctx = "transfers_error_mapping")   

# Appending all error to training transfers logs
glueContext.write_dynamic_frame.from_catalog(
    frame=transfers_error_mapping,
    database="seiubg-rds-b2bds",
    table_name="b2bds_logs_traininghistorylog",
)
   

job.commit()
