import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import substring_index,split,col,expr,lit
import pandas
from datetime import datetime
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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

today = datetime.now()
suffix = today.strftime("%m_%d_%Y")

# Script generated for node PostgreSQL table
raw_cs_cornerstone_completion = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_cornerstone_completion",
    transformation_ctx="raw_cornerstone_completion",
).toDF()
raw_cs_cornerstone_completion = raw_cs_cornerstone_completion.withColumn("offering_title",substring_index(raw_cs_cornerstone_completion.offering_title, '(', 1))\
    .withColumn("instructor_name",lit("instructor"))


raw_cs_cornerstone_completion.createOrReplaceTempView("raw_cs_cornerstone_completion")


# Renaming the columns and filtering results with status as Passed
spark.sql("""select bg_person_id as learner_id, offering_title as class_title, course_title as class_id, 
          cast(completed_date as date) date, user_last_name as last_name, user_first_name as first_name, user_email as learner_email, 
          0 as duration, cast(registered_date as date) when_enrolled, instructor_name as instructor, status, 
          phone_number from raw_cs_cornerstone_completion where status = 'Passed' """).createOrReplaceTempView("raw_cs_cornerstone_completion")



# Script generated for node PostgreSQL
prod_person = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person",
    transformation_ctx="prod_person").toDF()
prod_person.createOrReplaceTempView("prodperson")
    
    
    # Script generated for node PostgreSQL
course_catalog = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_coursecatalog",
    transformation_ctx="course_catalog",).toDF()
course_catalog.createOrReplaceTempView("coursecatalog")

# Script generated for node PostgreSQL
instructor = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_instructor",
    transformation_ctx="instructor",
).toDF()
instructor.createOrReplaceTempView("instructor")


## Removing duplicate courses, if the course is offered in both online(ONL) and also Instructor-Led (ILT) the we pick the ILT courseid as default
spark.sql("with coursecatalog as (select *, ROW_NUMBER () OVER ( PARTITION BY curr.coursename, curr.courselanguage,curr.trainingprogramcode ORDER BY curr.coursetype ASC ) courserank from coursecatalog curr) select  coursename ,credithours ,trainingprogramtype, trainingprogramcode, coursetype, courselanguage,courseversion,courseid,dshscourseid from coursecatalog where courserank = 1").createOrReplaceTempView("coursecatalog")

## Capturing only the active instuctors from the instructor table
spark.sql("select distinct trim(concat(trim(firstname),' ',trim(lastname))) as instructorname , dshsinstructorid  as instructorid from instructor where isactive = 'true'").createOrReplaceTempView("instructor")


## Identify and flagging valid and invalid records if lastname,personid,email1 not found in prod person table
spark.sql("""select cs.*,CASE WHEN pr.lastname is null or coalesce(pr.email1,pr.email2) is null or pr.personid   is null   THEN 0 ELSE 1 END ValidInd 
          from raw_cs_cornerstone_completion  cs left join prodperson pr on  
lower(trim(cs.last_name))=lower(pr.lastname)  and lower(trim(cs.learner_email))=lower(coalesce(pr.email1,pr.email2)) and  trim(cs.learner_id) =cast(pr.personid as string)""").createOrReplaceTempView("raw_cs_cornerstone_completionLastName_person_email_valid_flag")

## identifying duplicate records based on learner_id,class_title getting earliest record and flagging valid and invalid records
spark.sql("select date,learner_id,class_id,last_name,class_title,duration,learner_email,instructor,when_enrolled, phone_number,first_name,status, CASE WHEN RNO>1 then 0 else 1 end as ValidInd from (select *, row_number() over  (Partition by learner_id,class_title ORDER BY  date ASC )  rno  from  raw_cs_cornerstone_completionLastName_person_email_valid_flag where ValidInd=1  ) DS").createOrReplaceTempView("raw_cs_cornerstone_DuplicatesError")


## Combining duplicate records and lastname,personid,email1 not found in prodperson
finalInvalidDF = spark.sql("select *from raw_cs_cornerstone_completionLastName_person_email_valid_flag where ValidInd=0  UNION  select * from raw_cs_cornerstone_DuplicatesError where ValidInd = 0")



## valid records process it to training history table
spark.sql("select *from raw_cs_cornerstone_DuplicatesError where ValidInd=1" ).createOrReplaceTempView("raw_cc_course_completions")

## Combining coursecatalog , instructor and completion details   
cccompletionsdf = spark.sql("select distinct sc.learner_id ,sc.class_title ,sc.date as completiondate ,tc.credithours,tc.trainingprogramtype,tc.trainingprogramcode ,tc.courseid ,tc.coursetype ,tc.courselanguage ,tc.dshscourseid, sc.instructor as instructorname,ins.instructorid,'CORNERSTONE' as trainingsource, current_timestamp() as filemodifieddate from raw_cc_course_completions sc left join coursecatalog tc on lower(trim(sc.class_title)) = lower(trim(tc.coursename)) left join instructor ins on lower(trim(ins.instructorname)) = lower(trim(sc.instructor))")

cccompletionsdf.show()

## creating DynamicFrame for valid indicator records to insert into training history table 
traininghistorydf = DynamicFrame.fromDF(cccompletionsdf, glueContext, "traininghistorydf")

# Script generated for node Apply Mapping

ApplyMapping_node1646956720451 = ApplyMapping.apply(
    frame=traininghistorydf,
    mappings=[
        ("learner_id", "string", "personid", "long"),
        ("class_title", "string", "coursename", "string"),
        ("completiondate", "date", "completeddate", "date"),
        ("credithours", "string", "credithours", "double"),
        ("trainingprogramtype", "string", "trainingprogramtype", "string"),
        ("trainingprogramcode", "string", "trainingprogramcode", "long"),
        ("courseid", "string", "courseid", "string"),
        ("coursetype", "string", "coursetype", "string"),
        ("courselanguage", "string", "courselanguage", "string"),
        ("instructorname", "string", "instructorname", "string"),
        ("instructorid", "string", "instructorid", "string"),
        ("trainingsource", "string", "trainingsource", "string"),
         ("filemodifieddate", "timestamp", "filedate", "timestamp"),
          ("dshscourseid", "string", "dshscourseid", "string"),
    ],
    transformation_ctx="ApplyMapping_node1646956720451",
)

selectFields_raw_trainghistory = SelectFields.apply(
    frame=ApplyMapping_node1646956720451,
    paths=[
        "personid",
        "completeddate",
        "credithours",
        "coursename",
        "courseid",
        "coursetype",
        "courselanguage",
        "trainingprogramcode",
        "trainingprogramtype",
        "instructorname",
        "instructorid",
        "trainingsource",
        "filedate",
        "dshscourseid"
    ],
    transformation_ctx="selectFields_raw_trainghistory",
)

dyf_dropNullfields = DropNullFields.apply(frame = selectFields_raw_trainghistory)

#Script generated for node PostgreSQL
glueContext.write_dynamic_frame.from_catalog(
    frame=dyf_dropNullfields,
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_traininghistory"
)

## used to generate csv file invalid records from source file into s3 outbound folder
outboundDF = finalInvalidDF.withColumnRenamed("date","Date")\
                        .withColumnRenamed("learner_id","Learner ID")\
                        .withColumnRenamed("class_id","Class ID")\
                        .withColumnRenamed("last_name","First Name")\
                        .withColumnRenamed("class_title","Class Title")\
                        .withColumnRenamed("duration","Duration")\
                        .withColumnRenamed("learner_email","Learner Email")\
                        .withColumnRenamed("instructor","Instructor")\
                        .withColumnRenamed("when_enrolled","When Enrolled")\
                        .withColumnRenamed("phone_number","Phone Number")\
                        .withColumnRenamed("first_name","First Name")\
                        .withColumnRenamed("status","Status")


pandas_df = outboundDF.toPandas()

pandas_df.to_csv("s3://"+S3_BUCKET+"/Outbound/cornerstone/cornerstone_completion_error_"+suffix+".csv", header=True, index=None, sep='|')

# Adding to traininghistory logs
finalInvalidDF.createOrReplaceTempView("finalInvalidDF")
errordf = spark.sql("select cast(learner_id as long) personid, class_title as coursename, class_id as courseid, cast(date as date) completeddate, cast(duration as double) credithours,instructor as instructorname from finalInvalidDF")
errorcdf = DynamicFrame.fromDF(errordf, glueContext, "historydf")

glueContext.write_dynamic_frame.from_catalog(
    frame=errorcdf,
    database="seiubg-rds-b2bds",
    table_name="b2bds_logs_traininghistorylog",
)


job.commit()