import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.functions import col,when,input_file_name,expr,to_date,regexp_extract,to_timestamp,length,lpad
import boto3
import json
import pandas 



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



## Capturing the secrets from secrets manger
secretsmanager = boto3.client('secretsmanager')
secretsresponse = secretsmanager.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)

database_secrets = json.loads(secretsresponse['SecretString'])

B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_NAME = 'b2bds'


## read the s3 bucket for the for the earliest uploaded file  
s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')

objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Inbound/raw/qualtrics/caregiver_intake/'))
objects.sort(key=lambda o: o.last_modified)

#print(objects[0].key)
inputfilename =  objects[0].key

print(inputfilename)

agencypersondf = glueContext.create_dynamic_frame.from_options(
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
             "s3://seiubg-b2bds-prod-feeds-fp7mk/"+(inputfilename)+""
        ],
        "recurse": True,
    },
    transformation_ctx="agencypersondf",
)

## Capture all the responseid from the raw.cg_qual_agency_person for differntiating the old and new response id 
agencypersonpreviousdf = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_cg_qual_agency_person", transformation_ctx = "agencypersonpreviousdf")
agencypersonpreviousdf = agencypersonpreviousdf.toDF().select("responseid")
agencypersonpreviousdf.createOrReplaceTempView("agencypersonpreviousdf")


## Converting all he blank columns to null and captung the filename, filedate
agencypersondf = agencypersondf.toDF()
agencypersondf = agencypersondf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in agencypersondf.columns])
agencypersondf = agencypersondf.withColumn("filename", input_file_name())
agencypersondf = agencypersondf.withColumn("filenamenew",regexp_extract(col('filename'), '(s3://)(.*)(/Inbound/raw/qualtrics/caregiver_intake/)(.*)', 4))
agencypersondf = agencypersondf.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filenamenew'), '(Caregiverdata)(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)', 2),"yyyyMMddHHmmss"))

##, applying the schema
agencypersondf = DynamicFrame.fromDF(agencypersondf, glueContext, "agencypersondf")
agencypersonmapping = ApplyMapping.apply(frame = agencypersondf, mappings = [("Q10_4", "string", "newhire_person_zipcode", "string"),("Q3", "string", "cg_status", "string"), ("Q5_4", "string", "agency_phone", "string"), ("Q9_5", "string", "newhire_person_email2", "string"), ("Q22", "string", "person_workercategory", "string"), ("Q28_4", "string", "terminated_person_zipcode", "string"), ("Q_RecaptchaScore", "string", "q_recaptchascore", "string"), ("Progress", "string", "progress", "string"), ("Status", "string", "status", "string"), ("EndDate", "string", "enddate", "string"), ("RecordedDate", "string", "recordeddate", "string"), ("ResponseId", "string", "responseid", "string"), ("Finished", "string", "finished", "string"), ("LocationLatitude", "string", "locationlatitude", "string"), ("LocationLongitude", "string", "locationlongitude", "string"), ("IPAddress", "string", "ipaddress", "string"), ("UserLanguage", "string", "userlanguage", "string"), ("RecipientLastName", "string", "recipientlastname", "string"), ("Q40", "string", "person_hire_date", "string"), ("Q5_1", "string", "agency_firstname", "string"), ("ExternalReference", "string", "externalreference", "string"), ("RecipientFirstName", "string", "recipientfirstname", "string"), ("Q14", "string", "employername", "string"), ("Q8_4", "string", "newhire_person_dob", "string"), ("Q8_5", "string", "newhire_person_ssn", "string"), ("Q10_2", "string", "newhire_person_city", "string"), ("Q8_2", "string", "newhire_person_middlename", "string"), ("Q26_3", "string", "terminated_person_lastname", "string"), ("RecipientEmail", "string", "recipientemail", "string"), ("Q9_2", "string", "newhire_person_phone2", "string"), ("Q9_3", "string", "newhire_person_phone3", "string"), ("Duration (in seconds)", "string", "duration_in_seconds", "string"), ("Q27_2", "string", "terminated_person_email", "string"), ("Q2", "string", "preferredlanguage", "string"), ("DistributionChannel", "string", "distributionchannel", "string"), ("Q26_2", "string", "terminated_person_middlename", "string"), ("Q28_3", "string", "terminated_person_state", "string"), ("Q9_1", "string", "newhire_person_phone1", "string"), ("Q3", "string", "person_emp_status", "string"), ("Q31", "string", "person_termination_date", "string"), ("Q26_1", "string", "terminated_person_firstname", "string"), ("Q8_1", "string", "newhire_person_firstname", "string"), ("Q28_1", "string", "terminated_person_street", "string"), ("Q26_6", "string", "terminated_person_ssn", "string"), ("Q27_1", "string", "terminated_person_phone", "string"), ("Q35", "string", "terminated_person_employerbranch", "string"), ("Q9_4", "string", "newhire_person_email1", "string"), ("Q10_1", "string", "newhire_person_street", "string"), ("Q26_4", "string", "terminated_person_dob", "string"), ("Q26_7", "string", "terminated_person_id", "string"), ("Q33", "string", "newhire_personemployerbranch", "string"), ("Q8_3", "string", "newhire_person_lastname", "string"), ("Q28_2", "string", "terminated_person_city", "string"), ("Q5_2", "string", "agency_lastname", "string"), ("Q10_3", "string", "newhire_person_state", "string"),("filemodifieddate", "timestamp", "filemodifieddate", "timestamp"),("filenamenew", "string", "filename", "string"), ("Q5_3", "string", "agency_email", "string"), ("Q21", "string", "exempt", "string"), ("StartDate", "string", "startdate", "string")], transformation_ctx = "applymapping1")

agencypersonmappingresolvechoice = ResolveChoice.apply(frame = agencypersonmapping, choice = "make_cols", transformation_ctx = "resolvechoice4")
agencypersondf = agencypersonmapping.toDF()


agencypersondf = agencypersondf.withColumn("dob",to_date(col('terminated_person_dob'), "MM/dd/yyyy"))
agencypersondf.createOrReplaceTempView("agencypersondf")

## capturing all the person information from prod.person table 
prodpersondf = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_prod_person", transformation_ctx = "prodpersondf")
prodpersondf = prodpersondf.toDF()
prodpersondf.createOrReplaceTempView("person")

## capturing all the terminated person information current file
agencyterminatedpersondf = spark.sql("select * from agencypersondf where cg_status = '2' and terminated_person_id in (select CAST(personid as STRING) from person)")
agencyterminatedpersondf = agencyterminatedpersondf.withColumn('prefix_terminated_person_ssn', when(length(agencyterminatedpersondf['terminated_person_ssn']) < 4 ,lpad(agencyterminatedpersondf['terminated_person_ssn'],4,'0')).when(agencyterminatedpersondf.terminated_person_ssn == '0', None).otherwise(agencyterminatedpersondf['terminated_person_ssn']))
agencyterminatedpersondf.createOrReplaceTempView("agencyterminatedpersondf")

##matching the terminated person from agency aganist the prod.person table  firstname,lastname,ssn and email1,email2 for accuracy
agencyterminatedpersoninprodperson = spark.sql("select c.* from agencyterminatedpersondf c join person p on CAST(p.personid as STRING) = c.terminated_person_id where (upper(c.terminated_person_firstname) = upper(p.firstname) or upper(c.terminated_person_lastname) = upper(p.lastname)) and (c.prefix_terminated_person_ssn = p.ssn or c.dob = p.dob or c.terminated_person_email = coalesce(p.email1, p.email2))")
agencyterminatedpersoninprodperson.createOrReplaceTempView("agencyterminatedpersoninprodperson")

# un matching the terminated person from agency aganist the prod.person table  
agencypersondf_error = spark.sql("select * from agencypersondf cg where cg.terminated_person_id not in (select terminated_person_id from agencyterminatedpersoninprodperson)")

# matching the terminated person from agency aganist the prod.person table and new hire persons from agency
agencypersondf_clean = spark.sql("select * from agencypersondf cg where cg.cg_status = 1 or (cg_status = 2 and cg.terminated_person_id in (select terminated_person_id from agencyterminatedpersoninprodperson))")

agencypersondf_clean = agencypersondf_clean.drop("dob")
agencypersondf_error = agencypersondf_error.drop("dob")


## error file is generated and posted to s3
errorfilenameprefix = spark.sql("select distinct filename FROM agencypersondf p limit 1").toPandas().iat[0,0][0:-4]
agencypersondf_error_pandas_df = agencypersondf_error.toPandas()
agencypersondf_error_pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/qualtrics/errorlog/"+errorfilenameprefix+"-error.csv", header=True, index=None, sep='|')

agencypersondf_clean.createOrReplaceTempView("agencypersoncleandf")


spark.sql("select trim(responseid) from agencypersonpreviousdf").show()
spark.sql("select *,  true as isdelta from agencypersoncleandf where trim(responseid) not in (select trim(responseid) from agencypersonpreviousdf)").show()
spark.sql("select *, false as isdelta from agencypersoncleandf where trim(responseid) in (select trim(responseid) from agencypersonpreviousdf)").show()

## to determine if response id is new,  Capture all the responseid from the raw.cg_qual_agency_person. For differntiating response id is present in the existing dataset then old, else its new 
agencypersondf_clean = spark.sql("""
   select *, true as isdelta from agencypersoncleandf where trim(responseid) not in (select trim(responseid) from agencypersonpreviousdf)
    UNION 
   select *, false as isdelta from agencypersoncleandf where trim(responseid) in (select trim(responseid) from agencypersonpreviousdf)
    """)

## tuncate and load the new data 
mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
agencypersondf_clean.write.option("truncate",True).jdbc(url=url, table="raw.cg_qual_agency_person", mode=mode, properties=properties)


## Move the processed file to the archive location 
sourcekey = objects[0].key
targetkey = objects[0].key.replace("/raw/", "/archive/")
print(sourcekey)
print(targetkey)
copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
bucket.copy(copy_source, targetkey)
s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()

job.commit()