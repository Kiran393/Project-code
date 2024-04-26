import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node PostgreSQL
raw_credential_delta_df = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_credential_delta",
)

person_history_doh = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_staging_personhistory", transformation_ctx = "person_history_doh").toDF().filter(col("sourcekey").like('DOH-%'))

if person_history_doh.count() != 0 : 
    raw_credential_delta_df = raw_credential_delta_df.toDF().filter(col("audit")=="New")
else :
    print("count is 0")
    raw_credential_delta_df = raw_credential_delta_df.toDF()

raw_credential_delta_df =raw_credential_delta_df.withColumn("sourcekey",concat(lit("DOH-"),raw_credential_delta_df.credentialnumber))

raw_credential_delta_df = raw_credential_delta_df.filter(col("providernamedoh").isNotNull())

raw_credential_delta_df = raw_credential_delta_df.withColumn("new_dob",raw_credential_delta_df.dateofbirth.cast('string')).withColumn("new_dob",to_date(col("new_dob"),"MM/dd/yyyy")).filter(year(col("new_dob")) >= lit(1902)).filter(col("new_dob")<=lit(date_sub(current_date(),((18*365)+4)))).filter(col("new_dob")>=lit(date_sub(current_date(),((80*365)+20)))).withColumn("new_dob",date_format(col("new_dob"),"yyyy-MM-dd"))



raw_credential_delta_df.select("providernamedoh","credentialnumber").filter(col("credentialnumber")=='HM61195494').withColumn("providernamearray",regexp_replace(trim(col('providernamedoh')),'\s+',' ')).show(20,truncate=False)

raw_credential_delta_df = raw_credential_delta_df.withColumn("providernamedoh",regexp_replace(trim(col('providernamedoh')),'\s+',' '))\
  .withColumn("firstname",split((col('providernamedoh')), '\s')[1])\
  .withColumn("middlename",concat_ws(" ",split(col('providernamedoh'), '\s')[2],split(col('providernamedoh'), '\s')[3],split(col('providernamedoh'), '\s')[4]))\
  .withColumn("lastname",split((col('providernamedoh')), '\s')[0])\
  .withColumn("middlename",when(col('middlename')=="",None).otherwise(trim(col('middlename'))))\
  .withColumn("firstname",when(col('firstname')=="",None).otherwise(trim(col('firstname'))))\
  .withColumn("lastname",when(col('lastname')=="",None).otherwise(trim(col('lastname'))))


#raw_credential_delta_df.filter(col("firstname") == "").select("providernamedoh","credentialnumber","lastname","firstname","middlename").show(20,truncate=False)

raw_credential_delta_df = raw_credential_delta_df.withColumn("hiredate", to_timestamp(col("dateofhire").cast('string'), 'MM/dd/yyyy'))
                                                
raw_credential_delta_df = raw_credential_delta_df.where("firstname is not null and lastname is not null and new_dob is not null")

raw_credential_delta_df = DynamicFrame.fromDF(raw_credential_delta_df, glueContext, "raw_credential_delta_df")

mappings_transform = ApplyMapping.apply(frame = raw_credential_delta_df, mappings = [("firstname", "string", "firstname", "string"),("middlename", "string", "middlename", "string"),("lastname", "string", "lastname", "string"),("sourcekey", "string", "sourcekey", "string"),("credentialnumber", "string", "credentialnumber", "string"),("new_dob", "string", "dob", "string"),("filemodifieddate", "timestamp", "filemodifieddate", "date")], transformation_ctx = "mappings_transform")

target_select_transform = SelectFields.apply(frame = mappings_transform, paths = ["firstname","middlename","lastname","sourcekey","credentialnumber","dob","filemodifieddate"], transformation_ctx = "target_select_transform")

resolvechoice3 = ResolveChoice.apply(frame = target_select_transform, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_staging_personhistory", transformation_ctx = "resolvechoice3")

resolvechoice4 = DropNullFields.apply(frame = resolvechoice3,  transformation_ctx = "resolvechoice4")

#Script generated for node PostgreSQL
PostgreSQL_node1646258626262 = glueContext.write_dynamic_frame.from_catalog(
    frame=resolvechoice4,
    database="seiubg-rds-b2bds",
    table_name="b2bds_staging_personhistory",
    transformation_ctx="PostgreSQL_node1646258626262",
)

job.commit()
