import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat, col, lit, coalesce, year, current_date, date_format, to_date,regexp_replace
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import boto3
import re

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "postgresrds", table_name = "rawb2bdevdb_raw_cdwa", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_cdwa", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("language", "string", "language", "string"), ("ssn", "string", "ssn", "string"), ("first_name", "string", "firstname", "string"), ("email", "string", "email", "string"), ("phone_2", "int", "phone", "string"), ("last_name", "string", "lastname", "string"), ("hire_date", "date", "hiredate", "timestamp"), ("person_id", "int", "cdwa_id", "string"), ("employee_classification", "string", "workercategory", "string"), ("exempt_status", "string", "exempt", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]

###### selecting only valid rows from CDWA ############

new_df = datasource0.toDF()

new_df.createOrReplaceTempView("cdwa")
new_df = spark.sql("select * from cdwa where isvalid = true")


new_df= new_df.withColumn("phyaddress", concat(col("physical_add_1"),lit(", "), col("physical_city"),lit(", "), col("physical_state"),lit(", "), col("physical_zip"))).withColumn("mailaddress", concat(col("mailing_add_1"),lit(", "), col("mailing_city"),lit(", "), col("mailing_state"),lit(", "), col("mailing_zip"))).withColumn('prefix_ssn', F.when(F.length(new_df['ssn']) < 4 ,F.lpad(new_df['ssn'],4,'0')).otherwise(new_df['ssn']))

#new_df = new_df.withColumn("mobilephone",concat(lit("1"),regexp_replace(col('phone_1'), '[^0-9]', ''))).withColumn("homephone",concat(lit("1"),regexp_replace(col('phone_2'), '[^0-9]', '')))

new_df = new_df.withColumn('phone_1', substring(regexp_replace(col('phone_1'), '[^0-9]', ''),-10,10)).withColumn('mobilephone',when(length('phone_1')==10,concat(lit(1),col('phone_1'))).otherwise(None)).withColumn('phone_2', substring(regexp_replace(col('phone_2'), '[^0-9]', ''),-10,10)).withColumn('homephone',when(length('phone_2')==10,concat(lit(1),col('phone_2'))).otherwise(None))

new_df.select("homephone","mobilephone").distinct().show()

new_df = new_df.withColumn('isahcas_eligible',when(col("ahcas_eligible") == 1, lit('true')).otherwise(lit('false')))

print('first dataframe dates')
new_df.select("classification_start_date","hire_date").distinct().show(truncate=0)
################################Validation-starts#############################################


#regex = """^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$"""
#phonexpr = """^(?:\+?(\d{1})?-?\(?(\d{3})\)?[\s-\.]?)?(\d{3})[\s-\.]?(\d{4})[\s-\.]?"""


#new_df = new_df.withColumn("is_validphone", when(col("phone").rlike(phonexpr), lit("valid")).otherwise(lit("invalid")))
#new_df = new_df.withColumn("is_validemail", when(col("email").rlike(regex), lit("valid")).otherwise(lit("invalid")))

#new_df.select("is_validphone","phone","is_validemail","email").show(truncate = False)
#new_df = new_df.where((new_df.is_validemail == "valid") & (new_df.is_validphone == "valid")) 
print("Testing Validation")
#new_df.show()

# Validation for Date of Birth 

new_df = new_df.withColumn("new_dob",new_df.dob.cast('string'))
new_df = new_df.withColumn("new_dob",to_date(col("new_dob"),"yyyyMMdd")).filter(F.year(col("new_dob")) >= F.lit(1900)).filter(col("new_dob")<=F.lit(F.date_sub(F.current_date(),((18*365)+4)))).withColumn("new_dob",date_format(col("new_dob"),"yyyy-MM-dd")).withColumn("sourcekey",concat(lit("CDWA-"),col("personid"))).withColumn("mailingcountry",lit("USA")).withColumn("trackingdate",to_timestamp(col("classification_start_date").cast('string'), 'yyyyMMdd')).withColumn("hiredate", to_timestamp(col("hire_date").cast('string'), 'yyyyMMdd')).withColumn("background_check_date", to_date(col("background_check_date").cast('string'), 'yyyyMMdd')).withColumn("ie_date", to_date(col("ie_date").cast('string'), 'yyyyMMdd'))
new_df = new_df.withColumn("filemodifieddate",to_date(regexp_extract(col('filename'), '(CDWA-O-BG-ProviderInfo-)(\d\d\d\d-\d\d-\d\d)(.csv)', 2),"yyyy-MM-dd")).withColumn("exempt", when(col("exempt_status") =="Yes","true").otherwise("false"))

#new_df.select("new_dob", "dob", "phone", "email").show(2, False)
new_df.printSchema()

new_df.select("trackingdate","hiredate").distinct().show(truncate=0)

################################Validation-ends#############################################

newdatasource0 = DynamicFrame.fromDF(new_df, glueContext, "newdatasource0")
#newdatasourcemap = newdatasourcetmp.apply_mapping([("language", "string", "language", "string"), ("phyaddress", "string", "physicaladdress", "string"), ("ssn", "string", "ssn", "string"), ("first_name", "string", "firstname", "string"), ("email", "string", "email1", "string"), ("phone", "int", "phone", "string"), ("last_name", "string", "lastname", "string"), ("hire_date", "date", "hiredate", "timestamp"), ("person_id", "int", "cdwa_id", "string"), ("employee_classification", "string", "workercategory", "string"), ("exempt_status", "string", "exempt", "string")])
applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("language", "string", "language", "string"),("isahcas_eligible", "string", "ahcas_eligible", "boolean"),("background_check_date", "date", "last_background_check_date", "date"),("ie_date", "date", "ie_date", "date"),("phyaddress", "string", "physicaladdress", "string"),("filemodifieddate", "date", "filemodifieddate", "date"),("mailaddress", "string", "mailingaddress", "string"), ("prefix_ssn", "string", "ssn", "string"), ("first_name", "string", "firstname", "string"),("middle_name", "string", "middlename", "string"),("trackingdate", "timestamp", "trackingdate", "timestamp"), ("email", "string", "email1", "string"), ("mobilephone", "string", "mobilephone", "string"),("homephone", "string", "homephone", "string"), ("last_name", "string", "lastname", "string"),("carina_eligible", "string", "iscarinaeligible", "boolean"), ("hiredate", "timestamp", "hiredate", "timestamp"), ("personid", "string", "cdwa_id", "bigint"),("employee_classification", "string", "workercategory", "string"), ("exempt", "string", "exempt", "string"), ("new_dob", "string", "dob", "string"),("mailing_add_1","string","mailingstreet1","string"),("mailingcountry","string","mailingcountry","string"),("mailing_add_2","string","mailingstreet2","string"),("mailing_city","string","mailingcity","string"),("mailing_state","string","mailingstate","string"),("mailing_zip","string","mailingzip","string"),("sourcekey","string","sourcekey","string"),("employee_id","decimal(9,0)","dshsid","decimal(9,0)")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["sourcekey", "firstname", "goldenrecordid", "cdwa_id", "language", "type", "workercategory", "credentialnumber", "mailingaddress", "hiredate", "lastname", "ssn", "tccode", "physicaladdress", "phone", "modified", "personid", "exempt", "email1", "status"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["sourcekey", "firstname", "goldenrecordid","last_background_check_date", "cdwa_id", "language","ahcas_eligible", "type", "workercategory", "credentialnumber", "mailingaddress", "hiredate", "lastname", "ssn", "tccode", "physicaladdress", "mobilephone", "homephone", "modified", "personid", "exempt", "email1", "status","ie_date", "dob","mailingstreet1","mailingstreet2","mailingcity","mailingstate","mailingzip","dshsid","iscarinaeligible","middlename","mailingcountry","trackingdate","filemodifieddate"], transformation_ctx = "selectfields2")

selectfields2.toDF().select("mobilephone","homephone","ie_date").show()
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "postgresrds", table_name = "stagingb2bdevdb_staging_personhistory", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_staging_personhistory", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
#resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
resolvechoice4 = DropNullFields.apply(frame = resolvechoice3,  transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "postgresrds", table_name = "stagingb2bdevdb_staging_personhistory", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_staging_personhistory", transformation_ctx = "datasink5")
job.commit()