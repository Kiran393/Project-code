import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat, col, lit, coalesce, year, current_date, date_format, to_date, date_sub,regexp_replace
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import when
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat,col,sha2,concat_ws,when,md5,current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.functions import substring


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "postgresrds", table_name = "rawb2bdevdb_raw_cg_qual_agency_person", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_cg_qual_agency_person", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("newhire_person_lastname", "string", "lastname", "string"), ("userlanguage", "string", "language", "string"), ("newhire_person_email1", "string", "email1", "string"), ("newhire_person_street", "string", "physicaladdress", "string"), ("newhire_person_ssn", "string", "ssn", "string"), ("person_workercategory", "string", "workercategory", "string"), ("newhire_person_firstname", "string", "firstname", "string"), ("person_hire_date", "string", "hiredate", "timestamp"), ("newhire_person_phone1", "string", "homephone", "string"), ("exempt", "string", "exempt", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]

rawdf = datasource0.toDF()
#filter condition for delta 
rawdf = rawdf.filter("isdelta == 'true'")
rawdf.createOrReplaceTempView("cg_qual_agency_person")
#rawdf = spark.sql("select * from cg_qual_agency_person where (cg_status='1' and person_hire_date is not null and person_hire_date <> '') or (cg_status='2' and terminated_person_id is not null and person_termination_date is not null)")
#rawdf.printSchema()
rawdf =rawdf.withColumn("newaddress",concat(col("newhire_person_street"),lit(", "), col("newhire_person_city"),lit(", "), col("newhire_person_state"),lit(", "), col("newhire_person_zipcode"))).withColumn("terminatedaddress",concat(col("terminated_person_street"),lit(", "), col("terminated_person_city"),lit(", "), col("terminated_person_state"),lit(", "), col("terminated_person_zipcode"))).withColumn("mailingcountry",lit("USA"))
rawdf = rawdf.withColumn("exempt", when(col("exempt") =="1","true").otherwise("false")).withColumn("status", when(col("cg_status")=="1","Active")
          .when(col("cg_status")=="2","Terminated"))

#hire_dateexpr =  """^(\d{1,2})[-/](\d{1,2})[-/](\d{4})+$"""
#rawdf.filter(col("person_hire_date").rlike(hire_dateexpr))
#rawdf = rawdf.withColumn("person_hire_date", to_date(regexp_replace(col("person_hire_date"), "[/-]", "-"),"MM-dd-yyyy"))
rawdf =rawdf.withColumn("hire_date", coalesce(*[to_date("person_hire_date", f) for f in ("MM/dd/yy","MM/dd/yyyy","MM-dd-yyyy","MMddyyyy")])).withColumn("person_hire_dt",when((year(col("hire_date"))>1900) & (month(col("hire_date")) < 13) & (dayofmonth(col("hire_date")) < 32),col("hire_date")))
#rawdf= rawdf.filter(col("person_hire_date").isNotNull())
rawdf_filtered = rawdf.withColumn("firstname", when(rawdf.cg_status == "1",rawdf.newhire_person_firstname) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_firstname) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("lastname", when(rawdf.cg_status == "1",rawdf.newhire_person_lastname) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_lastname) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("middlename", when(rawdf.cg_status == "1",rawdf.newhire_person_middlename) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_middlename) 
                                 .when(rawdf.cg_status.isNull() ,"")) .withColumn("email1", when(rawdf.cg_status == "1",rawdf.newhire_person_email1) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_email) 
                                 .when(rawdf.cg_status.isNull() ,""))  .withColumn("homephone", when(rawdf.cg_status == "1",rawdf.newhire_person_phone2) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_phone)
                                 .when(rawdf.cg_status.isNull() ,""))  .withColumn("mobilephone", when(rawdf.cg_status == "1",rawdf.newhire_person_phone1) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_phone)                                  
                                 .when(rawdf.cg_status.isNull() ,"")) .withColumn("mailingaddress", when(rawdf.cg_status == "1",rawdf.newaddress) 
                                 .when(rawdf.cg_status == "2",rawdf.terminatedaddress) 
                                 .when(rawdf.cg_status.isNull() ,""))  .withColumn("ssn", when(rawdf.cg_status == "1",rawdf.newhire_person_ssn) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_ssn) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("dob", when(rawdf.cg_status == "1",rawdf.newhire_person_dob) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_dob) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("mailingstreet1", when(rawdf.cg_status == "1",rawdf.newhire_person_street) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_street) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("mailingcity", when(rawdf.cg_status == "1",rawdf.newhire_person_city) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_city) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("mailingstate", when(rawdf.cg_status == "1",rawdf.newhire_person_state) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_state) 
                                 .when(rawdf.cg_status.isNull() ,"")).withColumn("mailingzip", when(rawdf.cg_status == "1",rawdf.newhire_person_zipcode) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_zipcode) 
                                 .when(rawdf.cg_status.isNull() ,""))
rawdf_filtered = rawdf_filtered.withColumn("dob", coalesce(*[to_date("dob", f) for f in ("MM/dd/yy","MM/dd/yyyy","MM-dd-yyyy","MMddyyyy")])).withColumn("person_dob",when((year(col("dob"))>1900) & (month(col("dob")) < 13) & (dayofmonth(col("dob")) < 32),col("dob")))
#rawdf_filtered.select("person_dob").show()
rawdf_filtered = rawdf_filtered.withColumn('prefix_ssn', F.when(F.length(rawdf_filtered['ssn']) < 4 ,F.lpad(rawdf_filtered['ssn'],4,'0')).when(rawdf_filtered.ssn == '0', None).otherwise(rawdf_filtered['ssn']))
rawdf_filtered= rawdf_filtered.withColumn("person_dob",  col("person_dob").cast('string'))

rawdf_filtered = rawdf_filtered.withColumn("MD5",md5(concat_ws("",col("prefix_ssn"),col("person_dob"))))
rawdf_filtered.createOrReplaceTempView("cg_qual_agency_person")
rawdf_filtered = spark.sql("select * from cg_qual_agency_person where (status='Active' and hire_date is not null and hire_date <> '') or (status='Terminated' and terminated_person_id is not null and person_termination_date is not null)")
rawdf_filtered01 =rawdf_filtered.withColumn("categorycode",lit("SHCA"))\
              .withColumn("employername",when(rawdf.employername == "1","102")
              .when(rawdf_filtered.employername == "2","113")
              .when(rawdf_filtered.employername == "3","105")
              .when(rawdf_filtered.employername == "4","106")
              .when(rawdf_filtered.employername == "5","116")
              .when(rawdf_filtered.employername == "6","107")
              .when(rawdf_filtered.employername == "7","117")
              .when(rawdf_filtered.employername == "8","109")
              #.when(rawdf_filtered.employername == "9","103")
              .when(rawdf_filtered.employername == "10","104")
              .when(rawdf_filtered.employername == "11","111")
              .when(rawdf_filtered.employername == "12","108")
              .when(rawdf_filtered.employername == "13","119")
              .when(rawdf_filtered.employername == "14","120")
              .when(rawdf_filtered.employername == "15","121"))
              #.when(rawdf_filtered.employername == "16","426"))
rawdf_filtered02= rawdf_filtered01.withColumn("branchid",when(rawdf_filtered01.employername == "102", 390)
                        .when(rawdf_filtered01.employername == "113", 398)
                        .when(rawdf_filtered01.employername == "105", 391)
                        .when(rawdf_filtered01.employername == "106", 223)
                        .when(rawdf_filtered01.employername == "116", 201)
                        .when(rawdf_filtered01.employername == "107", 393)
                        .when(rawdf_filtered01.employername == "117", 202)
                        .when(rawdf_filtered01.employername == "109", 394)
                        .when(rawdf_filtered01.employername == "104", 388)
                        .when(rawdf_filtered01.employername == "111", 396)
                        .when(rawdf_filtered01.employername == "108", 246)
                        .when(rawdf_filtered01.employername == "119", 284)
                        .when(rawdf_filtered01.employername == "120", 205)
                        .when(rawdf_filtered01.employername == "121", 206))
                        #.when(rawdf_filtered01.employername == "426", 429))

rawdf_filtered03 =rawdf_filtered02.withColumn("source",when(rawdf_filtered02.branchid == 390, "Addus")
                        .when(rawdf_filtered02.branchid == 398, "AllWaysCaring")
                        .when(rawdf_filtered02.branchid == 391, "Amicable")
                        .when(rawdf_filtered02.branchid == 223 ,"CCS")
                        .when(rawdf_filtered02.branchid == 201, "CDM")
                        .when(rawdf_filtered02.branchid == 393, "Chesterfield")
                        .when(rawdf_filtered02.branchid == 202, "CoastalCap")
                        .when(rawdf_filtered02.branchid == 394, "ConcernedCitizens")
                        .when(rawdf_filtered02.branchid == 388, "FirstChoice")
                        .when(rawdf_filtered02.branchid == 396, "FullLife")
                        .when(rawdf_filtered02.branchid == 246, "KWA")
                        .when(rawdf_filtered02.branchid == 284, "OlyCap")
                        .when(rawdf_filtered02.branchid == 205, "Seamar")
                        .when(rawdf_filtered02.branchid == 206, "SLR"))\
                        .withColumn("person_workercategory",lit("Standard HCA"))\
                        .withColumn("termdate",to_date(col("person_termination_date"),"MM-dd-yyyy"))\
                        .withColumn("language", when(rawdf_filtered02.preferredlanguage == "1", "English")
                        .when(rawdf_filtered02.preferredlanguage == "2", "Amharic")
                        .when(rawdf_filtered02.preferredlanguage == "3", "Arabic")
                        .when(rawdf_filtered02.preferredlanguage == "4", "Chinese")
                        .when(rawdf_filtered02.preferredlanguage == "5", "Farsi")
                        .when(rawdf_filtered02.preferredlanguage == "6", "Khmer")
                        .when(rawdf_filtered02.preferredlanguage == "7", "Korean")
                        .when(rawdf_filtered02.preferredlanguage == "8", "Lao")
                        .when(rawdf_filtered02.preferredlanguage == "9", "Nepali")
                        .when(rawdf_filtered02.preferredlanguage == "10", "Punjabi")
                        .when(rawdf_filtered02.preferredlanguage == "11", "Russian")
                        .when(rawdf_filtered02.preferredlanguage == "12", "Samoan")
                        .when(rawdf_filtered02.preferredlanguage == "13", "Somali")
                        .when(rawdf_filtered02.preferredlanguage == "14", "Spanish")
                        .when(rawdf_filtered02.preferredlanguage == "15", "Tagalog")
                        .when(rawdf_filtered02.preferredlanguage == "16", "Ukrainian")
                        .when(rawdf_filtered02.preferredlanguage == "17", "Vietnamese"))\
                        
# Hash of SSN and DOB
rawdf_filtered05= rawdf_filtered03.filter(col("employername").isNotNull())
rawdf_filtered05= rawdf_filtered05.filter(col("branchid").isNotNull())
rawdf_hash = rawdf_filtered05.withColumn("sourcehashkey",concat(col("source"),lit("-"),substring(col("MD5"), 1, 10)))

#rawdf_hash.createOrReplaceTempView("qual")
#spark.sql("select sourcehashkey,* from qual where prefix_ssn ='4713'").show()					   


################################Validation-starts#############################################


regex = """^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$"""
phonexpr = """^(?:\+?(\d{1})?-?\(?(\d{3})\)?[\s-\.]?)?(\d{3})[\s-\.]?(\d{4})[\s-\.]?"""



rawdf_Valid = rawdf_hash.withColumn("is_validphone", when( col("homephone").isNull() | col("homephone").rlike(phonexpr), lit("valid")).otherwise(lit("invalid")))
rawdf_Valid = rawdf_Valid.withColumn("is_validphone", when( col("mobilephone").isNull() | col("mobilephone").rlike(phonexpr), lit("valid")).otherwise(lit("invalid")))
rawdf_Valid = rawdf_Valid.withColumn("is_valid_email", when( col("email1").isNull() | col("email1").rlike(regex) | col("email1").rlike("") , lit("valid")).otherwise(lit("invalid")))



rawdf_Valid.select("is_validphone","homephone","hire_date","is_valid_email","email1","dob","person_workercategory","categorycode").show(truncate = False)
rawdf_Valid = rawdf_Valid.where((rawdf_Valid.is_validphone == "valid") & (rawdf_Valid.is_valid_email == "valid")) 



rawdf_Valid.select("newaddress","terminatedaddress","hire_date","firstname", "sourcehashkey","dob","person_workercategory","categorycode").show()
# Validation for Date of Birth 

rawdf_Valid = rawdf_Valid.withColumn("new_dob",rawdf_Valid.person_dob).withColumn("homephone",concat(lit("1"),regexp_replace(col('homephone'), '[^A-Z0-9_]', ''))).withColumn("mobilephone",concat(lit("1"),regexp_replace(col('mobilephone'), '[^A-Z0-9_]', '')))
rawdf_Valid = rawdf_Valid.withColumn("new_dob",to_date(col("new_dob"),"yyyy-MM-dd")).filter(F.year(col("new_dob")) >= F.lit(1900)).filter(col("new_dob")<=F.lit(F.date_sub(F.current_date(),((18*365)+4)))).withColumn("new_dob",date_format(col("new_dob"),"yyyy-MM-dd"))


#rawdf_Valid.createOrReplaceTempView("cg_qual_agency_person")
#spark.sql("select * from cg_qual_agency_person where sourcehashkey is null").show()

################################Validation-ends#############################################


newdatasource0 = DynamicFrame.fromDF(rawdf_Valid, glueContext, "newdatasource0")
applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("lastname", "string", "lastname", "string"),("middlename", "string", "middlename", "string"),("sourcehashkey", "string", "sourcekey", "string"),("new_dob", "string", "dob", "string"),("terminated_person_id", "string", "agencyid", "long"), ("userlanguage", "string", "language", "string"), ("email1", "string", "email1", "string"),("language", "string", "preferred_language", "string"),("mailingaddress", "string", "mailingaddress", "string"), ("prefix_ssn", "string", "ssn", "string"), ("person_workercategory", "string", "workercategory", "string"),("status", "string", "status", "string"),("filemodifieddate", "timestamp", "filemodifieddate", "date"), ("categorycode", "string", "categorycode", "string"),("firstname", "string", "firstname", "string"), ("person_hire_dt", "date", "hiredate", "timestamp"),("person_hire_dt", "date", "trackingdate", "timestamp"), ("homephone", "string", "homephone", "string"),("mobilephone", "string", "mobilephone", "string"), ("exempt", "string", "exempt", "string"),("mailingstreet1","string","mailingstreet1","string"),("mailingcity","string","mailingcity","string"),("mailingstate","string","mailingstate","string"),("mailingzip","string","mailingzip","string"),("mailingcountry","string","mailingcountry","string")], transformation_ctx = "applymapping1")

## @type: SelectFields
## @args: [paths = ["sourcekey", "firstname", "goldenrecordid", "cdwa_id", "language", "type", "workercategory", "credentialnumber", "mailingaddress", "hiredate", "lastname", "ssn", "tccode", "physicaladdress", "homephone", "modified", "personid", "exempt", "email1", "status"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["sourcekey", "firstname", "goldenrecordid", "cdwa_id", "language", "type", "workercategory", "credentialnumber", "mailingaddress", "categorycode", "dob", "hiredate","middlename" ,"lastname","trackingdate","preferred_language", "ssn", "tccode", "physicaladdress", "homephone", "mobilephone","modified", "exempt", "email1","agencyid", "status","filemodifieddate","mailingstreet1","mailingcity","mailingstate","mailingcountry","mailingzip","filedate"], transformation_ctx = "selectfields2")
#selectfields2.toDF().select("categorycode","workercategory","dob").show()
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
df = resolvechoice4.toDF()
print("validdf count" ,df.count())

## @type: DataSink
## @args: [database = "postgresrds", table_name = "stagingb2bdevdb_staging_personhistory", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_staging_personhistory", transformation_ctx = "datasink5")

print('done')

job.commit()