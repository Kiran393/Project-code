import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import sha2,concat_ws,when,md5,current_timestamp
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_date,date_format, substring

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "postgresrds", table_name = "b2bdevdb_raw_cg_qual_agency_person", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_cg_qual_agency_person", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("terminated_person_id", "string", "employeeid", "long"), ("person_workercategory", "string", "workercategory", "string"), ("cg_status", "string", "empstatus", "string"), ("person_termination_date", "string", "terminationdate", "string"), ("employername", "string", "employerid", "string"), ("person_hire_date", "string", "hiredate", "timestamp")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
rawdf = datasource0.toDF()
#filter condition for delta
rawdf = rawdf.filter("isdelta == 'true' ")
#rawdf.createOrReplaceTempView("cg_qual_agency_person")
#rawdf = spark.sql("select * from cg_qual_agency_person where (cg_status='1' and person_hire_date is not null and person_hire_date <> '') or (cg_status='2' and terminated_person_id is not null and person_termination_date is not null)")
rawdf = rawdf.withColumn("ssn", when(rawdf.cg_status == "1",rawdf.newhire_person_ssn) \
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_ssn)\
                                 .when(rawdf.cg_status.isNull() ,""))\
                                 .withColumn("dob", when(rawdf.cg_status == "1",rawdf.newhire_person_dob) 
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_dob)\
                                 .when(rawdf.cg_status.isNull() ,""))\
                                 .withColumn("firstname", when(rawdf.cg_status == "1",rawdf.newhire_person_firstname)
                                 .when(rawdf.cg_status == "2",rawdf.terminated_person_firstname))
rawdf = rawdf.withColumn("dob", coalesce(*[to_date("dob", f) for f in ("MM/dd/yy","MM/dd/yyyy","MM-dd-yyyy","MMddyyyy")])).withColumn("person_dob",when((year(col("dob"))>1900) & (month(col("dob")) < 13) & (dayofmonth(col("dob")) < 32),col("dob")))
rawdf= rawdf.withColumn("dob",  col("dob").cast('string'))
# Hash of SSN and DOB
rawdf = rawdf.withColumn('prefix_ssn', F.when(F.length(rawdf['ssn']) < 4 ,F.lpad(rawdf['ssn'],4,'0')).when(rawdf.ssn == '0', None).otherwise(rawdf['ssn']))
rawdf_hash = rawdf.withColumn("MD5",md5(concat_ws("",col("prefix_ssn"),col("dob"))))
#rawdf_hash = rawdf_hash.withColumn("source",concat(lit("QUAL-"), col("MD5")))


#rawdf =rawdf.withColumn("source",concat(lit("QAUL-"),"newhire_person_ssn")).withColumn("terminated_person_ssn", when(rawdf.cg_status == "2",rawdf.terminated_person_ssn))
#rawdf000 =rawdf_hash.withColumn("person_hire_date",to_date(col("person_hire_date"),"MM-dd-yyyy"))
rawdf000 =rawdf_hash.withColumn("hire_date", coalesce(*[to_date("person_hire_date", f) for f in ("MM/dd/yy","MM/dd/yyyy","MM-dd-yyyy", "yyyy-MM-dd","MMddyyyy")]))
rawdf000= rawdf000.withColumn("person_hire_dt",when((year(col("hire_date"))>1900) & (month(col("hire_date")) < 13) & (dayofmonth(col("hire_date")) < 32),col("hire_date")))
rawdf000= rawdf000.withColumn("termdate", coalesce(*[to_date("person_termination_date", f) for f in ("yyy/MM/dd","MM/dd/yy","MM/dd/yyyy","MM-dd-yyyy", "yyyy-MM-dd")]))
#rawdf000= rawdf000.withColumn("terminateddate",when(year(col("terminateddate"))>1900,col("termdate")))
rawdf000= rawdf000.withColumn("termdate",when((year(col("termdate"))>1900) & (month(col("termdate")) < 13) & (dayofmonth(col("termdate")) < 32),col("termdate")))
rawdf000.createOrReplaceTempView("cg_qual_agency_person")
rawdf000 = spark.sql("select * from cg_qual_agency_person where (cg_status='1' and person_hire_dt is not null and person_hire_dt <> '') or (cg_status='2' and terminated_person_id is not null and termdate is not null)")
#.filter(col("person_hire_date").isNotNull())

#rawdf000.select("hire_date").show()
rawdf00 =rawdf000.withColumn("createdby",(lit("cg_employmentrelationship_gluejob")))
rawdf01 =rawdf00.withColumn("categorycode",lit("SHCA"))
rawdf_filtered =rawdf01.withColumn("person_workercategory", lit("Standard HCA"))
rawdf_filtered01 =rawdf_filtered.withColumn("employername",when(rawdf.employername == "1","102")
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
#rawdf_filtered01.select("employername").show() 
rawdf_filtered02 =rawdf_filtered01.withColumn("cg_status",when(rawdf_filtered01.cg_status == "1", "Active")
                        .when(rawdf_filtered01.cg_status == "2", "Terminated")
                        .when(rawdf_filtered01.cg_status.isNull() ,""))

#rawdf_filtered02.select("cg_status").show()  
print(type(rawdf_filtered02))
print(type(rawdf_filtered01))
rawdf_filtered03 =rawdf_filtered02.withColumn("branchid",when(rawdf_filtered02.employername == "102", 390)
                        .when(rawdf_filtered02.employername == "113", 398)
                        .when(rawdf_filtered02.employername == "105", 391)
                        .when(rawdf_filtered02.employername == "106", 223)
                        .when(rawdf_filtered02.employername == "116", 201)
                        .when(rawdf_filtered02.employername == "107", 393)
                        .when(rawdf_filtered02.employername == "117", 202)
                        .when(rawdf_filtered02.employername == "109", 394)
                        .when(rawdf_filtered02.employername == "104", 388)
                        .when(rawdf_filtered02.employername == "111", 396)
                        .when(rawdf_filtered02.employername == "108", 246)
                        .when(rawdf_filtered02.employername == "119", 284)
                        .when(rawdf_filtered02.employername == "120", 205)
                        .when(rawdf_filtered02.employername == "121", 206))
                        #.when(rawdf_filtered02.employername == "426", 429))  
#rawdf_filtered03.select("branchid").show()
rawdf_filtered04 =rawdf_filtered03.withColumn("sourcekey",when(rawdf_filtered03.branchid == 390, "Addus")
                        .when(rawdf_filtered03.branchid == 398, "AllWaysCaring")
                        .when(rawdf_filtered03.branchid == 391, "Amicable")
                        .when(rawdf_filtered03.branchid == 223 ,"CCS")
                        .when(rawdf_filtered03.branchid == 201, "CDM")
                        .when(rawdf_filtered03.branchid == 393, "Chesterfield")
                        .when(rawdf_filtered03.branchid == 202, "CoastalCap")
                        .when(rawdf_filtered03.branchid == 394, "ConcernedCitizens")
                        .when(rawdf_filtered03.branchid == 388, "FirstChoice")
                        .when(rawdf_filtered03.branchid == 396, "FullLife")
                        .when(rawdf_filtered03.branchid == 246, "KWA")
                        .when(rawdf_filtered03.branchid == 284, "OlyCap")
                        .when(rawdf_filtered03.branchid == 205, "Seamar")
                        .when(rawdf_filtered03.branchid == 206, "SLR"))
'''                        
rawdf_filtered05 = rawdf_filtered04.withColumn("priority",when(rawdf_filtered04.person_workercategory == "Standard HCA", "1"))\

                        .when(rawdf_filtered04.person_workercategory == "Standard HCA", "1")
                        .when(rawdf_filtered04.person_workercategory == "DDD Parent Provider", "3")
                        .when(rawdf_filtered04.person_workercategory == "Non-DDD Parent Provider" ,"4")
                        .when(rawdf_filtered04.person_workercategory == "Respite", "5")
                        .when(rawdf_filtered04.person_workercategory == "Limited Service Provider", "6"))\
                        .withColumn("Role",(lit("CARE")))\
                        .withColumn("isoverridee",(lit("0")))\
                        .withColumn("isignoredd",(lit("0")))\
                        
'''
rawdf_filtered05 = rawdf_filtered04.withColumn("priority",(lit("1")))\
                        .withColumn("Role",(lit("CARE")))\
                        .withColumn("isoverridee",(lit("0")))\
                        .withColumn("isignoredd",(lit("0")))
#rawdf_filtered05.select("sourcekey","priority","Role","termdate","firstname","person_termination_date","person_hire_dt","isoverridee","isignoredd").show()
#rawdf_filtered05.printSchema()
                        
########## Remove Nulls from EmployerId, EmployeeId, BranchId
#rawdf_filtered04= rawdf_filtered04.filter(col("employeeid").isNotNull())
rawdf_filtered05= rawdf_filtered05.filter(col("employername").isNotNull())
rawdf_filtered05= rawdf_filtered05.filter(col("branchid").isNotNull())
rawdf_filtered05= rawdf_filtered05.filter(col("firstname").isNotNull())




#rawdf_filtered05.select("sourcekey","person_hire_date","terminated_person_id","termdate").show()
rawdf_hash = rawdf_filtered05.withColumn("source",concat(col("sourcekey"),lit("-"), substring(col("MD5"), 1, 10)))
rawdf_hash= rawdf_hash.withColumn("authstart",col("person_hire_dt")).withColumn("authend",col("termdate"))
#rawdf_hash.createOrReplaceTempView("cg_qual_agency_person")
#spark.sql("select * from cg_qual_agency_person where sourcekey is null").show()
newdatasource0 = DynamicFrame.fromDF(rawdf_hash, glueContext, "newdatasource0")  
applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("terminated_person_id", "string", "agencyid", "long"),("person_hire_dt", "date", "hiredate", "timestamp"),("createdby", "string", "createdby", "string"), ("person_workercategory", "string", "workercategory", "string"), ("branchid", "int", "branchid", "int"),("categorycode", "string", "categorycode", "string"), ("cg_status", "string", "empstatus", "string"),("Role", "string", "Role", "string"), ("termdate", "date", "terminationdate", "string"),("source", "string", "source", "string"), ("employername", "string", "employerid", "string"),("priority", "string", "priority", "int"),("homephone", "string", "homephone", "string"),("authstart", "date", "authstart", "string"),("authend", "date", "authend", "string"),("authstart", "date", "trackingdate", "string"),("filemodifieddate", "timestamp", "filedate", "date")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["branchid", "isoverride", "role", "isignored", "categorycode", "createddate", "filedate", "authend", "modifieddate", "source", "relationshipid", "employeeid", "hiredate", "employerid", "createdby", "authstart", "trackingdate", "modified", "personid", "relationship", "empstatus", "terminated", "employer_id", "terminationdate", "recordcreateddate", "created", "workercategory", "priority", "recordmodifieddate"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["branchid", "isoverride", "role", "isignored", "categorycode", "createddate", "filedate", "authend", "modifieddate", "source", "relationshipid", "employeeid", "hiredate", "employerid", "createdby", "authstart", "trackingdate", "modified", "personid", "relationship", "empstatus","Role", "employer_id","agencyid", "terminationdate", "recordcreateddate", "created", "workercategory", "priority", "recordmodifieddate",], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "postgresrds", table_name = "stagingb2bdevdb_staging_employmentrelationshiphistory", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_staging_employmentrelationshiphistory", transformation_ctx = "resolvechoice3")
#resolvechoice3.toDF().select("hiredate").show()
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
#resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
resolvechoice4 = DropNullFields.apply(frame = resolvechoice3,  transformation_ctx = "resolvechoice4")
df = resolvechoice4.toDF()
#df.select("categorycode","workercategory","priority").show()
print("validdf count" ,df.count())
## @type: DataSink
## @args: [database = "postgresrds", table_name = "stagingb2bdevdb_staging_employmentrelationshiphistory", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_staging_employmentrelationshiphistory", transformation_ctx = "datasink5")

print('done')

job.commit()