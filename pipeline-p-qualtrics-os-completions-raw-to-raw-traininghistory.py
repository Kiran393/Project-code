import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Join


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

agency_person = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_os_qual_agency_person", transformation_ctx = "datasource0").toDF()
agency_person = agency_person.filter("isdelta == 'true'")
agency_person = agency_person.withColumn("os_completion_date", coalesce(*[to_date("os_completion_date", f) for f in ("MM/dd/yyyy","MM-dd-yyyy", "yyyy-MM-dd","MMddyyyy")]))

agency_person.createOrReplaceTempView("os_qual_agency_person")


# prod_person = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_prod_person", transformation_ctx = "datasource1").toDF().createOrReplaceTempView("person")


glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person"
).toDF().selectExpr("*").createOrReplaceTempView("person")


spark.sql("with oscompletions as (select person_id,os_completion_date, filemodifieddate, finished , ROW_NUMBER () OVER ( PARTITION BY person_id ORDER BY filemodifieddate DESC, os_completion_date ASC ) completionsrank from os_qual_agency_person) select cast(person_id as long) as personid,os_completion_date, filemodifieddate, finished from oscompletions where completionsrank = 1 ").createOrReplaceTempView("os_qual_agency_person")

traininghistory = spark.sql("with oscompletions as (select personid,os_completion_date, filemodifieddate from os_qual_agency_person where finished = '1' ), person as (select distinct dshsid , personid from person where dshsid is not null and personid is not null) select pr.personid ,o.os_completion_date as completeddate,'Orientation & Safety EN' as coursename ,5 as credithours ,'1000065ONLEN02' as courseid ,100 as trainingprogramcode ,'Orientation & Safety' as trainingprogramtype ,'EN' as courselanguage,'ONL' as coursetype,'QUAL' as trainingsource, 'instructor' as instructorname, 'instructor' as instructorid ,pr.dshsid,o.filemodifieddate from oscompletions o join person pr on (o.personid = pr.dshsid or o.personid = pr.personid)")


traininghistory.show()
traininghistory.printSchema()

newdatasource0 = DynamicFrame.fromDF(traininghistory, glueContext, "newdatasource0")                            
                            


applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [
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
    ("filemodifieddate", "timestamp", "filedate", "timestamp"),
    ], transformation_ctx = "applymapping1")
                       
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["personid","dshsid","courseid","completeddate", "coursename","credithours", "trainingprogramtype", "trainingprogramcode", "coursetype", "courselanguage","instructorname","instructorid", "trainingsource","filedate"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_traininghistory", transformation_ctx = "resolvechoice3")


resolvechoice4 = DropNullFields.apply(frame = resolvechoice3, transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_raw_traininghistory", transformation_ctx = "datasink5")

job.commit()
