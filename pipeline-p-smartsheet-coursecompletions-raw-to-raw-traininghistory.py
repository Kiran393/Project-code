import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node PostgreSQL table
raw_ss_course_completions = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_ss_course_completion",
    transformation_ctx="raw_ss_course_completions",
).toDF().filter("isdelta == 'true'").createOrReplaceTempView("raw_ss_course_completions")

# Script generated for node PostgreSQL
course_catalog = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_coursecatalog",
    transformation_ctx="course_catalog",
).toDF().createOrReplaceTempView("coursecatalog")

# Script generated for node PostgreSQL
instructor = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_instructor",
    transformation_ctx="instructor",
).toDF().createOrReplaceTempView("instructor")


## Removing duplicate courses, if the course is offered in both online(ONL) and also Instructor-Led (ILT) the we pick the ILT courseid as default
spark.sql("with coursecatalog as (select *, ROW_NUMBER () OVER ( PARTITION BY curr.coursename, curr.courselanguage,curr.trainingprogramcode ORDER BY curr.coursetype ASC ) courserank from coursecatalog curr) select  coursename ,credithours ,trainingprogramtype, trainingprogramcode, coursetype, courselanguage,courseversion,courseid,dshscourseid from coursecatalog where courserank = 1").createOrReplaceTempView("coursecatalog")

## Capturing only the active instuctors from the instructor table
spark.sql("select distinct trim(concat(trim(firstname),' ',trim(lastname))) as instructorname , dshsinstructorid  as instructorid from instructor where isactive = 'true'").createOrReplaceTempView("instructor")

## Removing the duplicate course completions received from multiple sheets and only preserving completions in the earliest sheet date
spark.sql("with ss_raw as (select  sc.learner_id ,sc.class_title,sc.date ,sc.instructor ,to_date(element_at(split(sc.sheet_name, '-'),2),'yyyyMMdd') as sheetdate, ROW_NUMBER () OVER ( PARTITION BY sc.learner_id, sc.class_title ORDER BY to_date(element_at(split(sc.sheet_name, '-'),2),'yyyyMMdd') DESC, sc.date ASC ) courserank from raw_ss_course_completions sc  where sc.attendance = 'Attended'  order by sc.class_title desc, sc.date) select * from ss_raw where  courserank = 1").createOrReplaceTempView("raw_ss_course_completions")

## Combining coursecatalog , instructor and completion details   
sscompletionsdf = spark.sql("select distinct sc.learner_id ,sc.class_title ,sc.date as completiondate ,tc.credithours,tc.trainingprogramtype,tc.trainingprogramcode ,tc.courseid ,tc.coursetype ,tc.courselanguage ,tc.dshscourseid, sc.instructor as instructorname,ins.instructorid,'SMARTSHEET' as trainingsource, current_timestamp() as filemodifieddate from raw_ss_course_completions sc left join coursecatalog tc on lower(trim(sc.class_title)) = lower(concat(trim(tc.coursename),' ',tc.courselanguage)) left join instructor ins on lower(trim(ins.instructorname)) = lower(trim(sc.instructor))")

print("Smartsheets Course completions count")
print(sscompletionsdf.count())
# sscompletionsdf.show()
# sscompletionsdf.printSchema()


newdatasource = DynamicFrame.fromDF(sscompletionsdf, glueContext, "newdatasource")
# Script generated for node Apply Mapping

ApplyMapping_node1646956720451 = ApplyMapping.apply(
    frame=newdatasource,
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
    transformation_ctx="b2bdevdb_raw_traininghistory",
)

dyf_dropNullfields = DropNullFields.apply(frame = selectFields_raw_trainghistory)

#Script generated for node PostgreSQL
PostgreSQL_node1647971538967 = glueContext.write_dynamic_frame.from_catalog(
    frame=dyf_dropNullfields,
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_traininghistory",
    transformation_ctx="PostgreSQL_node1647971538967",
)

job.commit()
