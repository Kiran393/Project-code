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

# Script generated for node PostgreSQL table
PostgreSQLtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_traininghistory",
    transformation_ctx="PostgreSQLtable_node1",
).toDF().createOrReplaceTempView("traininghistory")

# Script generated for node PostgreSQL
PostgreSQL_node1648005308401 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_transcript",
    transformation_ctx="PostgreSQL_node1648005308401",
).toDF().createOrReplaceTempView("transcript")

allcompletions = spark.sql("select curr.*,prev.courseid as prevcourseid,ROW_NUMBER () OVER ( PARTITION BY curr.personid, trim(regexp_replace(curr.courseid,right(curr.courseid,7),'')) ORDER BY curr.completeddate ASC ) courserank   from traininghistory curr left join transcript prev on lower(trim(regexp_replace(curr.courseid,right(curr.courseid,7),''))) = lower(trim(regexp_replace(prev.courseid,right(prev.courseid,7),''))) and curr.personid = prev.personid where curr.personid is not null")


uniquiecompletions = allcompletions.where("prevcourseid is null and courserank = 1 and courseid is not null").selectExpr( "personid","completeddate","credithours","coursename","courseid","coursetype","courselanguage","trainingprogramcode","trainingprogramtype","instructorname","instructorid","trainingsource","recordcreateddate","recordmodifieddate","dshscourseid").distinct()

uniquiecompletions.show()

print("Unique Completions Count")
print(uniquiecompletions.count())

uniquiecompletionsdatasource = DynamicFrame.fromDF(uniquiecompletions, glueContext, "uniquiecompletionsdatasource")

# Script generated for node ApplyMapping
ApplyMapping_uniquiecompletions = ApplyMapping.apply(
    frame=uniquiecompletionsdatasource,
    mappings=[
        ("coursename", "string", "coursename", "string"),
        ("trainingprogramtype", "string", "trainingprogramtype", "string"),
        ("coursetype", "string", "coursetype", "string"),
        ("instructorname", "string", "instructorname", "string"),
        ("trainingprogramcode", "long", "trainingprogramcode", "long"),
        ("personid", "long", "personid", "long"),
        ("completeddate", "date", "completeddate", "date"),
        ("courseid", "string", "courseid", "string"),
        ("trainingsource", "string", "trainingsource", "string"),
        ("courselanguage", "string", "courselanguage", "string"),
        ("credithours", "double", "credithours", "double"),
        ("instructorid", "string", "instructorid", "string"),
        ("recordcreateddate", "timestamp", "recordcreateddate", "timestamp"),
        ("recordmodifieddate", "timestamp", "recordmodifieddate", "timestamp"),
         ("dshscourseid", "string", "dshscourseid", "string"),
    ],
    transformation_ctx="ApplyMapping_uniquiecompletions",
)

selectFields_staging_trainghistory = SelectFields.apply(
    frame=ApplyMapping_uniquiecompletions,
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
        "recordcreateddate",
        "recordmodifieddate",
        "dshscourseid"
    ],
    transformation_ctx="selectFields_staging_trainghistory",
)

uniquiecompletions_dropNullfields = DropNullFields.apply(frame = selectFields_staging_trainghistory)

# Script generated for staging traininghistory PostgreSQL table
PostgreSQLtable_staging_traininghistory = glueContext.write_dynamic_frame.from_catalog(
    frame=uniquiecompletions_dropNullfields,
    database="seiubg-rds-b2bds",
    table_name="b2bds_staging_traininghistory",
    transformation_ctx="PostgreSQLtable_node3",
)

personidnulls = spark.sql("select * from traininghistory where personid is null")

duplicatecompletions = allcompletions.where("prevcourseid is not null or courserank > 1 or courseid is null").selectExpr( "personid","completeddate","credithours","coursename","courseid","coursetype","courselanguage","trainingprogramcode","trainingprogramtype","instructorname","instructorid","trainingsource","recordcreateddate","recordmodifieddate","dshsid","qual_person_id","dshscourseid")

allduplicates = duplicatecompletions.unionByName(personidnulls,allowMissingColumns=True)

#duplicatecompletions.show()

print("Duplicate Completions Count to log.trainghistorylog")
print(duplicatecompletions.count())

duplicatecompletionsdatasource = DynamicFrame.fromDF(allduplicates, glueContext, "duplicatecompletionsdatasource")

# Script generated for node Apply Mapping
ApplyMapping_duplicatecompletionsandpersonidnulldatasource = ApplyMapping.apply(
    frame=duplicatecompletionsdatasource,
    mappings=[
        ("recordcreateddate", "timestamp", "recordcreateddate", "timestamp"),
        ("coursename", "string", "coursename", "string"),
        ("trainingprogramtype", "string", "trainingprogramtype", "string"),
        ("coursetype", "string", "coursetype", "string"),
        ("instructorname", "string", "instructorname", "string"),
        ("trainingprogramcode", "long", "trainingprogramcode", "long"),
        ("personid", "long", "personid", "long"),
        ("qual_person_id", "long", "qual_person_id", "long"),
        ("completeddate", "date", "completeddate", "date"),
        ("courseid", "string", "courseid", "string"),
        ("trainingsource", "string", "trainingsource", "string"),
        ("courselanguage", "string", "courselanguage", "string"),
        ("credithours", "double", "credithours", "double"),
        ("instructorid", "string", "instructorid", "string"),
        ("dshsid", "long", "dshsid", "long"),
        ("recordmodifieddate", "timestamp", "recordmodifieddate", "timestamp"),
         ("dshscourseid", "string", "dshscourseid", "string"),
    ],
    transformation_ctx="ApplyMapping_duplicatecompletionsandpersonidnulldatasource",
)

selectFields_log_trainghistorylog = SelectFields.apply(
    frame=ApplyMapping_duplicatecompletionsandpersonidnulldatasource,
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
        "dshsid",
        "qual_person_id",
        "recordcreateddate",
        "recordmodifieddate",
        "dshscourseid"
    ],
    transformation_ctx="selectFields_log_trainghistorylog",
)

duplicatecompletionsandpersonidnulldatasource_dropNullfields = DropNullFields.apply(frame = selectFields_log_trainghistorylog)


# Script generated for node PostgreSQL
PostgreSQL_node1648005129227 = glueContext.write_dynamic_frame.from_catalog(
    frame=duplicatecompletionsandpersonidnulldatasource_dropNullfields,
    database="seiubg-rds-b2bds",
    table_name="b2bds_logs_traininghistorylog",
    transformation_ctx="PostgreSQL_node1648005129227",
)

job.commit()
