import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date,col,coalesce,when,expr
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# Script for node PostgreSQL read from raw exam table
raw_examcdf = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_exam",
    transformation_ctx="raw_examcdf",
)

# Converting the multiple formats of date to single date datatype
raw_examdf = raw_examcdf.toDF().withColumn("examdate", coalesce(*[to_date("examdate", f) for f in ("MM/dd/yy", "MM/dd/yyyy", "MM-dd-yyyy", "MMddyyyy")]))

# inferng balanks to nulls
raw_examdf = raw_examdf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in raw_examdf.columns])

#creating hash value based on all columns of the raw exam table
raw_examdf = raw_examdf.withColumn("rawhash",expr("md5(concat_ws('',studentid, taxid, credentialnumber, examdate, examstatus, examtitlefromprometrics, testlanguage, testsite, sitename, rolesandresponsibilitiesofthehomecareaide, supportingphysicalandpsychosocialwellbeing, promotingsafety, handwashingskillresult, randomskill1result, randomskill2result, randomskill3result,commoncarepracticesskillresult))"))
raw_examdf.createOrReplaceTempView("rawexam")

# Script for node PostgreSQL  read from prod exam table
prod_examcdf = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_exam",
    transformation_ctx="prod_examcdf",
)
# Converting the multiple formats of date to single date datatype
prod_examdf = prod_examcdf.toDF().withColumn("examdate", coalesce(*[to_date("examdate", f) for f in ("MM/dd/yy", "MM/dd/yyyy", "MM-dd-yyyy", "MMddyyyy")]))

# inferng balanks to nulls
prod_examdf = prod_examdf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in prod_examdf.columns])

#creating hash value based on all columns of the prod exam table
prod_examdf = prod_examdf.withColumn("prodhash",expr("md5(concat_ws('',studentid, taxid, credentialnumber, examdate, examstatus, examtitlefromprometrics, testlanguage, testsite, sitename, rolesandresponsibilitiesofthehomecareaide, supportingphysicalandpsychosocialwellbeing, promotingsafety, handwashingskillresult, randomskill1result, randomskill2result, randomskill3result,commoncarepracticesskillresult))"))
prod_examdf.createOrReplaceTempView("prodexam")


spark.sql("""
  select * from rawexam where trim(rawhash) not in (select trim(prodhash) from prodexam)
    """).show()
# Filtering the data which is not present in prod.exam table based on the its table hash
raw_examdf = spark.sql("""
  select * from rawexam where trim(rawhash) not in (select trim(prodhash) from prodexam)
    """)

rexamdatasource = DynamicFrame.fromDF(raw_examdf, glueContext, "datasource0")

# ApplyMapping to match the target table
ApplyMapping_node2 = ApplyMapping.apply(
    frame=rexamdatasource,
    mappings=[
        ("recordcreateddate", "timestamp", "recordcreateddate", "timestamp"),
        ("filename", "string", "filename", "string"),
        ("filemodifieddate", "timestamp", "filemodifieddate", "timestamp"),
        ("examstatus", "string", "examstatus", "string"),
        ("testlanguage", "string", "testlanguage", "string"),
        ("testsite", "string", "testsite", "string"),
        ("randomskill1result", "string", "randomskill1result", "string"),
        ("credentialnumber", "string", "credentialnumber", "string"),
        ("studentid", "string", "studentid", "string"),
        ("promotingsafety", "string", "promotingsafety", "string"),
        ("randomskill2result", "string", "randomskill2result", "string"),
        ("commoncarepracticesskillresult","string","commoncarepracticesskillresult","string"),
        ("randomskill3result", "string", "randomskill3result", "string"),
        ("taxid", "string", "taxid", "string"),
        ("examtitlefromprometrics", "string", "examtitlefromprometrics", "string"),
        ("sitename", "string", "sitename", "string"),
        ("examdate", "date", "examdate", "string"),
        ("supportingphysicalandpsychosocialwellbeing","string","supportingphysicalandpsychosocialwellbeing","string"),
        ("handwashingskillresult", "string", "handwashingskillresult", "string"),
        ("rolesandresponsibilitiesofthehomecareaide","string","rolesandresponsibilitiesofthehomecareaide","string"),
        ("recordmodifieddate", "timestamp", "recordmodifieddate", "timestamp"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script for node PostgreSQL to write to  prod exam table
PostgreSQLtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_exam",
    transformation_ctx="PostgreSQLtable_node3",
)

job.commit()

