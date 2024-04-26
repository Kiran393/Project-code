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
    table_name="b2bds_staging_vw_getproviderinfo",
    transformation_ctx="PostgreSQLtable_node1",
)

df = PostgreSQLtable_node1.toDF()
df.printSchema()

df = df.withColumn("employerid", col("employerid").cast("int")).withColumn("createdby",lit("cdwa-employementrelationshiphistory-gluejob"))
df.select("employerid").show()
df.printSchema()
PostgreSQLtable_node2 = DynamicFrame.fromDF(df, glueContext, "PostgreSQLtable_node2")



# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=PostgreSQLtable_node2,
    mappings=[
        ("sourcekey", "string", "source", "string"),
        ("classificationcode", "string", "categorycode", "string"),
        ("filedate", "date", "filedate", "date"),
        ("initialtrackingdate", "date", "trackingdate", "string"),
        ("createdby", "string", "createdby", "string"),
        ("hire_date", "date", "hiredate", "timestamp"),
        ("priority", "int", "priority", "int"),
        ("employerid", "int", "employerid", "string"),
        ("authorized_end_date", "timestamp", "authend", "string"),
        ("termination_date", "timestamp", "terminationdate", "string"),
        ("authorized_start_date", "timestamp", "authstart", "string"),
        ("employee_id", "decimal", "employeeid", "bigint"),
        ("employee_classification", "string", "workercategory", "string"),
        ("relationship", "string", "relationship", "string"),
        ("empstatus", "string", "empstatus", "string"),
        #("person_id", "string", "personid", "bigint"),
    ],
    transformation_ctx="ApplyMapping_node2",
)
ApplyMapping_node2.toDF().show()


# Script generated for node PostgreSQL table
PostgreSQLtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="seiubg-rds-b2bds",
    table_name="b2bds_staging_employmentrelationshiphistory",
    transformation_ctx="PostgreSQLtable_node3",
)

job.commit()
