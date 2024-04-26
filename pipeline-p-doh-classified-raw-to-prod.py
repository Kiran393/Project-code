import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import re


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
DataSource0 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_dohclassified",
    transformation_ctx="DataSource0",
)
DataSource1 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_dohclassified",
    transformation_ctx="DataSource1",
)

raw_df = DataSource0.toDF()
prod_df = DataSource1.toDF()
raw_df.createOrReplaceTempView ("raw_doh_classified")
prod_df.createOrReplaceTempView("prod_doh_classified")
updated_df = spark.sql("select Distinct * from raw_doh_classified A where NOT EXISTS (Select 1 from prod_doh_classified B where A.credentialnumber = B.credentialnumber)")

#updated_df = updated_df.withColumn("date",(re.search(r'\d{4}_\d{2}_\d{2}',col("filename"))))
#update_df = updated_df.withColumn("classifieddatenew",datetime.strptime((col("match"),'%Y_%m_%d')))
print("new column")
#updated_df.select(col("date")).show(truncate = False)
#match = re.search(r'\d{4}_\d{2}_\d{2}', text)
#date = datetime.strptime(match.group(), '%Y_%m_%d')
updated_df = updated_df.withColumn("filereceiveddate",to_date("filereceiveddate",'MM/dd/yyyy'))\
                       .withColumn("applicationdate",to_date(col('applicationdate'),'MM/dd/yyyy'))\
                       .withColumn("datedshsbenefitpaymentudfupdated",to_date(col('datedshsbenefitpaymentudfupdated'),'MM/dd/yyyy'))\
                       .withColumn("recordmodifieddate",current_timestamp())\
                       .withColumn("classifieddate",to_date(regexp_extract(col("filename"),'(DSHS Benefit Classified File )(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)',2),"yyyyMMddHHmmss"))\
                    
#Add filedate logic for classifieddate
#updated_df.printSchema()
updated_df.show()
newdatasource0 = DynamicFrame.fromDF(updated_df, glueContext, "newdatasource0")
# Script generated for node Apply Mapping
ApplyMapping_node1646956720451 = ApplyMapping.apply(
    frame=newdatasource0,
    mappings=[
        ("filefirstname", "string", "filefirstname", "string"),
        ("filelastname", "string", "filelastname", "string"),
        ("filename", "string", "filename", "string"),
        ("filereceiveddate", "date", "filereceiveddate", "date"),
        ("dohname", "string", "dohname", "string"),
        ("credentialnumber", "string", "credentialnumber", "string"),
        ("credentialstatus", "string", "credentialstatus", "string"),
        ("applicationdate", "date", "applicationdate", "date"),
        ("datedshsbenefitpaymentudfupdated", "date", "datedshsbenefitpaymentudfupdated", "date"),
        ("classifieddate", "date", "classifieddate", "date"),
        ("recordmodifieddate", "timestamp", "recordmodifieddate", "timestamp"),
    ],
    transformation_ctx="ApplyMapping_node1646956720451",
)

# Script generated for node Select Fields
SelectFields_node1646956724845 = SelectFields.apply(
    frame=ApplyMapping_node1646956720451,
    paths=[
        "filefirstname",
        "filelastname",
        "filereceiveddate",
        "dohname",
        "credentialnumber",
        "credentialstatus",
        "applicationdate",
        "datedshsbenefitpaymentudfupdated",
        "classifieddate",
        "recordmodifieddate",
    ],
    transformation_ctx="SelectFields_node1646956724845",
)
dyf_dropNullfields = DropNullFields.apply(frame = SelectFields_node1646956724845)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1646956743022 = glueContext.write_dynamic_frame.from_catalog(
    frame=dyf_dropNullfields,
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_dohclassified",
    transformation_ctx="AWSGlueDataCatalog_node1646956743022",
)
#uncomment the below line if it is commented out for testing
job.commit()