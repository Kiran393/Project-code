import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "postgresrds", table_name = "b2bdevdb_raw_dohcompleted", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_dohcompleted", transformation_ctx = "DataSource0")
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_prod_dohcompleted", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("filefirstname", "string", "filefirstname", "string"), ("reconnotes", "string", "reconnotes", "string"), ("filelastname", "string", "filelastname", "string"), ("dateuploaded", "timestamp", "dateuploaded", "timestamp"), ("reconciled", "string", "reconciled", "string"), ("credentialstatus", "string", "credentialstatus", "string"), ("filereceiveddate", "string", "filereceiveddate", "string"), ("credentialnumber", "string", "credentialnumber", "string"), ("dohname", "string", "dohname", "string"), ("approvedinstructorcodeandname", "string", "approvedinstructorcodeandname", "string"), ("applicationdate", "string", "applicationdate", "string"), ("dateapprovedinstructorcodeandnameudfupdated", "string", "dateapprovedinstructorcodeandnameudfupdated", "string"), ("filename", "string", "filename", "string"), ("dategraduatedfor70hours", "string", "dategraduatedfor70hours", "string"), ("examfeepaid", "string", "examfeepaid", "string"), ("recordmodifieddate", "timestamp", "recordmodifieddate", "timestamp")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
#raw_df = DataSource0.toDF()
raw_df = DataSource0.toDF()
prod_df = DataSource1.toDF()
raw_df.createOrReplaceTempView ("raw_doh_complete")
prod_df.createOrReplaceTempView("prod_doh_complete")
updated_df = spark.sql("select Distinct * from raw_doh_complete A where NOT EXISTS (Select 1 from prod_doh_complete B where A.credentialnumber = B.credentialnumber)")
print("select for concat")
updated_df.select(split(col("approvedinstructorcodeandname"),"\s+").getItem(0)).show()
updated_df.select(concat(split(col("approvedinstructorcodeandname"),"\s+").getItem(1),lit(" "),split(col("approvedinstructorcodeandname"),"\s+").getItem(2))).show()
updated_df = updated_df.withColumn("filereceiveddate",to_date("filereceiveddate",'MM/dd/yyyy'))\
                       .withColumn("applicationdate",to_date(col('applicationdate'),'MM/dd/yyyy'))\
                       .withColumn("dateinstructorupdated",to_date(col('dateapprovedinstructorcodeandnameudfupdated'),'MM/dd/yyyy'))\
                       .withColumn("dategraduatedfor70hours",to_date(col('dategraduatedfor70hours'),'MM/dd/yyyy'))\
                       .withColumn("recordmodifieddate",current_timestamp())\
                       .withColumn("approvedinstructorcode",split(col("approvedinstructorcodeandname"),"\s+").getItem(0))\
                       .withColumn("approvedinstructorname",concat(split(col("approvedinstructorcodeandname"),"\s+").getItem(1),lit(" "),split(col("approvedinstructorcodeandname"),"\s+").getItem(2)))\
                       .withColumn("completeddate",to_date(regexp_extract(col("filename"),'(DSHS Benefit Training Completed File )(\d\d\d\d\d\d\d\d\d\d\d\d\d\d)(.csv)',2),"yyyyMMddHHmmss"))
updated_df.show(truncate = False)
updated_df.printSchema()
newdatasource0 = DynamicFrame.fromDF(updated_df, glueContext, "newdatasource0")
Transform1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("filefirstname", "string", "filefirstname", "string"),  ("filelastname", "string", "filelastname", "string"),  ("credentialstatus", "string", "credentialstatus", "string"), ("filereceiveddate", "date", "filereceiveddate", "date"), ("credentialnumber", "string", "credentialnumber", "string"), ("dohname", "string", "dohname", "string"), ("approvedinstructorcode", "string", "approvedinstructorcode", "string"),("approvedinstructorname", "string", "approvedinstructorname", "string"), ("applicationdate", "date", "applicationdate", "date"),("completeddate","date","completeddate","date"),("dateinstructorupdated", "date", "dateinstructorupdated", "date"), ("filename", "string", "filename", "string"), ("dategraduatedfor70hours", "date", "dategraduatedfor70hours", "date"), ("recordmodifieddate", "timestamp", "recordmodifieddate", "timestamp")], transformation_ctx = "Transform1")
## @type: SelectFields
## @args: [paths = ["filefirstname", "reconnotes", "filelastname", "dateuploaded", "reconciled", "credentialstatus", "filereceiveddate", "credentialnumber", "dohname", "approvedinstructorcodeandname", "applicationdate", "dateapprovedinstructorcodeandnameudfupdated", "filename", "dategraduatedfor70hours", "examfeepaid", "recordmodifieddate"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = Transform1]
Transform0 = SelectFields.apply(frame = Transform1, paths = ["filefirstname", "filelastname", "credentialstatus","completeddate", "filereceiveddate", "credentialnumber", "dohname", "approvedinstructorname","approvedinstructorcode", "applicationdate", "dateinstructorupdated",  "dategraduatedfor70hours", "recordmodifieddate"], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [database = "postgresrds", table_name = "b2bdevdb_prod_dohcompleted", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
dyf_dropNullfields = DropNullFields.apply(frame = Transform0)


DataSink0 = glueContext.write_dynamic_frame.from_catalog(frame = dyf_dropNullfields, database = "seiubg-rds-b2bds", table_name = "b2bds_prod_dohcompleted", transformation_ctx = "DataSink0")
#uncomment the below line if it is commented out for testing

job.commit()