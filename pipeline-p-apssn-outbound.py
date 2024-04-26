import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas
from datetime import datetime
import boto3
import json
from cryptography.fernet import Fernet
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


#Get fernet key from secretsmanager
client1 = boto3.client('secretsmanager')

response1 = client1.get_secret_value(
    SecretId='prod/b2bds/glue/fernet'
)

secrets = json.loads(response1['SecretString'])
key = secrets['key']
f = Fernet(key)

# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val
    
# Register UDF's
decrypt = udf(decrypt_val, StringType())

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1651673286473 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person",
    transformation_ctx="AWSGlueDataCatalog_node1651673286473",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1651673320896 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_staging_idcrosswalk",
    transformation_ctx="AWSGlueDataCatalog_node1651673320896",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1651673351240 = glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_employmentrelationship",
    transformation_ctx="AWSGlueDataCatalog_node1651673351240",
)

idcrosswalkdf = AWSGlueDataCatalog_node1651673320896.toDF()
employmentrelationshipdf = AWSGlueDataCatalog_node1651673351240.toDF()

idcrosswalkdf = idcrosswalkdf.withColumn("fullssn",decrypt("fullssn",lit(key)))

idcrosswalkdf.select("fullssn").show(truncate = False)

idcrosswalkdf.createOrReplaceTempView("idcrosswalk")
employmentrelationshipdf.createOrReplaceTempView("employmentrelationship")

joindf = spark.sql("select Distinct i.personid,i.fullssn from idcrosswalk i left join employmentrelationship emp \
                    on i.personid = emp.personid and emp.role = 'CARE'\
                    where length(i.fullssn) >= 7 and i.personid is not null")
                    
#Add leading 0's to 7 and 8 digit SSN
joindf = joindf.withColumn("fullssn", format_string("%09d", col("fullssn").cast('int')))

print("joindf:")                    
joindf.show(truncate = False)
today = datetime.now()
suffix = today.strftime("%Y%m%d_%H%M%S")

print(suffix)

pandas_df = joindf.toPandas()
pandas_df['personid'] = pandas_df['personid'].astype(int)

pandas_df.head()
pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/dshs/01250_APSSN_TPTOADSA_"+suffix+"_All.txt", header=False, index=False, sep='|')


#job.commit()
