import cryptography
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name,to_date,regexp_extract
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import substring, expr, length
from pyspark.sql.functions import concat, col, lit, when
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as fun
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3
import json
from cryptography.fernet import Fernet
import pandas
import csv


client10 = boto3.client('secretsmanager')

response1 = client10.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)

client2 = boto3.client('secretsmanager')

response2 = client10.get_secret_value(
    SecretId='prod/b2bds/glue/fernet'
)


database_secrets = json.loads(response1['SecretString'])
secrets = json.loads(response2['SecretString'])
key = secrets['key']

B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_NAME = 'b2bds'

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


Client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
Response = Client.list_objects_v2(Bucket='seiubg-b2bds-prod-feeds-fp7mk', Prefix='Inbound/raw/cdwa/ProviderInfo/')
objs = Response.get('Contents')
sourcekey = [obj['Key'] for obj in sorted(objs, key=get_last_modified) if obj['Key'].endswith('.csv')][0]
print(sourcekey)

targetkey = sourcekey.replace("/raw/", "/archive/")
#print(targetkey)


# Define Encrypt User Defined Function 
def encrypt_val(clear_text,MASTER_KEY):
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text
    
# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val

# Register UDF's
encrypt = udf(encrypt_val, StringType())
decrypt = udf(decrypt_val, StringType())


# Script generated for node Data Catalog table
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_staging_idcrosswalk", transformation_ctx = "datasource1")

idcrosswalkdf = datasource1.toDF()

idcrosswalkdf.show()

DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ",","optimizePerformance": True,},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://seiubg-b2bds-prod-feeds-fp7mk/"+sourcekey+""],
        "recurse": True,
    },
    transformation_ctx="DataCatalogtable_node1",
)

dataframe1 = DataCatalogtable_node1.toDF()
#if dataframe1.count() != 0:
df2=dataframe1.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in dataframe1.columns])


#dataframe1.printSchema()

dataframe2 = df2.withColumn("filename", input_file_name())
dataframe2 = dataframe2.withColumn("filenamenew",expr("substring(filename, 66, length(filename))"))
dataframe2 = dataframe2.withColumn("filemodifieddate",to_date(regexp_extract(col('filenamenew'), '(CDWA-O-BG-ProviderInfo-)(\d\d\d\d-\d\d-\d\d)(.csv)', 2),"yyyy-MM-dd"))
dataframe2.printSchema()
if dataframe2.count() != 0:

    dataframe2.createOrReplaceTempView("temp1")
    new_df1 = spark.sql("SELECT DISTINCT p.filenamenew as filename FROM temp1 p WHERE p.filemodifieddate = ((SELECT MAX(t2.filemodifieddate) AS MAX FROM temp1 t2))")
    new_df1.show()
    pandas_df1 = new_df1.toPandas()
    newsuffix = pandas_df1.iat[0,0]
    #dataframe2.filter("filemodifieddate is null").show(truncate = False)
    
    new_cols=(column.replace('"', '').strip() for column in dataframe2.columns)
    df2 = dataframe2.toDF(*new_cols)
    
    #df2.printSchema()
    #df2.show(truncate = False)
    
    df3 = df2.withColumn("Client ID", regexp_replace("Client ID",'"',''))
    df3 = df3.withColumn("Client ID", regexp_replace("Client ID",'/[\r\n\x0B\x0C\u0085\u2028\u2029]+/g',''))
    
    #df3 = df3.withColumn("Client ID", regexp_replace("Client ID",'/[\r\n\x0B\x0C\u0085\u2028\u2029]+/g',''))
    
    for colname in df3.columns:
      df3 = df3.withColumn(colname, fun.trim(fun.col(colname)))
    
    #df3.select('Client ID','filemodifieddate').show()
    
    df3 = df3.withColumn("Client ID",translate(translate('Client ID',"\r",""),"\n", ""))
    
    print('removing blank space ClientIDs')
    df4 = df3.withColumn("Client ID", when(col("Client ID").like(''), None).otherwise(col("Client ID")))
    
    print('removing newline ClientIDs')
    df4 = df4.withColumn("Client ID", when(col("Client ID").like("%\n%"), None).otherwise(col("Client ID")))
    
    print('removing strict newline ClientIDs')
    df4 = df4.withColumn("Client ID", when(col("Client ID").like("%\r\n%"), None).otherwise(col("Client ID")))
    
    print('removing blank space MiddleNames')
    df4 = df4.withColumn("Middle Name", when(col("Middle Name").like(''), None).otherwise(col("Middle Name")))
    
    print('removing blank space marital status')
    df4 = df4.withColumn("Marital Status", when(col("Marital Status").like(''), None).otherwise(col("Marital Status")))
    
    print('removing blank space emp_classification')
    df4 = df4.withColumn("Employee Classification", when(col("Employee Classification").like(''), None).otherwise(col("Employee Classification")))
    
    #print('formatting all dates')
    #df4 = df4.withColumn("Hire Date",to_date(col("Hire Date"),"YYYYMMDD")).withColumn("Classification Start Date",to_date(col("Classification Start Date"),"YYYYMMDD")).withColumn("Termination Date",to_date(col("Termination Date"),"YYYYMMDD"))
    
    df4 = df4.withColumn("personid",col("person id"))
    print("cdwa df :")
    #df4.show(truncate = False)
    df4.createOrReplaceTempView ("cdwa")
    print("printing cdwa schema")
    df4.printSchema()
    idcrosswalkdf.createOrReplaceTempView("idcrosswalk")
    print("printing idcrosswalk schema")
    idcrosswalkdf.printSchema()
    ##Check if there are any duplicate ssn
    idcrosswalkdeencrypted_df =idcrosswalkdf.withColumn("decryptssn", decrypt("fullssn",lit(key)))
    idcrosswalkdeencrypted_df.createOrReplaceTempView("idcrosswalkdecrypt")
    joindf_ssn = spark.sql("""select * from cdwa left join idcrosswalkdecrypt on cdwa.ssn = idcrosswalkdecrypt.decryptssn
                            where cdwa.personid != idcrosswalkdecrypt.cdwaid""")
    print("Joined df with decrypted ssn:") #remove this print post testing
    joindf_ssn.show(truncate = False)
    print("count:")
    print(joindf_ssn.count())
    #If duplicate ssn's are found
    if(joindf_ssn.count() > 0):
        print("Found duplicates")
        personidlist = []
        personidlistdf = joindf_ssn.select("cdwa.personid")
        personidlist = personidlistdf.rdd.map(lambda x: x.personid).collect()
        #personidlist.append(joindf_ssn.select("cdwa.personid"))
        print(personidlist)
        #filter with personids that have been found with duplicate ssn
        cdwadf_withssndups = df4.filter(df4.personid.isin(personidlist))
        cdwadf_withssndups = df4.filter(df4.personid.isin(personidlist))
        #cdwadf_withssndups= cdwadf_withssndups.withColumn(col("isvalid"),lit("false"))
        print("cdwadf with dup ssn:")
        cdwadf_withssndups.show(truncate = False)
        #write the records with duplicate ssn into error file
        pandas_df = cdwadf_withssndups.toPandas()
        pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/cdwa/ingestion_errorlog/duplicatessn-"+newsuffix+"", header=True, index=None,quotechar='"',encoding='utf-8', sep=',',quoting=csv.QUOTE_ALL)
        #filter with personids that have been found without duplicate ssn
        cdwadf_withoutssndups = df4.filter(~df4.personid.isin(personidlist))
        print("cdwadf without dup ssn:")
        cdwadf_withoutssndups.show(truncate = False)
    
        #check for ssn length 7,8,9 if not it is a hard error needs to be added into s3 error log
        cdwadf_ssn_len_err = cdwadf_withoutssndups.filter((length(col("ssn")) <7) | (length(col("ssn")) > 9))
        pandas_df1 = cdwadf_ssn_len_err.toPandas()
        pandas_df.append(pandas_df1)
        #Add to error log
        pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/cdwa/ingestion_errorlog/duplicatessn-"+newsuffix+"", header=True, index=None,quotechar='"',encoding='utf-8', sep=',',quoting=csv.QUOTE_ALL)
        
        # drop columns with all null values
        pandas_df = pandas_df.dropna(axis='columns', how='all')
        df = spark.createDataFrame(pandas_df)
        dataframe2 = DynamicFrame.fromDF(df, glueContext, "dataframe2")
        applymapping1 = ApplyMapping.apply(frame = dataframe2, mappings = [("filemodifieddate", "string", "filedate", "string"),("filenamenew", "string", "filename", "string"),("Person ID", "string", "personid", "string")], transformation_ctx = "applymapping1")
        df = applymapping1.toDF()
        mode = "overwrite"
        url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
        properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
        df.write.option("truncate",True).jdbc(url=url, table="raw.cdwaduplicatessnerrors", mode=mode, properties=properties)
        
        
        cdwadf = cdwadf_withoutssndups
    else:
        cdwadf = df4
    cdwadf.createOrReplaceTempView("cdwa")   
    #Adding record/new caregivers to idcrosswalk table if the cdwaid does not exist in the idcrosswalk table
    idcrosswalktupdated_df = spark.sql("select Distinct * from cdwa A where NOT EXISTS (Select 1 from idcrosswalk B where A.personid = B.cdwaid)")
    #idcrosswalktupdated_df.show(truncate = False)
    idcrosswalkencrypted_df =idcrosswalktupdated_df.filter("ssn is NOT NULL").withColumn("fullssn", encrypt("ssn",lit(key)))
    #idcrosswalkencrypted_df.select("fullssn").show()
    newdatasource0 = DynamicFrame.fromDF(idcrosswalkencrypted_df, glueContext, "newdatasource0")
    
    #Left Pad the ssn with 0 to make it 9 digits if < 9 digits
    cdwadf = cdwadf.withColumn("ssn", format_string("%09d", col("ssn").cast('int')))
    #last 4 digits of ssn
    cdwadf = cdwadf.withColumn("ssn",substring("ssn",6,4))
    
    cdwadf.createOrReplaceTempView("Tab")
    
    print("ssn less than 4", spark.sql("select * from Tab where length(ssn) < 4 ").show())
    
    #cdwadf.select("ssn").show()
    cdwadf.printSchema()
    
    # Script generated for node ApplyMapping
    ApplyMapping_node3 = ApplyMapping.apply(
        frame=newdatasource0,
        mappings=[
            ("fullssn", "string", "fullssn", "string"),
            ("personid", "string", "cdwaid", "long"),
        ],
        transformation_ctx="ApplyMapping_node3",
    )
    
    DataCatalogtable_node2 = DynamicFrame.fromDF(cdwadf, glueContext, "DataCatalogtable_node2")
    
    # Script generated for node ApplyMapping
    ApplyMapping_node2 = ApplyMapping.apply(
        frame=DataCatalogtable_node2,
        mappings=[
            ("employee id", "string", "employee_id", "bigint"),
            ("personid", "string", "personid", "string"),
            ("first name", "string", "first_name", "string"),
            ("Middle Name", "string", "middle_name", "string"),
            ("last name", "string", "last_name", "string"),
            ("ssn", "string", "ssn", "string"),
            ("dob", "string", "dob", "int"),
            ("gender", "string", "gender", "string"),
            ("race", "string", "race", "string"),
            ("language", "string", "language", "string"),
            ("Marital Status", "string", "marital_status", "string"),
            ("phone 1", "string", "phone_1", "long"),
            ("phone 2", "string", "phone_2", "long"),
            ("email", "string", "email", "string"),
            ("mailing add 1", "string", "mailing_add_1", "string"),
            ("mailing add 2", "string", "mailing_add_2", "string"),
            ("mailing city", "string", "mailing_city", "string"),
            ("mailing state", "string", "mailing_state", "string"),
            ("mailing zip", "string", "mailing_zip", "string"),
            ("physical add 1", "string", "physical_add_1", "string"),
            ("physical add 2", "string", "physical_add_2", "string"),
            ("physical city", "string", "physical_city", "string"),
            ("physical state", "string", "physical_state", "string"),
            ("physical zip", "string", "physical_zip", "string"),
            ("Employee Classification", "string", "employee_classification", "string"),
            ("exempt status", "string", "exempt_status", "string"),
            ("Background Check Date", "string", "background_check_date", "int"),
            ("i&e date", "string", "ie_date", "int"),
            ("hire date", "string", "hire_date", "int"),
            ("classification start date", "string", "classification_start_date", "int"),
            ("carina eligible", "string", "carina_eligible", "string"),
            ("ahcas eligible", "string", "ahcas_eligible", "string"),
            ("o&s completed", "string", "os_completed", "int"),
            ("termination date", "string", "termination_date", "int"),
            ("authorized start date", "string", "authorized_start_date", "int"),
            ("authorized end date", "string", "authorized_end_date", "int"),
            ("client count", "string", "client_count", "int"),
            ("client relationship", "string", "client_relationship", "string"),
            ("Client ID", "string", "client_id", "string"),
            ("filenamenew", "string", "filename", "string"),
            ("Ethnicity", "string", "ethnicity", "string"),
            ("filemodifieddate", "string", "filemodifieddate", "date")
        ],
        transformation_ctx="ApplyMapping_node2",
    )
    
    final_df = ApplyMapping_node2.toDF()
    #final_df.select("filename","filemodifieddate").show()
    print("final_df count")
    print(final_df.count())
    
    final_df = final_df.withColumn("mailing_state",substring(upper(col("mailing_state")), 1, 2)).withColumn("physical_state",substring(upper(col("physical_state")), 1, 2))
    
    final_df.printSchema()
    #final_df.filter("personid = 3499101").show()
    
    
    
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
    properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
    final_df.write.option("truncate",True).jdbc(url=url, table="raw.cdwa", mode=mode, properties=properties)
    
    AWSGlueDataCatalog3 = glueContext.write_dynamic_frame.from_catalog(
        frame=ApplyMapping_node3,
        database="seiubg-rds-b2bds",
        table_name="b2bds_staging_idcrosswalk",
        transformation_ctx="AWSGlueDataCatalog3",
    )
    
    df2 = DynamicFrame.fromDF(joindf_ssn, glueContext, "df2")
    applymappin = ApplyMapping.apply(frame = df2, mappings = [("filemodifieddate", "string", "filedate", "string"),("filenamenew", "string", "filename", "string"),("Person ID", "string", "personid", "string")], transformation_ctx = "applymapping1")
    df = applymappin.toDF()
    df.write.option("truncate",True).jdbc(url=url, table="raw.cdwaduplicatessnerrors", mode=mode, properties=properties)

    
    copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
    bucket.copy(copy_source, targetkey)
    s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
    print('done')
    
   
else:
    dataframe2 = DynamicFrame.fromDF(dataframe2, glueContext, "dataframe2")
    applymapping1 = ApplyMapping.apply(frame = dataframe2, mappings = [("filemodifieddate", "string", "filemodifieddate", "date")], transformation_ctx = "applymapping1")
    

    selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["filemodifieddate"], transformation_ctx = "selectfields2")
    
    
 
 
 
    resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_cdwa", transformation_ctx = "resolvechoice3")
   
    resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
  
    
    final_df = resolvechoice4.toDF()
    print("final_df count")
    print(final_df.count())
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
    properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
    final_df.write.option("truncate",True).jdbc(url=url, table="raw.cdwa", mode=mode, properties=properties)
    
    copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
    bucket.copy(copy_source, targetkey)
    s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
    print('done')
    

job.commit()
