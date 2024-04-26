import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import json
from awsglue.job import Job
from pyspark.sql.functions import sha2,md5,concat_ws,col,current_timestamp,input_file_name,expr
from pyspark.sql.functions import col,when,to_timestamp,regexp_extract,to_date
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as fun
from pyspark.sql.types import IntegerType,BooleanType,DateType,StringType,LongType,BinaryType
import boto3

client10 = boto3.client('secretsmanager')

response1 = client10.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)


database_secrets = json.loads(response1['SecretString'])

B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_NAME = 'b2bds'


Client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')
get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
Response = Client.list_objects_v2(Bucket='seiubg-b2bds-prod-feeds-fp7mk', Prefix='Inbound/raw/credential/')
objs = Response.get('Contents')
sourcekey = [obj['Key'] for obj in sorted(objs, key=get_last_modified) if obj['Key'].endswith('.TXT')][0]
print(sourcekey)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "credentials", table_name = "credential", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "credentials", table_name = "credential", transformation_ctx = "datasource0")
datasource0 = glueContext.create_dynamic_frame.from_options(
format_options={
"quoteChar": '"',
"withHeader": True,
"separator": "|",
"optimizePerformance": True,
},
connection_type="s3",
format="csv",
connection_options={
"paths": ["s3://seiubg-b2bds-prod-feeds-fp7mk/"+sourcekey+""],
"recurse": True,
},
transformation_ctx="datasource0",
)
#datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "credentials", table_name = "credential_delta", transformation_ctx = "datasource0")

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential_delta", transformation_ctx = "datasource1")
## @type: ApplyMapping
## @args: [mapping = [("studentid", "long", "studentid", "int"), ("taxid", "long", "taxid", "int"), ("providernumber", "long", "providernumber", "int"), ("credentialnumber", "string", "credentialnumber", "string"), ("providernamedshs", "string", "providernamedshs", "string"), ("providernamedoh", "string", "providernamedoh", "string"), ("dateofbirth", "string", "dateofbirth", "string"), ("dateofhire", "string", "dateofhire", "date"), ("limitedenglishproficiencyindicator", "string", "limitedenglishproficiencyindicator", "string"), ("firstissuancedate", "string", "firstissuancedate", "date"), ("lastissuancedate", "string", "lastissuancedate", "date"), ("expirationdate", "string", "expirationdate", "date"), ("credentialtype", "string", "credentialtype", "string"), ("credentialstatus", "string", "credentialstatus", "string"), ("lepprovisionalcredential", "string", "lepprovisionalcredential", "string"), ("lepprovisionalcredentialissuedate", "string", "lepprovisionalcredentialissuedate", "date"), ("lepprovisionalcredentialexpirationdate", "string", "lepprovisionalcredentialexpirationdate", "date"), ("actiontaken", "string", "actiontaken", "string"), ("continuingeducationduedate", "string", "continuingeducationduedate", "date"), ("longtermcareworkertype", "string", "longtermcareworkertype", "string"), ("excludedlongtermcareworker", "string", "excludedlongtermcareworker", "string"), ("paymentdate", "string", "paymentdate", "date"), ("credentiallastdateofcontact", "string", "credentiallastdateofcontact", "date"), ("preferredlanguage", "string", "preferredlanguage", "string"), ("credentialstatusdate", "string", "credentialstatusdate", "date"), ("nctrainingcompletedate", "string", "nctrainingcompletedate", "date"), ("examscheduleddate", "string", "examscheduleddate", "date"), ("examscheduledsitecode", "string", "examscheduledsitecode", "string"), ("examscheduledsitename", "string", "examscheduledsitename", "string"), ("examtestertype", "string", "examtestertype", "string"), ("examemailaddress", "string", "examemailaddress", "string"), ("examdtrecdschedtestdate", "string", "examdtrecdschedtestdate", "date"), ("phonenum", "string", "phonenum", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
new_df = datasource0.toDF()
dataframe1 = new_df.withColumn("filename", input_file_name())
dataframe2 = dataframe1.withColumn("filenamenew",expr("substring(filename, 59, length(filename))"))
dataframe2 = dataframe2.withColumn("filemodifieddate",to_timestamp(regexp_extract(col('filenamenew'), '(01250_TPCertification_ADSAToTP_)(\d\d\d\d\d\d\d\d_\d\d\d\d\d\d)(.TXT)', 2),"yyyyMMdd_HHmmss"))
if dataframe2.count() != 0:
    df2=dataframe2.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in dataframe2.columns])
    dataframe2.select("filenamenew","filemodifieddate").show(truncate=False)
    new_df = df2.withColumn("studentid",col("studentid").cast(LongType()))\
    	  .withColumn("taxid",col("taxid").cast(IntegerType()))\
    	  .withColumn("providernumber",col("providernumber").cast(LongType()))
    
    #new_df = new_df.withColumn("hashidkey",sha2(concat_ws("||",*new_df.columns),256))
    new_df = new_df.withColumn("recordmodifieddate",current_timestamp())
    new_df = new_df.withColumn('hashidkey',md5(concat_ws("",col("studentid"),col("taxid"),col("providernumber"),col("credentialnumber"),col("providernamedshs"),col("providernamedoh"),col("dateofbirth"),col("dateofhire"),col("limitedenglishproficiencyindicator"),col("firstissuancedate"),col("lastissuancedate"),col("expirationdate"),col("credentialtype"),col("credentialstatus"),col("lepprovisionalcredential"),col("lepprovisionalcredentialissuedate"),col("lepprovisionalcredentialexpirationdate"),col("actiontaken"),col("continuingeducationduedate"),col("longtermcareworkertype"),col("excludedlongtermcareworker"),col("paymentdate"),col("credentiallastdateofcontact"),col("preferredlanguage"),col("credentialstatusdate"),col("nctrainingcompletedate"),col("examscheduleddate"),col("examscheduledsitecode"),col("examscheduledsitename"),col("examtestertype"),col("examemailaddress"),col("examdtrecdschedtestdate"),col("phonenum"))))
    
    #new_df = new_df.withColumn('hashidkey',hash(concat_ws("",col("studentid"),col("taxid"),col("providernumber"),col("credentialnumber"),col("providernamedshs"),col("providernamedoh"),col("dateofbirth"),col("dateofhire"),col("limitedenglishproficiencyindicator"),col("firstissuancedate"),col("lastissuancedate"),col("expirationdate"),col("credentialtype"),col("credentialstatus"),col("lepprovisionalcredential"),col("lepprovisionalcredentialissuedate"),col("lepprovisionalcredentialexpirationdate"),col("actiontaken"),col("continuingeducationduedate"),col("longtermcareworkertype"),col("excludedlongtermcareworker"),col("paymentdate"),col("credentiallastdateofcontact"),col("preferredlanguage"),col("credentialstatusdate"),col("nctrainingcompletedate"),col("examscheduleddate"),col("examscheduledsitecode"),col("examscheduledsitename"),col("examtestertype"),col("examemailaddress"),col("examdtrecdschedtestdate"),col("phonenum"))))
    
    #new_df = new_df.withColumn('hashidkey',md5(concat_ws("",col("studentid"),col("taxid"))))
    new_df.printSchema()
    print("printing debug")
    #new_df.filter(new_df.studentid == "375159905325").show()
    #new_df.select("hashidkey").show(truncate=False)
    
    #if df.count() > 0 and dfdelta.count = 0
    #dfdelta = df
    #datasource0withhashdelta = DynamicFrame.fromDF(dfdelta, glueContext, "datasource0withhashdelta")
    
    datasource0withhash = DynamicFrame.fromDF(new_df, glueContext, "datasource0withhash")
    
    applymapping1 = ApplyMapping.apply(frame = datasource0withhash, mappings = [("studentid", "long", "studentid", "long"), ("taxid", "int", "taxid", "int"), ("providernumber", "long", "providernumber", "long"), ("credentialnumber", "string", "credentialnumber", "string"), ("providernamedshs", "string", "providernamedshs", "string"), ("providernamedoh", "string", "providernamedoh", "string"), ("dateofbirth", "string", "dateofbirth", "string"),("filenamenew", "string", "filename", "string"),("filemodifieddate", "timestamp", "filemodifieddate", "timestamp"), ("dateofhire", "string", "dateofhire", "string"), ("limitedenglishproficiencyindicator", "string", "limitedenglishproficiencyindicator", "string"), ("firstissuancedate", "string", "firstissuancedate", "string"), ("lastissuancedate", "string", "lastissuancedate", "string"), ("expirationdate", "string", "expirationdate", "string"), ("credentialtype", "string", "credentialtype", "string"), ("credentialstatus", "string", "credentialstatus", "string"), ("lepprovisionalcredential", "string", "lepprovisionalcredential", "string"), ("lepprovisionalcredentialissuedate", "string", "lepprovisionalcredentialissuedate", "string"), ("lepprovisionalcredentialexpirationdate", "string", "lepprovisionalcredentialexpirationdate", "string"), ("actiontaken", "string", "actiontaken", "string"), ("continuingeducationduedate", "string", "continuingeducationduedate", "string"), ("longtermcareworkertype", "string", "longtermcareworkertype", "string"), ("excludedlongtermcareworker", "string", "excludedlongtermcareworker", "string"), ("paymentdate", "string", "paymentdate", "string"), ("credentiallastdateofcontact", "string", "credentiallastdateofcontact", "string"), ("preferredlanguage", "string", "preferredlanguage", "string"), ("credentialstatusdate", "string", "credentialstatusdate", "string"), ("nctrainingcompletedate", "string", "nctrainingcompletedate", "string"), ("examscheduleddate", "string", "examscheduleddate", "string"), ("examscheduledsitecode", "string", "examscheduledsitecode", "string"), ("examscheduledsitename", "string", "examscheduledsitename", "string"), ("examtestertype", "string", "examtestertype", "string"), ("examemailaddress", "string", "examemailaddress", "string"), ("examdtrecdschedtestdate", "string", "examdtrecdschedtestdate", "string"), ("phonenum", "string", "phonenum", "string"),("hashidkey","string","hashidkey","string"),("recordmodifieddate","timestamp","recordmodifieddate","timestamp")], transformation_ctx = "applymapping1")
    ## @type: SelectFields
    ## @args: [paths = ["credentialtype", "paymentdate", "limitedenglishproficiencyindicator", "credentialstatus", "dateofhire", "longtermcareworkertype", "actiontaken", "excludedlongtermcareworker", "taxid", "examscheduledsitename", "providernumber", "examdtrecdschedtestdate", "providernamedoh", "examemailaddress", "providernamedshs", "dateofbirth", "credentiallastdateofcontact", "lastissuancedate", "examtestertype", "examscheduleddate", "phonenum", "credentialnumber", "expirationdate", "lepprovisionalcredential", "continuingeducationduedate", "studentid", "examscheduledsitecode", "lepprovisionalcredentialexpirationdate", "lepprovisionalcredentialissuedate", "preferredlanguage", "firstissuancedate", "credentialstatusdate", "nctrainingcompletedate"], transformation_ctx = "selectfields2"]
    ## @return: selectfields2
    ## @inputs: [frame = applymapping1]
    selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["credentialtype", "paymentdate", "limitedenglishproficiencyindicator", "credentialstatus", "dateofhire", "longtermcareworkertype", "actiontaken", "excludedlongtermcareworker", "taxid", "examscheduledsitename", "providernumber", "examdtrecdschedtestdate", "providernamedoh", "examemailaddress", "providernamedshs", "dateofbirth", "credentiallastdateofcontact", "lastissuancedate", "examtestertype", "examscheduleddate", "phonenum", "credentialnumber", "expirationdate", "lepprovisionalcredential", "continuingeducationduedate", "studentid","filename","filemodifieddate", "examscheduledsitecode", "lepprovisionalcredentialexpirationdate", "lepprovisionalcredentialissuedate", "preferredlanguage", "firstissuancedate", "credentialstatusdate", "nctrainingcompletedate","hashidkey","modified"], transformation_ctx = "selectfields2")
    ## @type: ResolveChoice
    ## @args: [choice = "MATCH_CATALOG", database = "postgresrds", table_name = "rawb2bdevdb_raw_credential", transformation_ctx = "resolvechoice3"]
    ## @return: resolvechoice3
    ## @inputs: [frame = selectfields2]
    resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential", transformation_ctx = "resolvechoice3")
    ## @type: ResolveChoice
    ## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
    ## @return: resolvechoice4
    ## @inputs: [frame = resolvechoice3]
    resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
    ## @type: DataSink
    ## @args: [database = "postgresrds", table_name = "rawb2bdevdb_raw_credential", transformation_ctx = "datasink5"]
    ## @return: datasink5
    ## @inputs: [frame = resolvechoice4]
    #Add truncate and overrite
    final_df = resolvechoice4.toDF()
    final_df.select("filemodifieddate","filename","studentid").show()
    print("final_df count")
    print(final_df.count())
    final_df.createOrReplaceTempView("Tab")
    
    spark.sql("select distinct filemodifieddate from Tab").show()
    
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
    properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
    final_df.write.option("truncate",True).jdbc(url=url, table="raw.credential", mode=mode, properties=properties)
    
    print("completed writing")
    #below is replaced with truncate and load for raw.credential table
    #datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bdevdb_raw_credential", transformation_ctx = "datasink5")
    # first full load
    deltadf = datasource1.toDF()
    #deltadf.show(truncate=False)
    print("delta count")
    print(deltadf.count())
    
    
    
    targetkey = sourcekey.replace("/raw/", "/archive/")
    print(sourcekey)
    print(targetkey)
    copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
    bucket.copy(copy_source, targetkey)
    s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
    
    
    if deltadf.count() == 0 : 
            datasink6 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential_delta", transformation_ctx = "datasink6")
else:
    dataframe2 = DynamicFrame.fromDF(dataframe2, glueContext, "dataframe2")
    applymapping1 = ApplyMapping.apply(frame = dataframe2, mappings = [("filemodifieddate", "string", "filemodifieddate", "date")], transformation_ctx = "applymapping1")
    

    selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["filemodifieddate"], transformation_ctx = "selectfields2")
    
    
 
 
 
    resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential", transformation_ctx = "resolvechoice3")
   
    resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")
  
    
    final_df = resolvechoice4.toDF()
    print("final_df count")
    print(final_df.count())
    mode = "overwrite"
    url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
    properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
    final_df.write.option("truncate",True).jdbc(url=url, table="raw.credential", mode=mode, properties=properties)
    
    targetkey = sourcekey.replace("/raw/", "/archive/")
    print(sourcekey)
    print(targetkey)
    copy_source = {  'Bucket': 'seiubg-b2bds-prod-feeds-fp7mk', 'Key': sourcekey }
    bucket.copy(copy_source, targetkey)
    s3.Object("seiubg-b2bds-prod-feeds-fp7mk", sourcekey).delete()
job.commit()
