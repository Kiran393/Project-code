import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat,col,sha2,concat_ws,when,md5,current_timestamp,row_number
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

import boto3
import json


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



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
credential_df = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential", transformation_ctx = "datasource0").toDF()
credential_delta_df = glueContext.create_dynamic_frame.from_catalog(database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential_delta", transformation_ctx = "datasource1").toDF()
credential_df = credential_df.drop("hashidkey")
credential_delta_df = credential_delta_df.drop("hashidkey","newhashidkey","audit")

credential_df = credential_df.withColumn("newhashidkey",md5(concat_ws("",col("taxid"),col("providernumber"),col("credentialnumber"),col("providernamedshs"),col("providernamedoh"),col("dateofbirth"),col("dateofhire"),col("limitedenglishproficiencyindicator"),col("firstissuancedate"),col("lastissuancedate"),col("expirationdate"),col("credentialtype"),col("credentialstatus"),col("lepprovisionalcredential"),col("lepprovisionalcredentialissuedate"),col("lepprovisionalcredentialexpirationdate"),col("actiontaken"),col("continuingeducationduedate"),col("longtermcareworkertype"),col("excludedlongtermcareworker"),col("paymentdate"),col("credentiallastdateofcontact"),col("preferredlanguage"),col("credentialstatusdate"),col("nctrainingcompletedate"),col("examscheduleddate"),col("examscheduledsitecode"),col("examscheduledsitename"),col("examtestertype"),col("examemailaddress"),col("examdtrecdschedtestdate"),col("phonenum"))))

credential_delta_df = credential_delta_df.withColumn("oldhashidkey",md5(concat_ws("",col("taxid"),col("providernumber"),col("credentialnumber"),col("providernamedshs"),col("providernamedoh"),col("dateofbirth"),col("dateofhire"),col("limitedenglishproficiencyindicator"),col("firstissuancedate"),col("lastissuancedate"),col("expirationdate"),col("credentialtype"),col("credentialstatus"),col("lepprovisionalcredential"),col("lepprovisionalcredentialissuedate"),col("lepprovisionalcredentialexpirationdate"),col("actiontaken"),col("continuingeducationduedate"),col("longtermcareworkertype"),col("excludedlongtermcareworker"),col("paymentdate"),col("credentiallastdateofcontact"),col("preferredlanguage"),col("credentialstatusdate"),col("nctrainingcompletedate"),col("examscheduleddate"),col("examscheduledsitecode"),col("examscheduledsitename"),col("examtestertype"),col("examemailaddress"),col("examdtrecdschedtestdate"),col("phonenum"))))

credential_df.createOrReplaceTempView("credential")
credential_delta_df.createOrReplaceTempView("credential_delta")


credential_clean_df  = spark.sql("""
    select * from credential where md5(concat_ws('', studentid, credentialtype)) not in (
		select md5(concat_ws('', studentid, credentialtype)) from credential group by studentid,credentialtype having count(1) > 1
	) and credentialnumber not in (
		select credentialnumber from credential group by credentialnumber having count(1) > 1
	) and credentialtype <> 'NA'
""")

credential_clean_df.show(20,truncate=False)
credential_clean_df.printSchema()
print(credential_clean_df.count())

#resultdf.filter("credentialnumber = 'LP60087624'").show()

credential_clean_df.createOrReplaceTempView("credential_clean")

newandupdatedcredentialsdf = spark.sql("select cc.*, 'New' as audit from  credential_clean cc LEFT ANTI JOIN credential_delta dc on cc.newhashidkey == dc.oldhashidkey").drop("studentid")

newandupdatedcredentialsdf.show(20,truncate=False)
newandupdatedcredentialsdf.printSchema()

print(newandupdatedcredentialsdf.count())

newandupdatedcredentialsdf.createOrReplaceTempView("credential_new")

#resultdf = newandupdatedcredentialsdf.unionByName(credential_delta_df, allowMissingColumns=True)

resultdf = spark.sql("""
    select dc.studentid as studentid
    ,case WHEN cn.audit = 'New' then cn.taxid ELSE dc.taxid END AS taxid
    ,case WHEN cn.audit = 'New' then cn.providernumber ELSE dc.providernumber END AS providernumber
    ,case WHEN cn.audit = 'New' then cn.credentialnumber ELSE dc.credentialnumber END AS credentialnumber
    ,case WHEN cn.audit = 'New' then cn.providernamedshs ELSE dc.providernamedshs END AS providernamedshs
    ,case WHEN cn.audit = 'New' then cn.providernamedoh ELSE dc.providernamedoh END AS providernamedoh
    ,case WHEN cn.audit = 'New' then cn.dateofbirth ELSE dc.dateofbirth END AS dateofbirth
    ,case WHEN cn.audit = 'New' then cn.dateofhire ELSE dc.dateofhire END AS dateofhire
    ,case WHEN cn.audit = 'New' then cn.limitedenglishproficiencyindicator ELSE dc.limitedenglishproficiencyindicator END AS limitedenglishproficiencyindicator
    ,case WHEN cn.audit = 'New' then cn.firstissuancedate ELSE dc.firstissuancedate END AS firstissuancedate
    ,case WHEN cn.audit = 'New' then cn.lastissuancedate ELSE dc.lastissuancedate END AS lastissuancedate
    ,case WHEN cn.audit = 'New' then cn.expirationdate ELSE dc.expirationdate END AS expirationdate
    ,case WHEN cn.audit = 'New' then cn.credentialtype ELSE dc.credentialtype END AS credentialtype
    ,case WHEN cn.audit = 'New' then cn.credentialstatus ELSE dc.credentialstatus END AS credentialstatus
    ,case WHEN cn.audit = 'New' then cn.lepprovisionalcredential ELSE dc.lepprovisionalcredential END AS lepprovisionalcredential
    ,case WHEN cn.audit = 'New' then cn.lepprovisionalcredentialissuedate ELSE dc.lepprovisionalcredentialissuedate END AS lepprovisionalcredentialissuedate
    ,case WHEN cn.audit = 'New' then cn.lepprovisionalcredentialexpirationdate ELSE dc.lepprovisionalcredentialexpirationdate END AS lepprovisionalcredentialexpirationdate
    ,case WHEN cn.audit = 'New' then cn.actiontaken ELSE dc.actiontaken END AS actiontaken
    ,case WHEN cn.audit = 'New' then cn.continuingeducationduedate ELSE dc.continuingeducationduedate END AS continuingeducationduedate
    ,case WHEN cn.audit = 'New' then cn.longtermcareworkertype ELSE dc.longtermcareworkertype END AS longtermcareworkertype
    ,case WHEN cn.audit = 'New' then cn.excludedlongtermcareworker ELSE dc.excludedlongtermcareworker END AS excludedlongtermcareworker
    ,case WHEN cn.audit = 'New' then cn.paymentdate ELSE dc.paymentdate END AS paymentdate
    ,case WHEN cn.audit = 'New' then cn.credentiallastdateofcontact ELSE dc.credentiallastdateofcontact END AS credentiallastdateofcontact
    ,case WHEN cn.audit = 'New' then cn.preferredlanguage ELSE dc.preferredlanguage END AS preferredlanguage
    ,case WHEN cn.audit = 'New' then cn.credentialstatusdate ELSE dc.credentialstatusdate END AS credentialstatusdate
    ,case WHEN cn.audit = 'New' then cn.nctrainingcompletedate ELSE dc.nctrainingcompletedate END AS nctrainingcompletedate
    ,case WHEN cn.audit = 'New' then cn.examscheduleddate ELSE dc.examscheduleddate END AS examscheduleddate
    ,case WHEN cn.audit = 'New' then cn.examscheduledsitecode ELSE dc.examscheduledsitecode END AS examscheduledsitecode
    ,case WHEN cn.audit = 'New' then cn.examscheduledsitename ELSE dc.examscheduledsitename END AS examscheduledsitename
    ,case WHEN cn.audit = 'New' then cn.examtestertype ELSE dc.examtestertype END AS examtestertype
    ,case WHEN cn.audit = 'New' then cn.examemailaddress ELSE dc.examemailaddress END AS examemailaddress
    ,case WHEN cn.audit = 'New' then cn.examdtrecdschedtestdate ELSE dc.examdtrecdschedtestdate END AS examdtrecdschedtestdate
    ,case WHEN cn.audit = 'New' then cn.phonenum ELSE dc.phonenum END AS phonenum
    ,dc.oldhashidkey as oldhashidkey
    ,cn.newhashidkey as newhashidkey
    ,cn.audit as audit
    ,case WHEN cn.audit = 'New' then cn.recordcreateddate ELSE dc.recordcreateddate END AS recordcreateddate
    ,case WHEN cn.audit = 'New' then cn.recordmodifieddate ELSE dc.recordmodifieddate END AS recordmodifieddate
    ,case WHEN cn.audit = 'New' then cn.filemodifieddate ELSE dc.filemodifieddate END AS filemodifieddate
    ,case WHEN cn.audit = 'New' then cn.filename ELSE dc.filename END AS filename
    from credential_new cn FULL OUTER JOIN credential_delta dc on cn.credentialnumber == dc.credentialnumber
    """)

resultdf.show()
resultdf.printSchema()
print(resultdf.count())

resultdf.filter("credentialnumber = 'LP60087624'").show()

resultdf.filter("audit is NULL").show(1,truncate=True)

resultdf.filter("studentid is NULL").show(1,truncate=True)

new_df = resultdf.withColumn("row_number",row_number().over(Window.partitionBy("credentialnumber").orderBy(col("recordcreateddate").desc_nulls_last()))).where(col("row_number") == 1)

#_nulls_last
new_df.filter("credentialnumber = 'LP60087624'").show()

new_df.filter("audit is NULL").show(1,truncate=True)


newdatasource0 = DynamicFrame.fromDF(new_df, glueContext, "newdatasource0")
applymapping1 = ApplyMapping.apply(frame = newdatasource0, mappings = [("studentid", "long", "studentid", "long"), ("taxid", "int", "taxid", "int"), ("providernumber", "long", "providernumber", "int"), ("credentialnumber", "string", "credentialnumber", "string"), ("providernamedshs", "string", "providernamedshs", "string"), ("providernamedoh", "string", "providernamedoh", "string"), ("dateofbirth", "string", "dateofbirth", "string"), ("dateofhire", "string", "dateofhire", "string"), ("limitedenglishproficiencyindicator", "string", "limitedenglishproficiencyindicator", "string"), ("firstissuancedate", "string", "firstissuancedate", "string"), ("lastissuancedate", "string", "lastissuancedate", "string"), ("expirationdate", "string", "expirationdate", "string"), ("credentialtype", "string", "credentialtype", "string"), ("credentialstatus", "string", "credentialstatus", "string"), ("lepprovisionalcredential", "string", "lepprovisionalcredential", "string"), ("lepprovisionalcredentialissuedate", "string", "lepprovisionalcredentialissuedate", "string"), ("lepprovisionalcredentialexpirationdate", "string", "lepprovisionalcredentialexpirationdate", "string"), ("actiontaken", "string", "actiontaken", "string"), ("continuingeducationduedate", "string", "continuingeducationduedate", "string"), ("longtermcareworkertype", "string", "longtermcareworkertype", "string"), ("excludedlongtermcareworker", "string", "excludedlongtermcareworker", "string"), ("paymentdate", "string", "paymentdate", "string"), ("credentiallastdateofcontact", "string", "credentiallastdateofcontact", "string"), ("preferredlanguage", "string", "preferredlanguage", "string"), ("credentialstatusdate", "string", "credentialstatusdate", "string"), ("nctrainingcompletedate", "string", "nctrainingcompletedate", "string"), ("examscheduleddate", "string", "examscheduleddate", "string"), ("examscheduledsitecode", "string", "examscheduledsitecode", "string"), ("examscheduledsitename", "string", "examscheduledsitename", "string"), ("examtestertype", "string", "examtestertype", "string"), ("examemailaddress", "string", "examemailaddress", "string"), ("examdtrecdschedtestdate", "string", "examdtrecdschedtestdate", "string"), ("phonenum", "string", "phonenum", "string"),("oldhashidkey","string","hashidkey","string"),("newhashidkey","string","newhashidkey","string"),("audit","string","audit","string"),("recordmodifieddate","timestamp","recordmodifieddate","timestamp"),("filemodifieddate","timestamp","filemodifieddate","timestamp"),("filename","string","filename","string"),("recordcreateddate","timestamp","recordcreateddate","timestamp")], transformation_ctx = "applymapping1")


selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["credentialtype", "paymentdate", "limitedenglishproficiencyindicator", "credentialstatus", "dateofhire", "longtermcareworkertype", "actiontaken", "excludedlongtermcareworker", "taxid", "examscheduledsitename", "providernumber", "examdtrecdschedtestdate", "providernamedoh", "examemailaddress", "providernamedshs", "dateofbirth", "credentiallastdateofcontact", "lastissuancedate", "examtestertype", "examscheduleddate", "phonenum", "credentialnumber", "expirationdate", "lepprovisionalcredential", "continuingeducationduedate", "studentid", "examscheduledsitecode", "lepprovisionalcredentialexpirationdate", "lepprovisionalcredentialissuedate", "preferredlanguage", "firstissuancedate", "credentialstatusdate", "nctrainingcompletedate","newhashidkey","hashidkey","audit","recordmodifieddate","filename","filemodifieddate","recordcreateddate"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential_delta", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

#uncomment the below line if commented out for testing
#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "seiubg-rds-b2bds", table_name = "b2bds_raw_credential_delta", transformation_ctx = "datasink5")
##########################Overwrite script - start ####################################

final_df = resolvechoice4.toDF()
#final_df = new_df
print("final_df count")
print(final_df.count())

final_df.show(20,truncate=False)
final_df.printSchema()

mode = "overwrite"
url = "jdbc:postgresql://"+B2B_HOST+"/"+B2B_NAME
properties = {"user": B2B_USER,"password": B2B_PASSWORD,"driver": "org.postgresql.Driver"}
final_df.write.option("truncate",True).jdbc(url=url, table="raw.credential_delta", mode=mode, properties=properties)

print("completed writing")

##########################Overwrite script - end ####################################


job.commit()