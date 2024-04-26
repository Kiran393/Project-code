import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import pandas
from datetime import datetime

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

today = datetime.now()
suffix = today.strftime("%m_%d_%Y")

glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_employer",
).toDF().selectExpr("employerid", "employername").createOrReplaceTempView("employer")

glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_employertrust",
).toDF().selectExpr("employerid","trust").createOrReplaceTempView("employertrust")

glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_employmentrelationship",
).toDF().selectExpr("cast(employerid as int) employerid","personid","empstatus").createOrReplaceTempView("employmentrelationshiphistory")

spark.sql("SELECT ser.personid,  concat_ws(',',collect_set(emp.employername)) AS activeemployers FROM employmentrelationshiphistory ser JOIN employer emp ON ser.employerid = emp.employerid JOIN employertrust bship ON ser.employerid = bship.employerid WHERE upper(ser.empstatus) = 'ACTIVE' AND upper(bship.trust) = 'TP' GROUP BY ser.personid").createOrReplaceTempView("employmentrelationship")

# Script generated for node PostgreSQL
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_credential_delta",
).toDF().selectExpr("credentialstatus","credentialnumber", "credentialtype","primarycredential").createOrReplaceTempView("credential")

# Script generated for node PostgreSQL
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_dohclassified"
).toDF().selectExpr("credentialnumber").createOrReplaceTempView("dohclassified")

spark.sql("SELECT credential.credentialstatus, credential.credentialnumber, credential.credentialtype, credential.primarycredential FROM credential WHERE upper(credential.credentialtype) = 'HM' AND credential.primarycredential = 1 AND NOT (credential.credentialnumber IN ( SELECT DISTINCT dohclassified.credentialnumber FROM dohclassified)) AND upper(credential.credentialstatus) = 'PENDING' ").createOrReplaceTempView("credentialcte")


glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_trainingrequirement",
).toDF().selectExpr("personid", "duedate" , "trainingprogram", "completeddate" , "earnedhours", "trackingdate", "trainingprogramcode","trainingid").createOrReplaceTempView("trainingrequirement")


spark.sql("select ospersonid, osduedate ,ostraining,oscompleted,osearnedhours,ostrackingdate from (SELECT tr.personid AS ospersonid, tr.duedate AS osduedate, tr.trainingprogram AS ostraining, tr.completeddate AS oscompleted, tr.earnedhours AS osearnedhours, tr.trackingdate AS ostrackingdate, ROW_NUMBER() OVER(PARTITION BY tr.personid,tr.trainingprogramcode ORDER BY tr.trainingid desc ,tr.completeddate desc) as trRank FROM trainingrequirement tr WHERE tr.trainingprogramcode = '100' and tr.completeddate  is not null and tr.earnedhours = 5 ) a where  a.trRank = 1").createOrReplaceTempView("oslearnertrainingcte")


# spark.sql("select * from employmentrelationship").show()
# spark.sql("select * from credentialcte").show()
# spark.sql("select * from oslearnertrainingcte").show()

glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_transcript",
).toDF().selectExpr("personid","courseid","instructorid","instructorname","trainingprogramcode").createOrReplaceTempView("transcript")


spark.sql("select * from ( select tr.personid AS btpersonid ,tr.duedate AS btduedate ,tr.trainingprogram AS bttraining ,tr.completeddate AS btcompleted ,tr.earnedhours AS btearnedhours ,tr.trackingdate AS bttrackingdate ,tr.trainingprogramcode as bttrainingprogramcode ,ROW_NUMBER() OVER(PARTITION BY tr.personid ORDER BY tr.trainingid desc ,tr.completeddate desc) as trRank from  trainingrequirement tr WHERE tr.trackingdate >= '2013-09-01 00:00:00' and tr.trainingprogramcode = '201' and (tr.completeddate  is null or (tr.completeddate > tr.duedate and tr.earnedhours = 70))) tr where tr.trRank = 1").createOrReplaceTempView("bt70trainingcompletedcte_tr")

spark.sql("select * from ( select 	tp.personid as btpersonid ,tp.trainingprogramcode as bttrainingprogramcode ,tp.instructorid as btinstructorid ,tp.instructorname as btinstructorname ,ROW_NUMBER() OVER(PARTITION BY tp.personid ORDER BY to_date('completeddate','yyyy-MM-dd HH:mm:ss')  desc) as tpRank from  transcript tp WHERE  tp.trainingprogramcode = '201' ) tp where tp.tpRank = 1").createOrReplaceTempView("bt70trainingcompletedcte_tp")

spark.sql("select tr.btpersonid ,tr.btduedate ,tr.bttraining ,tr.btcompleted ,tr.btearnedhours ,tr.bttrackingdate ,tr.bttrainingprogramcode ,tp.btinstructorid ,tp.btinstructorname from bt70trainingcompletedcte_tr tr left join bt70trainingcompletedcte_tp tp on  tr.btpersonid  = tp.btpersonid and tr.bttrainingprogramcode = tp.bttrainingprogramcode").createOrReplaceTempView("btlearnertrainingcte")


#spark.sql("select * from btlearnertrainingcte").show()


glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person"
).toDF().selectExpr("personid","lastname","firstname","right(ssn, 4) as ssn","date_format(dob,'MM/dd/yyyy') as dob","mailingstreet1" , "mailingcity" , "mailingstate" , "mailingzip", "null as county", "mobilephone","status","credentialnumber").createOrReplaceTempView("person")


#spark.sql("select * from person").show()


glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_staging_personhistory",
).toDF().selectExpr("personid","credentialnumber").createOrReplaceTempView("personhistory")

spark.sql("select DISTINCT credentialnumber, personid from personhistory where credentialnumber is not null and personid is not null").createOrReplaceTempView("personhistory")

outboundDF = spark.sql("SELECT distinct per.personid, per.lastname as lastname, per.firstname as firstname, per.ssn, per.dob as birthdate, upper(per.mailingstreet1) as mailingstreet1  , upper(per.mailingcity) as mailingcity, upper(per.mailingstate) as mailingstate, per.mailingzip, per.county, per.mobilephone, serv.activeemployers, date_format(bt.bttrackingdate, 'MM/dd/yyyy') as bttrackingdate, date_format(bt.btduedate, 'MM/dd/yyyy') as btduedate, cast(os.osearnedhours as int) as osearnedhours , date_format(os.oscompleted, 'MM/dd/yyyy') as oscompleted , COALESCE(bt.btearnedhours,0.0) as btearnedhours, date_format(bt.btcompleted, 'MM/dd/yyyy') as btcompleted, '0001' as trainingprogramcode, 'Training Partnership' as trainingprogramname, case when bt.btearnedhours = 70 then  bt.btinstructorid else null end as btinstructorid, case when bt.btearnedhours = 70 then  bt.btinstructorname else null end as btinstructorname, cred.credentialnumber FROM person per JOIN employmentrelationship serv ON per.personid = serv.personid JOIN btlearnertrainingcte bt ON bt.btpersonid = per.personid JOIN oslearnertrainingcte os ON os.ospersonid = per.personid JOIN personhistory prc ON per.personid = prc.personid JOIN credentialcte cred ON cred.credentialnumber = prc.credentialnumber WHERE upper(per.status) = 'ACTIVE'")



outboundDF = outboundDF.withColumnRenamed("personid","Student ID")\
                        .withColumnRenamed("lastname","Last Name")\
                        .withColumnRenamed("firstname","First Name")\
                        .withColumnRenamed("ssn","Social Security Number (Last 4)")\
                        .withColumnRenamed("birthdate","Birthdate")\
                        .withColumnRenamed("mailingstreet1","Mailing Street")\
                        .withColumnRenamed("mailingcity","Mailing City")\
                        .withColumnRenamed("mailingstate","Mailing State")\
                        .withColumnRenamed("mailingzip","Mailing Zip/Postal Code")\
                        .withColumnRenamed("county","County")\
                        .withColumnRenamed("mobilephone","Phone1")\
                        .withColumnRenamed("activeemployers","Active Employer(s)")\
                        .withColumnRenamed("bttrackingdate","Tracking Date")\
                        .withColumnRenamed("btduedate","Due Date")\
                        .withColumnRenamed("osearnedhours","Safety & Orientation Hours Completed")\
                        .withColumnRenamed("oscompleted","Safety & Orientation Completed On")\
                        .withColumnRenamed("btearnedhours","Basic Training Hours Completed")\
                        .withColumnRenamed("btcompleted","Basic Training Completed On")\
                        .withColumnRenamed("trainingprogramcode","Training Program Code")\
                        .withColumnRenamed("trainingprogramname","Training Program Name")\
                        .withColumnRenamed("btinstructorid","Last Instructor ID")\
                        .withColumnRenamed("btinstructorname","Last Instructor Name")\
                        .withColumnRenamed("credentialnumber","HCA Credential Number")
                        
outboundDF.show()

pandas_df = outboundDF.toPandas()

pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/doh/classified/Classified_DOH_BT75_Students_"+suffix+"_1.txt", header=True, index=None, sep='|')

job.commit()
