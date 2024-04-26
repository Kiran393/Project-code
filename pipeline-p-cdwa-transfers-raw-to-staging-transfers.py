import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import pandas
from datetime import date

#Default Job Arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#creating temp view from postgres db tables
glueContext.create_dynamic_frame.from_catalog(
    database = "seiubg-rds-b2bds", 
    table_name = "b2bds_raw_cdwatrainingtransfers").toDF()\
    .createOrReplaceTempView("cdwatrainingtransfers")

glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_person"
).toDF().selectExpr("*")\
    .createOrReplaceTempView("person")
    
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_trainingrequirement"
).toDF().selectExpr("*")\
    .createOrReplaceTempView("trainingrequirement")
    
    
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_employmentrelationship"
).toDF().selectExpr("*")\
    .createOrReplaceTempView("employmentrelationship")
    
    
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_trainingtransfers"
).toDF().selectExpr("*")\
.createOrReplaceTempView("prodtrainingtransfers")


#dedup within the trainingtransfers table on personid,trainingprogram and credithours with earliest completed date
spark.sql("""with cte as (select employeeid,personid,trainingprogram,classname,dshscoursecode,credithours,completeddate,trainingentity, 
reasonfortransfer,employerstaff,createddate,filedate,filename, ROW_NUMBER() OVER ( PARTITION BY personid, trainingprogram , credithours 
ORDER BY completeddate ASC ) completionsrank from cdwatrainingtransfers where isvalid = 'true')  
select * from cte where completionsrank = 1 """).createOrReplaceTempView("uniquetransfers")
#Run SQL Query
spark.sql("""select * from cdwatrainingtransfers""").show()
#duplicate records in trainingtransfers table
trainingtransfers_dup = spark.sql("""with cte as (select employeeid,personid,trainingprogram,classname,dshscoursecode,credithours,completeddate,trainingentity,
reasonfortransfer,employerstaff,createddate,filedate,filename,error_message, 
ROW_NUMBER() OVER ( PARTITION BY personid, trainingprogram, credithours 
ORDER BY completeddate ASC ) completionsrank from cdwatrainingtransfers where isvalid = 'true')  
select * from cte where completionsrank > 1 """)

#Query to get data which matche the requirement criteria
spark.sql("""select *,
case
when trainingprogram = 'Basic Training 70 Hours' then '201' 
when trainingprogram = 'Basic Training 30 Hours' then '202' 
when trainingprogram = 'Basic Training 9 Hours' then '203' 
when trainingprogram = 'Continuing Education' then '300' 
when trainingprogram = 'Orientation & Safety' then '100' 
END as trainingprogramcode  
from uniquetransfers   
where (cast(credithours as int) = 70 and trainingprogram = 'Basic Training 70 Hours') or 
(cast(credithours as int) = 30 and trainingprogram = 'Basic Training 30 Hours') or   
(cast(credithours as int)=9 and trainingprogram = 'Basic Training 9 Hours') or   
(cast(credithours as int)=5 and trainingprogram = 'Orientation & Safety') or  
 (trim(trainingprogram) = 'Continuing Education' and  cast(credithours as int) between 0 and 12 ) 
 """).createOrReplaceTempView("transfersdf")

#creating Union dataframe from cdwatrainingtransfers and prodtrainingtransfers
spark.sql("""with cte as (select personid,trainingprogram,
cast(credithours as numeric(5,2)),completeddate,trainingentity,reasonfortransfer from transfersdf
union all
select personid,trainingprogram,
transferhours,completeddate,transfersource,reasonfortransfer from prodtrainingtransfers)
select * ,ROW_NUMBER() OVER ( PARTITION BY personid, trainingprogram,credithours ORDER BY completeddate ASC ) completionsrank_1 
from cte""").createOrReplaceTempView("trainingtransfersunion")


spark.sql("""select * from transfersdf c where exists
(select 1 from  trainingtransfersunion t where c.personid=t.personid and c.trainingprogram=t.trainingprogram 
and cast(c.credithours as numeric(5,2)) =t.credithours and completionsrank_1=1)""").createOrReplaceTempView("transfers_dedup")




#Validating  transfers table data after dedup
transfers_validation = spark.sql("""with valid_trainingtrasfers
AS (SELECT tt.*,
             case
                  when (
                      cast(tt.completeddate as date) < person.hiredate
                      or   tr.personid is null) then false
                  else true end as is_valid,
             case
                  when cast(tt.completeddate as date) < person.hiredate then 'Rule 1 failed:Completion date should be greater than the hire date'
                  when tr.personid is null then 'Rule 2 failed: No open training requirement exist'
                  else NULL end as error_message,
             case    
                when tt.trainingprogramcode in ('201','202','203','204') and tt.credithours =tr.requiredhours
                        and (cast(tt.completeddate as date) <= coalesce(tr.duedateextension,tr.duedate )) then tt.credithours
                when tt.trainingprogramcode in ('300') and (cast(tt.completeddate as date) 
                    BETWEEN tr.trackingdate AND coalesce(tr.duedateextension,tr.duedate))
                    and tr.requiredhours-coalesce(tr.earnedhours,0)- coalesce(tr.transferredhours,0)=tt.credithours and person.hiredate>='2020-03-01'    
                    then tt.credithours + coalesce(tr.transferredhours,0) else NULL end As transferhours,
            case
                  when tt.trainingprogramcode in ('300') and (cast(tt.completeddate as date)
                        NOT BETWEEN tr.trackingdate AND coalesce(tr.duedateextension,tr.duedate)) 
                    then 'CE Completion should not exceed due date'
                  when tt.trainingprogramcode in ('201','202','203','204') and (cast(tt.completeddate as date) > coalesce(tr.duedateextension,tr.duedate)) 
                        then 'BT Completion should not exceed due date'
                  when tt.trainingprogramcode in ('300') and person.hiredate< '2020-03-01' 
                    then 'Caregiver is in active prior march 2020'
                  when  tt.trainingprogramcode in ('300') and coalesce(tr.earnedhours,0) + coalesce(tt.credithours,0) > coalesce(tr.requiredhours,0) 
                    then 'Excess CE Hours'
             else NULL
                 end as additionalerrors
    FROM transfers_dedup tt
        LEFT JOIN (   select distinct employeeid,er.personid
                        from employmentrelationship er
                       where lower(er.empstatus) = 'active') er
          ON tt.employeeid      = er.employeeid and tt.personid = er.personid
        LEFT JOIN (select personid, hiredate from person) person
          ON tt.personid        = person.personid
        LEFT JOIN (   select personid,
                               trainingid,
                             trainingprogram,
                             trackingdate,
                             duedate,
                             duedateextension,
                             requiredhours,
                             earnedhours,transferredhours
                      from trainingrequirement
                       where lower(status) = 'active') tr
          ON tt.personid        = tr.personid
         and tt.trainingprogram = tr.trainingprogram)
         
         select * from valid_trainingtrasfers""")
         
transfers_validation.show()

transfers_validation.createOrReplaceTempView("transfers_validation_view")

#selecting valid data based on flags is_valid as true and transferhours is not null into staging table
stagingtransfers = spark.sql("select * from transfers_validation_view where is_valid = true and transferhours is not null")
stagingtransfers_test =spark.sql("select * from transfers_validation_view where is_valid = true")
stagingtransfers_test.show()
#selecting valid data based on flags is_valid as false or transferhours is null  into log table
error_logs_after_validation = spark.sql("""select employeeid,personid,trainingprogram,classname,dshscoursecode,credithours,completeddate,trainingentity, 
reasonfortransfer,employerstaff,createddate,filedate,filename,concat_ws(';',error_message,additionalerrors) as error_message
from transfers_validation_view where is_valid = false or  transferhours is null""")


#Combining duplicate and error records 
trainingtransferslogs_df = error_logs_after_validation.unionByName(trainingtransfers_dup,allowMissingColumns=True)

validtransferstostaging = DynamicFrame.fromDF(stagingtransfers, glueContext, "trainingtransfers_df") 
errortransfertologs = DynamicFrame.fromDF(trainingtransferslogs_df, glueContext, "transferlogs") 
# Script generated for node ApplyMapping
validtransfers_targetmapping = ApplyMapping.apply(
    frame=validtransferstostaging,
    mappings=[
       ("employeeid", "bigint", "employeeid", "bigint"),
        ("personid", "bigint", "personid", "bigint"),
        ("trainingprogram", "string", "trainingprogram", "string"),
         ("trainingprogramcode", "string", "trainingprogramcode", "string"),
        ("classname", "string", "classname", "string"),
        ("dshscoursecode", "string", "dshscoursecode", "string"),
        ("credithours", "string", "transferhours", "double"),
        ("completeddate", "date", "completeddate", "date"),
        ("trainingentity", "string", "transfersource", "string"),
        ("reasonfortransfer", "string", "reasonfortransfer", "string"),
        ("filename", "string", "filename", "string"),
    ]
)
errortransfer_targetmapping = ApplyMapping.apply(
    frame=errortransfertologs,
    mappings=[
        ("employeeid", "bigint", "employeeid", "bigint"),
        ("personid", "bigint", "personid", "bigint"),
        ("trainingprogram", "string", "trainingprogram", "string"),
        ("classname", "string", "classname", "string"),
        ("dshscoursecode", "string", "dshscoursecode", "string"),
        ("credithours", "string", "credithours", "string"),
        ("completeddate", "date", "completeddate", "date"),
        ("trainingentity", "string", "trainingentity", "string"),
        ("reasonfortransfer", "string", "reasonfortransfer", "string"),
        ("employerstaff", "string", "employerstaff", "string"),
        ("createddate", "date", "createddate", "date"),
        ("filedate", "date", "filedate", "date"),
        ("filename", "string", "filename", "string"),
        ("error_message","string","error_message","string")
    ],
)

#Appending all valid records to staging training transfers 
glueContext.write_dynamic_frame.from_catalog(frame = validtransfers_targetmapping, 
                                             database = "seiubg-rds-b2bds", 
                                             table_name = "b2bds_staging_trainingtransfers")


# Appending all error to training transfers logs
glueContext.write_dynamic_frame.from_catalog(frame = errortransfer_targetmapping, 
                                             database = "seiubg-rds-b2bds", 
                                             table_name = "b2bds_logs_trainingtransferslogs")
job.commit()