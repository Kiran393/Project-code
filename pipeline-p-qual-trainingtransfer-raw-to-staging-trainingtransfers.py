import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat, col
import boto3
import json

#Default Job Arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)




#creating temp view from postgres db tables
glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_raw_qualtricstrainingtransfers"
).toDF().selectExpr("*").createOrReplaceTempView("rawqualtricstrainingtransfers")


glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_trainingtransfers"
).toDF().selectExpr("*").createOrReplaceTempView("prodtrainingtransfers")

glueContext.create_dynamic_frame.from_catalog(
    database="seiubg-rds-b2bds",
    table_name="b2bds_prod_coursecatalog"
).toDF().selectExpr("*").createOrReplaceTempView("prodcoursecatalog")


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




#Query to join with coursecatalog table to get training programname, code , cleaning dshscoursecode column and dedup  within the trainingtransfers table 

spark.sql("""with cte as (
 select *,
 case 
 when course_name like '%(CE)%'      
 then trim(TRAILING ' (CE)' from trim(course_name)) 
 when course_name like '%(BT70)%'      
 then trim(TRAILING ' (BT70)' from trim(course_name)) 
  when course_name like '%(BT7)%'      
 then trim(TRAILING ' (BT7)' from trim(course_name)) 
 when course_name like '%(BT30)%'      
 then trim(TRAILING ' (BT30)' from trim(course_name)) 
 when course_name like '%(BT9)%'      
 then trim(TRAILING ' (BT9)' from trim(course_name)) 
 when course_name like '%(O&S)%'      
 then trim(TRAILING ' (O&S)' from trim(course_name)) 
end as coursename_cleaned,
case 
when rlike(continuing_education_information_2,'(^[^#])') and rlike(continuing_education_information_2,'^.*CE+[0-9]|CO+[0-9]|NF+[0-9]')
then continuing_education_information_2
else NULL
end as dshscoursecode
from rawqualtricstrainingtransfers ), t_cte as(
select distinct a.*,b.trainingprogramtype,b.trainingprogramcode from cte a left join
(select distinct trainingprogramtype,trainingprogramcode from prodcoursecatalog where trainingprogramcode <> '603') 
b on a.coursename_cleaned = b.trainingprogramtype)
select *,'Qualtrics' as transfersource,ROW_NUMBER() OVER ( PARTITION BY personid, trainingprogramcode,continuing_education_information_1 ORDER BY completed_date ASC ) completionsrank 
    from t_cte  """).createOrReplaceTempView("cte_qualtricstrainingtransfers")
#Run SQL Query
print("printing cte_qualtricstrainingtransfers")
spark.sql("""select * from cte_qualtricstrainingtransfers""").show()

#Selecting only the unique transfers having "finished" = true and checking for exact transfer hours condition
spark.sql("""select * from cte_qualtricstrainingtransfers 
where  
(cast(credithours as double)= 70.00 and trainingprogramtype = 'Basic Training 70 Hours') or 
(cast(credithours as double) =  30.00 and trainingprogramtype = 'Basic Training 30 Hours') or  
(cast(credithours as double) =  7.00 and trainingprogramtype = 'Basic Training 7 Hours') or  
(cast(credithours as double) = 9.00 and trainingprogramtype = 'Basic Training 9 Hours') or   
(cast(credithours as double) = 5.00 and trainingprogramtype = 'Orientation & Safety') or  
 ((trainingprogramtype = 'Continuing Education' and  cast(credithours as double) between 0.00 and 12.00 )) and 
finished = 'True' and completionsrank = 1""").createOrReplaceTempView("transfers_unique")
print("printing transfers_unique")
spark.sql("""select * from transfers_unique""").show()

#Filtering duplicate transfers and keeping these to insert in training transfers logs table.
duplicatetransfers=spark.sql("""select *
from cte_qualtricstrainingtransfers where
 finished = 'True' and completionsrank >1 """)




#dedup against the trainingtransfers table in production and taking records which are not in prod.
spark.sql("""select finished,a.recordcreateddate,a.personid,a.credithours,a.course_name,a.reasonfortransfer,a.continuing_education_information_1,
a.continuing_education_information_2,a.completed_date,a.dshscoursecode,a.trainingentity,a.filename,a.trainingprogramtype,a.trainingprogramcode,a.transfersource
from  transfers_unique a
left join prodtrainingtransfers b
on a.personid= b.personid and
    a.trainingprogramtype = b.trainingprogram and
    a.credithours = b.transferhours and
    a.dshscoursecode = b.dshscoursecode
where b.personid is  null""").createOrReplaceTempView("transfers_dedup")

#selecting invalid records and ingesting to logs table
invalid_records = spark.sql(""" select finished,a.recordcreateddate,a.personid,a.credithours,a.course_name,a.reasonfortransfer,a.continuing_education_information_1,
a.continuing_education_information_2,a.completed_date,a.dshscoursecode,a.trainingentity,a.filename,a.trainingprogramtype,a.trainingprogramcode,a.transfersource
from  transfers_unique a
left join prodtrainingtransfers b
on a.personid= b.personid and
    a.trainingprogramtype = b.trainingprogram and
    a.credithours = b.transferhours and
    a.dshscoursecode = b.dshscoursecode
where b.personid is not null""")

print("printing transfers_dedup")
spark.sql("""select * from transfers_dedup""").show()

#Validating  transfers table data after dedup
validations = spark.sql("""with valid_trainingtrasfers
AS (SELECT tt.*,
             case
                  when (
                      cast(tt.completed_date as date) < person.hiredate
                      or   tr.personid is null) then false
                  else true end as isvalid,
             case
                  when cast(tt.completed_date as date) < person.hiredate then 'Rule 1 failed:Completion date should be greater than the hire date'
                  when tr.personid is null then 'Rule 2 failed: No open training requirement exist'
                  else NULL end as error_message,
             case    
                when tt.trainingprogramcode in ('201','202','203','204') and tt.credithours =tr.requiredhours
                        and (cast(tt.completed_date as date) <= coalesce(tr.duedateextension,tr.duedate )) then tt.credithours
                when tt.trainingprogramcode in ('300') and (cast(tt.completed_date as date) 
                    BETWEEN tr.trackingdate AND coalesce(tr.duedateextension,tr.duedate))
                    and tr.requiredhours-coalesce(tr.earnedhours,0)- coalesce(tr.transferredhours,0)=tt.credithours and person.hiredate>='2020-03-01'    
                    then tt.credithours + coalesce(tr.transferredhours,0) else NULL end As transfer_hours,
            case
                  when tt.trainingprogramcode in ('300') and (cast(tt.completed_date as date)
                        NOT BETWEEN tr.trackingdate AND coalesce(tr.duedateextension,tr.duedate)) 
                    then 'CE Completion should not exceed due date'
                  when tt.trainingprogramcode in ('201','202','203','204') and (cast(tt.completed_date as date) > coalesce(tr.duedateextension,tr.duedate)) 
                        then 'BT Completion should not exceed due date'
                  when tt.trainingprogramcode in ('300') and person.hiredate< '2020-03-01' 
                    then 'Caregiver is in active prior march 2020'
                  when  tt.trainingprogramcode in ('300') and coalesce(tr.earnedhours,0) + coalesce(tt.credithours,0) > coalesce(tr.requiredhours,0) 
                    then 'Excess CE Hours'
             else NULL
                 end as additionalerrors
           
    FROM transfers_dedup tt
        
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
         and tt.trainingprogramtype = tr.trainingprogram)
         
         select * from valid_trainingtrasfers  """)
         
validations.show()      
validations.createOrReplaceTempView("transfers_validation_view")
print(validations.count())
print("printing transfers_validation_view")
spark.sql("""select * from transfers_validation_view""").show()        
         
#selecting valid data based on flags is_valid as true and credithours is not null into staging table
staging_transfers = spark.sql("select * from transfers_validation_view where isvalid = true and transfer_hours is not null")
#staging_transfer_test = spark.sql("select * from transfers_validation_view where isvalid = true ")
print("staging_transfers")
staging_transfers.show()
print(staging_transfers.count())

#selecting valid data based on flags is_valid as false or credithours is null  into log table
error_logs_after_validation = spark.sql("""select *,concat_ws(';',error_message,additionalerrors) as errormessage
from transfers_validation_view where isvalid = false or  transfer_hours is null""")
print("error")
error_logs_after_validation.show()
print(error_logs_after_validation.count())


#Combining error records and duplicates to ingest into transfers logs table
error_records = duplicatetransfers.unionByName(error_logs_after_validation,allowMissingColumns=True)
transfers_logs = error_records.unionByName(invalid_records,allowMissingColumns=True)

stagingtransfers = DynamicFrame.fromDF(staging_transfers, glueContext, "stagingtransfers")
transferslogs = DynamicFrame.fromDF(transfers_logs, glueContext, "transferslogs")


# Script generated for node ApplyMapping
validtransfers_targetmapping = ApplyMapping.apply(
    frame=stagingtransfers,
    mappings=[
        ("personid", "bigint", "PersonID", "bigint"),
        ("trainingprogramtype", "string", "trainingprogram", "string"),
         ("trainingprogramcode", "string", "trainingprogramcode", "string"),
        ("credithours", "string", "transferhours", "numeric"),
        ("completed_date", "date", "completeddate", "date"),
        ("reasonfortransfer", "string", "reasonfortransfer", "string"),
        ("filename","string","filename","string"),
        ("dshscoursecode", "string", "dshscoursecode", "string"),
        ("transfersource","string","transfersource","string")    ],
)


#Appending all valid records to staging training transfers 
glueContext.write_dynamic_frame.from_catalog(
    frame=validtransfers_targetmapping,
    database="seiubg-rds-b2bds",
    table_name="b2bds_staging_trainingtransfers",
)

errortransfer_targetmapping = ApplyMapping.apply(
    frame=transferslogs,
    mappings=[
        ("personid", "bigint", "personid", "bigint"),
        ("trainingprogramtype", "string", "trainingprogram", "string"),
        ("credithours", "string", "credithours", "string"),
        ("completed_date", "date", "completeddate", "date"),
        ("trainingentity", "string", "trainingentity", "string"),
        ("reasonfortransfer", "string", "reasonfortransfer", "string"),
        ("finished","string","isvalid","boolean"),
        ("employerstaff", "string", "employerstaff", "string"),
        ("dshscoursecode", "string", "dshscoursecode", "string"),
        ("errormessage","string","error_message","string")

    ],
    
)

# Appending all error to training transfers logs
glueContext.write_dynamic_frame.from_catalog(
    frame=errortransfer_targetmapping,
    database="seiubg-rds-b2bds",
    table_name="b2bds_logs_trainingtransferslogs",
)

job.commit()
