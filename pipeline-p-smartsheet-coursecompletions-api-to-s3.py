############################################################################################################################################################################
############################################################################################################################################################################
############################################################################################################################################################################
############################################################################################################################################################################
import requests
import zipfile
import json
import io, os
import sys
import re
import boto3
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from pyspark.sql.functions import first
import time
import pandas 

id_list = []

suffix_list = []

s3_client = boto3.client('s3',region_name='us-west-2')

DF_list = []

token = 'fPVGkQ4EO69A9kmbnzWthVnpNdUMVxanwtggG'
# url = 'https://app.smartsheet.com/sheets/FVxQ4jMW2vGFg75Jw2fWCqmjxRGfXH6V9w55XFQ1'

url = 'https://api.smartsheet.com/2.0/reports/?includeAll=true'

url1 = 'https://api.smartsheet.com/2.0/reports/4594534932866948'
url2 = 'https://api.smartsheet.com/2.0/reports/1773634772592516'
url3 = 'https://api.smartsheet.com/2.0/reports/2420913724516228'
url4 = 'https://api.smartsheet.com/2.0/reports/6990398150207364'

#4594534932866948 -jan
#1773634772592516-jan
#2420913724516228 -dec
#6990398150207364 - dec
#decide the logic for choosing the monthly report number.

#number of months to go back
backlog = 5

#list of months to get data for
month_list = []

#month counter
i = 0

while(i <= backlog):
    
    month = datetime.datetime.now().month - i
    year = datetime.datetime.now().year
    if month < 1:
        month = 12 + month
        year-=1
    
    if month < 10:
        val = '0' + str(month) + str(year)
    else:
        val = str(month) + str(year)

    month_list.append(val)
    
    i+=1
    
print(str(month_list))
    
print('months chosen')


def upload_to_s3(data):
    
    time.sleep(1)
    today = datetime.datetime.now()
    suffix = today.strftime("%Y%m%d%H%M%S")
    print(suffix)
    
    k_end = 'Inbound/raw/smartsheets/raw/SmartSheet'+suffix+'.csv'
    s3_client.put_object(Body=data, Bucket='seiubg-b2bds-prod-feeds-fp7mk', Key=k_end)
    suffix_list.append(suffix)
    print('raw data ingested')

def callMyApi():
    
    print ("Calling API ...")
    
    try:
        
        res = requests.get(url,headers={'Authorization': 'Bearer fW2rLpCspKORv5Nu6OBYYCymquyzmkA6cNnxe','Content-Type': 'application/csv'})

        print("CSV to JSON...")
        data = json.loads(res.content, encoding='utf-8')
        
        for i in data['data']:
            if 'Timothy' in i['name']:
                if i['name'][19:25] in month_list:
                    id_list.append(i['id'])
        
        print('ids extracted from reports request')
        print(str(id_list))
        
        for rid in id_list:
            rurl = 'https://api.smartsheet.com/2.0/reports/'+str(rid)+'?includeAll=true'
            res = requests.get(rurl,headers={'Authorization': 'Bearer fW2rLpCspKORv5Nu6OBYYCymquyzmkA6cNnxe', 'Accept': 'text/csv'})
            #data = json.loads(res.content, encoding='utf-8')
            #print(res.content)
            
            upload_to_s3(res.content)

        #print(data)
        #pageNumber pageSize totalPages totalCount data
        
        print('json done.')

    except Exception as e:
        
        print(e)
        
    '''
    res1 = requests.get(url1,headers={'Authorization': 'Bearer fW2rLpCspKORv5Nu6OBYYCymquyzmkA6cNnxe','Content-Type': 'application/csv'})
    res2 = requests.get(url2,headers={'Authorization': 'Bearer fW2rLpCspKORv5Nu6OBYYCymquyzmkA6cNnxe','Content-Type': 'application/json'})
    res3 = requests.get(url3,headers={'Authorization': 'Bearer fW2rLpCspKORv5Nu6OBYYCymquyzmkA6cNnxe','Content-Type': 'application/json'})
    res4 = requests.get(url4,headers={'Authorization': 'Bearer fW2rLpCspKORv5Nu6OBYYCymquyzmkA6cNnxe','Content-Type': 'application/json'})

    print("JSON...")
    
    print ("Putting raw in S3")
    data1 = json.loads(str(res1.content, encoding='utf-8'))
    data2 = json.loads(str(res2.content, encoding='utf-8'))
    data3 = json.loads(str(res3.content, encoding='utf-8'))
    data4 = json.loads(str(res4.content, encoding='utf-8'))

    
    json_formatted_str = data.dumps(obj, indent=4)
    for i,j in data.items():
        print(i,' ',j)
    '''

def flatten(smartsheetDF):
    
    smartsheetDF2 = smartsheetDF.withColumn("columns_exploded", F.explode("columns")).withColumn("rows_exploded", F.explode("rows"))
    smartsheetDF3 = smartsheetDF2.select("rows_exploded.*", "columns_exploded.*")
    smartsheetDF4 = smartsheetDF3.select("virtualid","title")
    smartsheetDF5 = smartsheetDF4.distinct()
    smartsheetDF6 = smartsheetDF3.withColumn("cells_explode", F.explode("cells"))
    smartsheetDF7 = smartsheetDF6.select("cells_explode.value",'cells_explode.virtualColumnId','rowNumber')
    joinDF = smartsheetDF5.join(smartsheetDF7,smartsheetDF5["virtualid"] == smartsheetDF7["virtualColumnId"] ,how="inner")
    reshaped_df = joinDF.groupby(joinDF.rowNumber).pivot("title").agg(first("value"))
    
    return reshaped_df.coalesce(1)

def smartsheet(spark):
    
    print('loading raw for flattening')
    
    for s in suffix_list:
        smartsheetDF = spark.read.format("csv").option("header","true").option("InferSchema","true").load("s3://seiubg-b2bds-prod-feeds-fp7mk/Inbound/raw/smartsheets/raw/SmartSheet"+s+".csv")
        DF_list.append(smartsheetDF)
    
    #print(str(DF_list))
    '''
    print('starting flattening')

    for d in DF_list:
        d = flatten(d) 
        print(int(d.count()))

    print('joining curated DFs')
    '''
    
    if len(DF_list) != 0:
        outputDF = DF_list[0]

        for n in range(len(DF_list)):
            if n != 0:
                outputDF = outputDF.union(DF_list[n])
    
        print ("Putting curated in S3")
    

        pandas_df = outputDF.toPandas()
        pandas_df.to_csv("s3://seiubg-b2bds-prod-feeds-fp7mk/Inbound/raw/smartsheets/curated/SmartSheet"+suffix_list[0]+".csv", header=True, index=None, sep=',', mode='a')
        print('Curated data has been ingested in s3')
    else:
        print('no raw data found')
    
def main():
    
    spark = SparkSession \
	      .builder \
	      .master("local") \
	      .enableHiveSupport()\
	      .appName("Smartsheet") \
	      .getOrCreate()
	      
    spark.sparkContext.setLogLevel("ERROR")
    print('spark session has been created...')
    
    callMyApi()
    
    smartsheet(spark)
    


if __name__ == "__main__": 
    main()