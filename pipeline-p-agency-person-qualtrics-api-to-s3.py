import requests
import zipfile
import json
import io, os
import sys
import re
import boto3
from datetime import datetime

#suffix 
today = datetime.now()
suffix = today.strftime("%Y%m%d%H%M%S")
print(suffix)

def exportSurvey(apiToken, surveyId, dataCenter, fileFormat, suffix):
    surveyId = surveyId
    fileFormat = fileFormat
    dataCenter = dataCenter 
    # Setting static parameters
    requestCheckProgress = 0.0
    progressStatus = "inProgress"
    baseUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(dataCenter, surveyId)
    ##baseUrl = "https://sjc1.qualtrics.com/API/v3/surveys/SV_eLrujVCBzRnRpSC/export-responses/"
    headers = {
    "content-type": "application/json",
    "x-api-token": apiToken,
    }
    # Step 1: Creating Data Export
    downloadRequestUrl = baseUrl
    downloadRequestPayload = '{"format":"' + fileFormat + '"}'
    downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
    print("Json response")
    print(downloadRequestResponse.json())
    progressId = downloadRequestResponse.json()["result"]["progressId"]
    print(downloadRequestResponse.text)
    # Step 2: Checking on Data Export Progress and waiting until export is ready
    while progressStatus != "complete" and progressStatus != "failed":
        print ("progressStatus=", progressStatus)
        requestCheckUrl = baseUrl + progressId
        requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers)
        requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete")
        progressStatus = requestCheckResponse.json()["result"]["status"]
    #step 2.1: Check for error
    if progressStatus is "failed":
        raise Exception("export failed")
    fileId = requestCheckResponse.json()["result"]["fileId"]
    print(fileId)
    # Step 3: Downloading file
    requestDownloadUrl = baseUrl + fileId + '/file'
    requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)
    # Step 4: Unzipping the file
    ##zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall("MyQualtricsDownload")
    unzip = zipfile.ZipFile(io.BytesIO(requestDownload.content), 'r')
    print("NAMES LIST")
    print (unzip.namelist())
    #Let us verify the operation..
    caregiverdata = unzip.read('Caregiver Intake Form.csv')
    #caregiverdata = unzip.read('O&S Completion Confirmation Form.csv')
    print(caregiverdata)
    print ("Putting in S3")
    bucket = 'seiubg-b2bds-prod-feeds-fp7mk'
    subfolder = 'Inbound/raw/qualtrics/caregiver_intake/' 
    f_end = subfolder+'Caregiverdata'+suffix+'.csv'
    s3_client = boto3.client('s3',region_name='us-west-2')
    s3_client.put_object(Body=caregiverdata, Bucket=bucket, Key=f_end)
    print('Complete')

def main():
    try:
      apiToken = 'AUy5u7uEet3qU2zQ2I1bbc2z57GObX9v8vPlJAn4' 
      dataCenter = 'sjc1'
    except KeyError:
      print("set environment variables APIKEY and DATACENTER")
      sys.exit(2)
    try:
        surveyId= 'SV_eLrujVCBzRnRpSC'
        #surveyId= 'SV_6QYxPmGEQyf5wsS'
        #surveyId= 'SV_eLrujVCBzRnRpSC'
        fileFormat= 'csv'
    except IndexError:
        print ("usage: surveyId fileFormat")
        sys.exit(2)
    if fileFormat not in ["csv", "tsv", "spss"]:
        print ('fileFormat must be either csv, tsv, or spss')
        sys.exit(2)
    r = re.compile('^SV_.*')
    m = r.match(surveyId)
    if not m:
       print ("survey Id must match ^SV_.*")
       sys.exit(2)
    exportSurvey(apiToken, surveyId,dataCenter, fileFormat, suffix)
if __name__== "__main__":
    main()
