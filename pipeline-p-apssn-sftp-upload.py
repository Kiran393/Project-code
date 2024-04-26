import sys
import boto3
import json
import psycopg2
from psycopg2 import Error
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
import botocore.session
import paramiko
from paramiko import Transport, SFTPClient
import errno
from boto3.s3.transfer import TransferConfig
import os

# Set the desired multipart threshold value (1GB)
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=1*GB)


multipart_threshold = 1024 * 35
multipart_chunksize = 1024 * 35

today = datetime.now()
suffix = today.strftime("%Y-%m-%d")


s3client = boto3.client('s3', region_name='us-west-2')
s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')
objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Outbound/dshs'))
objects.sort(key=lambda o: o.last_modified)

file = []


first = list(objects[-1].key)
objkey = objects[-1].key
filedata= objects[-1].get()["Body"].read()
print (filedata.decode('utf8').count('\n')-1)
count = filedata.decode('utf8').count('\n')-1
obj = str(objects[-1].last_modified)
obj = (obj[:-6])
size = objects[-1].size
category = 'APSSN'

key = first[-44:]
final = ''.join(key)
print(final)

file.append(obj)
file.append(size)
file.append(final)
file.append(suffix)
file.append(category)
file.append(count)
print(file)

# setting up SFTP connection
client10 = boto3.client('secretsmanager')



response = client10.get_secret_value(
    SecretId='prod/b2bds/sftp/dshs'
)

sftp_secrets = json.loads(response['SecretString'])

SFTP_PASSWORD = sftp_secrets['password']
SFTP_USER = sftp_secrets['username']

SFTP_HOST = 'sft.wa.gov'

SFTP_PORT = 22

# Setting up Database connection


B2B_USER = 'seiudevb2b'
B2B_PASSWORD = 'BG775dat$st0re&'
B2B_HOST = 'dev-b2bdatastore.cluster-co2jktf3kedm.us-west-2.rds.amazonaws.com'
B2B_PORT = '5432'
B2B_NAME = 'b2bdevdb'





'''
ssh_client = paramiko.SSHClient()

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(SFTP_HOST,SFTP_PORT,SFTP_USER,SFTP_PASSWORD)

sftp = ssh_client.open_sftp()
print(sftp.listdir(path='TEST/'))


with sftp.open('TEST/'+final+'', 'w') as f:
    print("starting upload to sftp")
    s3client.download_fileobj('seiubg-b2bds-prod-feeds-fp7mk', ''+objects[-1].key+'', f,Config=config)
    print("done")
sftp.close()
'''

def postgress():
    #	ACCES_KEY=''
    #	SECRET_KEY=''

    print("start connection")

    try:
        conn = psycopg2.connect(host=B2B_HOST,
                                database="b2bdevdb", user=B2B_USER, password=B2B_PASSWORD)
        cur = conn.cursor();
        SQL = """INSERT INTO logs.sftpoutboundfilelog (name,  modified, numberofrows, size, category, proccesseddate) 
            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')""".format(file[2],file[0],file[5],file[1],file[4],file[3])
        cur.execute("begin;")
        cur.execute(SQL)
        cur.execute("commit;")
        print("UPLOAD SUCCESSFUL, FILE UPLOADED: " + file[2])
        print("connected")    
    except:
        raise Error

postgress()

print('connection established successfully')






