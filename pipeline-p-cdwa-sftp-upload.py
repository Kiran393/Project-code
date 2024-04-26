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
today = today.strftime("%Y-%m-%d")


file = []



s3client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk')
objects = list(s3.Bucket('seiubg-b2bds-prod-feeds-fp7mk').objects.filter(Prefix='Outbound/cdwa/Trainee_status_file/'))
objects.sort(key=lambda o: o.last_modified)
first = list(objects[-1].key)
obj = objects[-1].key
print(first)

key = first[-45:]
final = ''.join(key)
print(final)

filedata= objects[-1].get()["Body"].read()
print (filedata.decode('utf8').count('\n')-1)
count = filedata.decode('utf8').count('\n')-1
obj_dt = str(objects[-1].last_modified)
obj_dt = (obj_dt[:-6])
size = objects[-1].size
category = 'CDWA'


file.append(obj_dt)
file.append(size)
file.append(final)
file.append(today)
file.append(category)
file.append(count)
print(file)


client10 = boto3.client('secretsmanager')

response = client10.get_secret_value(
    SecretId='prod/b2bds/sftp/cdwa'
)

sftp_secrets = json.loads(response['SecretString'])

SFTP_PASSWORD = sftp_secrets['password']
SFTP_USER = sftp_secrets['username']
SFTP_HOST = 'ftp.mydirectcare.com'
SFTP_PORT = 22

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
print(sftp.listdir(path='Benefits_Group/Test/'))
with sftp.open('Benefits_Group/Test/To_CDWA/'+final+'', 'w') as f:
    print("starting upload to sftp")
    s3client.download_fileobj('seiubg-b2bds-prod-feeds-fp7mk', ''+objects[-1].key+'', f)
    print("done")
sftp.close()
print('connection established successfully')
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



'''def open_ftp_connection(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD): 
    
  #Opens ftp connection and returns connection object
  
  #client = paramiko.SSHClient()
  #client.load_system_host_keys() 
    transport = paramiko.Transport(SFTP_HOST, SFTP_PORT) 
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
    sftp = SFTPClient.from_transport(transport)
    with sftp.open('Benefits_Group/Prod/To_CDWA', 'wb', 32768) as f:
        s3.download_fileobj('seiubg-b2bds-prod-feeds-fp7mk', ''+final+'')
    sftp.close()
    print("done")
Client.close()
#s3client = boto3.client('s3')
#obj = s3client.get_object(Bucket='seiubg-b2bds-prod-feeds-fp7mk', Key=final)
#df = pd.read_csv(obj['Body'])
#df = pd.read_csv('s3:/bucket....csv')
#s3://seiubg-b2bds-prod-feeds-fp7mk/Outbound/cdwa/Trainee_status_file/CDWA-I-BG-TrainingStatusUpdate-2022-04-12.csv

  
sftp = open_ftp_connection(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD)
with sftp.open('Benefits_Group/Prod/To_CDWA', 'wb', 32768) as f:
    s3.download_fileobj('seiubg-b2bds-prod-feeds-fp7mk', ''+final+'')
    print("done")

#csvdf = df.to_csv()
#sftpclient.put(csvdf, 'Benefits_Group/Prod/To_CDWA/test.csv')

#s3.download_fileobj('seiubg-b2bds-prod-feeds-fp7mk', ''+final+'')'''

