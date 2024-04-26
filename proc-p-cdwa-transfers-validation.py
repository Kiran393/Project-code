import json
import boto3
import psycopg2
from psycopg2 import Error

# Accessing the Secrets Manager from boto3 lib
secretsmangerclient = boto3.client('secretsmanager')


# Accessing the secrets target database
databaseresponse = secretsmangerclient.get_secret_value(
    SecretId='prod/b2bds/rds/system-pipelines'
)

database_secrets = json.loads(databaseresponse['SecretString'])

B2B_USER = database_secrets['username']
B2B_PASSWORD = database_secrets['password']
B2B_HOST = database_secrets['host']
B2B_PORT = database_secrets['port']
B2B_DBNAME = 'b2bds'  # database_secrets['dbname']

# executing the stored procedure 
def execute_sp():
    print("start connection")
    try:
        conn = psycopg2.connect(host=B2B_HOST,
                                database=B2B_DBNAME, user=B2B_USER, password=B2B_PASSWORD)
        cur = conn.cursor()
        cur.execute("begin;")
        cur.execute("CALL raw.sp_processcdwatransfersdatavalidation()")
        cur.execute("commit;")
        print("Excuted SP raw.sp_processcdwatransfersdatavalidation fine")
        print("connected")
    except:
        raise Error


execute_sp()
