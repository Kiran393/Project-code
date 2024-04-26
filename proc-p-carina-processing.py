import boto3
import psycopg2
import json
from psycopg2 import Error
#from doh_dshs_lambda_handler import DexConnection
from botocore.exceptions import ClientError
import botocore.session


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

def postgress():
    #	ACCES_KEY=''
    #	SECRET_KEY=''

    print("start connection")

    try:
        conn = psycopg2.connect(host=B2B_HOST,
                                database="b2bds", user=B2B_USER, password=B2B_PASSWORD)

        cur = conn.cursor();
        print("connected")

        cur.execute("begin;")
        cur.execute("CALL staging.sp_updatecarinaeligibility();")
        cur.execute("commit;")

        print("Executed.")
        
    except:
        raise Error
    
    finally:
        # closing database connection.
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")
postgress()
