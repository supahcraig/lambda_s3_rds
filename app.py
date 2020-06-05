#import sys
import logging
import pymysql
import boto3
from secrets import get_secret

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

#rds_host = 'mysqlforlambdatest.cevfxcwql3rj.us-east-1.rds.amazonaws.com'  # RDS endpoint
rds_host = 'cnelson-rds-mysql-proxy.proxy-cevfxcwql3rj.us-east-1.rds.amazonaws.com'  # proxy endpoint

secret = get_secret(secret_name='cnelson-mysql-secret', region_name='us-east-1')

try:
    conn = pymysql.connect(rds_host, 
                           user=secret['username'], 
                           passwd=secret['password'], 
                           db=secret['dbname'], 
                           connect_timeout=10)

except pymysql.MySQLError as e:
    logger.error('Error:  Unexpected error:  could not connect to MySQL instance.')
    logger.error(e)
    exit(99)

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded.")     


def handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f'bucket = {bucket}')
    logger.info(f'key = {key}')

    obj = s3.get_object(Bucket=bucket, Key=key)
    rows = obj['Body'].read().decode('utf-8').split('\n')
    logging.info(f'The object has {len(rows)} rows.')
    # seems to end up with an extra row, not sure why

    insert_count = 0

    with conn.cursor() as cur:
        cur.execute('delete from players')  # clean up the table first

        insert_sql = 'insert into players (player_id, year_id, team_id) values (%s, %s, %s)'

        for row in rows:
            parsed_row = row.split(',')
            
            try:
                cur.execute(insert_sql, (parsed_row[0], parsed_row[1], parsed_row[3]) )
                insert_count += 1
    
            except IndexError as e:
                logging.error(e)
                # let this exception pass...need to understand why there's an extra row being read
                

    conn.commit()
    logging.info(f'Added {insert_count} items to RDS MySQL table.')

    return 200



