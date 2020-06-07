import logging
import pymysql
import boto3
from secrets import get_secret

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

#rds_host = 'YOUR.RDS.ENDPOINT'  # RDS endpoint
rds_host = 'YOUR.RDS-PROXY.ENDPOINT' # Proxy endpoint

secret = get_secret(secret_name='YOUR.SECRET', region_name='YOUR.REGION')

try:
    logger.info(f'Trying to connect to MySQL instance...')
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


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f'bucket = {bucket}')
    logger.info(f'key = {key}')

    logger.info(f'Fetching S3 bucket object {bucket}/{key}')
    obj = s3.get_object(Bucket=bucket, Key=key)
    logger.info(f'SUCCESS: Fetched S3 bucket object {bucket}/{key}')

    logger.info(f'Reading S3 bucket object {bucket}/{key}')
    rows = obj['Body'].read().decode('utf-8').split('\n')
    logger.info(f'SUCCESS: Read S3 bucket object {bucket}/{key}')
    logger.info(f'The object has {len(rows)} rows.')
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
    logging.info(f'SUCCESS: Added {insert_count} items to RDS MySQL table.')
    conn.close() # unsure how RDS proxy handles the connection close

    return 200



