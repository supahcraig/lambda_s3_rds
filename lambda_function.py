import logging
import pymysql
import boto3
from secrets import get_secret

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

#rds_host = 'YOUR_RDS_ENDPOINT'  # RDS endpoint
rds_host = 'YOUR_RDS_PROXY_ENDPOINT' # Proxy endpoint

secret = get_secret(secret_name='YOUR_SECRET', region_name='YOUR_REGION')

try:
    logger.info(f'Trying to connect to MySQL instance...')
    conn = pymysql.connect(rds_host, 
                           user=secret['username'], 
                           passwd=secret['password'], 
                           db=secret['dbname'], 
                           connect_timeout=10)

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded.")     

except pymysql.MySQLError as e:
    logger.error('Error:  Unexpected error:  could not connect to MySQL instance.')
    logger.error(e)
    exit(99)


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f'bucket = {bucket}')
    logger.info(f'key = {key}')

    logger.info(f'Fetching S3 bucket object {bucket}/{key}')
    obj = s3.get_object(Bucket=bucket, Key=key)
    logger.info(f'SUCCESS: Fetched S3 bucket object {bucket}/{key}')

    logger.info(f'Reading S3 bucket object {bucket}/{key}')
    rows = (line.decode('utf-8') for line in obj['Body'].iter_lines())
    # .iter_lines() is a generator
    logger.info(f'SUCCESS: Read S3 bucket object {bucket}/{key}')


    column_names = ['playerID','yearID','stint','teamID','lgID',
                    'G','AB','R','H','2B','3B','HR','RBI','SB','CS',
                    'BB','SO','IBB','HBP','SH','SF','GIDP']

    bind_placeholders = ', '.join('%(' + column + ')s' for column in column_names)

    insert_count = 0
    row_batch = []

    with conn.cursor() as cur:
        cur.execute('delete from batting')  # clean up the table first

        insert_sql = f'insert into batting ({", ".join(column_names)}) values ({bind_placeholders})'
        logging.debug(insert_sql)

        for row_count, row in enumerate(rows):
            logging.debug(row)

            if row_count > 0:
                #skip the header row
                parsed_row = row.split(',')
                
                # change empty strings to null when they get inserted
                scrubbed_row = [x if x != '' else None for x in parsed_row]
                logging.debug(scrubbed_row)

                row_batch.append(dict(zip(column_names, scrubbed_row)))
                
                if len(row_batch) == 5000:
                    # Once the batch gets to 5k rows send to the database
                    cur.executemany(insert_sql, row_batch)
                    logging.info(f'Inserted {insert_count} records...')
                    row_batch = []

                insert_count += 1
        
        # insert the final residual batch
        cur.executemany(insert_sql, row_batch)
        logging.info(f'Inserted {insert_count} records...')
        row_batch = []
        
    conn.commit()
    logging.info(f'SUCCESS: Added {insert_count} items to RDS MySQL table.')
    conn.close()
    
