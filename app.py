import sys
import logging
import rds_config
import pymysql
import boto3

s3 = boto3.client('s3')

rds_host = 'YOUR RDS ENDPOINT'  # RDS endpoint
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=10)
except pymysql.MySQLError as e:
    logger.error('Error:  Unexpected error:  could not connect to MySQL instance.')
    logger.error(e)
    sys.exit(99)

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded.")

def handler(event, context):
    logging.info('got inside the event handler')
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f'bucket = {bucket}')
    logger.info(f'key = {key}')

    obj = s3.get_object(Bucket=bucket, Key=key)
    rows = obj['Body'].read().decode('utf-8').split('\n')
    logging.info(f'The object has {len(rows)} rows.')
    # seems to end up with an extra row, not sure why

    item_count = 0

    with conn.cursor() as cur:
        cur.execute('delete from players')  # clean up the table first

        insert_sql = 'insert into players (player_id, year_id, team_id) values ("{}", "{}", "{}")'

        for row in rows:
            logging.info('inserting a row')
            parsed_row = row.split(',')
            
            try:
                cur.execute(insert_sql.format(parsed_row[0], parsed_row[1], parsed_row[3]))
                item_count += 1
    
            except IndexError as e:
                logging.error(e)
                

    conn.commit()
    logging.info(f'Added {item_count} items to RDS MySQL table.')

    return 200
