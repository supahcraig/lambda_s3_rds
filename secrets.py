import json
import boto3
import base64
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

def get_secret(secret_name, region_name):
     # this is the code provide by AWS when you create an RDS proxy/secret

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', 
                            region_name=region_name)
                            
    try:
        logging.info(f'Fetching the secret login info for {secret_name}/{region_name}')
        secret_value = client.get_secret_value(SecretId=secret_name)

    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in secret_value:
            return json.loads(secret_value['SecretString'])
            
        else:
            return json.loads(base64.b64decode(secret_value['SecretBinary']))
