#!/usr/bin/env python

import logging
import os
import re
import sys
import json
import argparse
import boto3
from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.exceptions import ClientError
from decimal import Decimal

# GENERAL CONSTANTS
# ---------------------------
CURRENT_PATH = os.getcwd()

# ---------------------------
# METHODS
# ---------------------------

def prepare_session(profile):
    """ Prepares session """
    try:
        logging.debug('Starting: prepare_session()')
        if profile is None:
            return boto3.Session()
        else:
            return boto3.Session(profile_name=profile)
    except:
        logging.critical('Failed to prepare AWS session! Check credentials file!')
    finally:
        logging.debug('Finishing: prepare_session()')

def parse_arguments():
    """ Parses arguments """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-p', '--profile',
        help='Name of AWS profile to use for execution. ' \
        'The profile must already exist in ~/.aws/config. ' \
        'This script blocks if the profile requires MFA sign-in.'
    )
    parser.add_argument(
        '-v', '--verbosity', action='count', default=0,
        help='Increase output verbosity'
    )
    parser.add_argument(
        '-t', '--table_name',
        help='DynamoDB table name'
    )
    parser.add_argument(
        '-r', '--region',
        help='Deployment region', default='ap-southeast-2'
    )
    parser.add_argument(
        '-s', '--schema_file',
        help='Schema file name. Must be a valid JSON schema.'
    )  
    parser.add_argument(
        '-i', '--version',
        help='Version (i for iteration) number. Must be a valid number eg. 1.0, 2.1 etc.'
    )            
    parser.add_argument(
        '-n', '--param-name',
        help='API QueryString Param name to be used to integrate with a schemas API endpoint.'
    )            

    return parser.parse_args()

def configure_logging(verbosity):
    """ Configures logging """
    level = logging.INFO
    if verbosity >= 1:
        level = logging.DEBUG
    
    # Quieten annoying messages about connections being dropped
    logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

    # Set the log format
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=level)

def upload_schema(client_dynamodb, table_name, schema_filename, version_number, param_name):
    """
    Perform parameter value substitution in cloudformation parameter templates.

    Args:
        client_dynamodb: the boto3 client to use for interacting with DynamoDB.
        table_name: Name of the dynamoDB table to PUT the schema into.
        schema_filename: Path to file containing schema.
        version_number: numeric version of the schema.
    """
    try:
        # Create a table interface
        table_dynamodb = client_dynamodb.Table(table_name)

        # Open the configuration file and parse it in. Expect JSON.
        with open(schema_filename) as json_file:  

            # Grab the JSON from the file
            # The schema is the entire JSON object
            schema = json.load(json_file)

            # The partition key is the ID value from the file, plus
            # the json extension.
            identifier = schema['id'] + '.json'

            response = table_dynamodb.put_item(
            Item={
                    'id': identifier,
                    'version': Decimal(version_number),
                    'definition': json.dumps(schema),
                    'param_name': param_name
                }
            )

        logging.info('Uploaded JSON Schema {0} v{1}'.format(identifier, version_number))

    except Exception as error:
        logging.critical('Error uploading JSON Schema {0} v{1}'.format(identifier, version_number))
        raise error

# ---------------------------
# MAIN
# ---------------------------

def main():

    """ Main method """
    args = parse_arguments()

    # Configure the logging library
    configure_logging(args.verbosity)

    # Prepare an AWS User session. This is based on a pre-existing
    # CLI profile if suitably configured. The profile must exist in the
    # configuration directory.
    session = prepare_session(args.profile)

    # Create session/boto3 clients in the appropriate deployment region
    client_dynamodb = session.resource('dynamodb')

    # Do the upload
    upload_schema(client_dynamodb, args.table_name, args.schema_file, args.version, args.param_name)

    sys.exit()

# Check if using as application or module
if __name__ == "__main__":
   main()