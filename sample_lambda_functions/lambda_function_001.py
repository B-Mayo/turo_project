import json

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # TODO implement

    ## Logging info in #AWS CloudWatch Logs
    logger.info('got event {}'.format(event))
    logger.info(event['body'])

    ## info from ******* webhook

    body = event['body']

    ## logic to determine what info to send to CloudWatch Log
    if "status=initialized" in body:
        logger.info('event initialized')
    elif "status=running" in body:
        logger.info('event running')
    elif "data_ready=1" and "************" in body:
        logger.info('event complete')
        logger.info('begin s3 upload process')

        ## begin S3 upload of turo host urls scrapped via *********
        import pandas as pd
        import numpy as np
        import requests
        import io

        logger.info('packages uploaded')
        # GET data from *******
        params = {"api_key": "tbZ-ZsudrQsL",
        "format": "csv"}

        r = requests.get('*****************', params=params)
        urlData = r.content
        raw_df = pd.read_csv(io.StringIO(urlData.decode('utf-8')))

        # clean data from ******
        raw_df.drop(['list1_listingValue','list1_delete_from_page'], axis=1,inplace=True)
        raw_df.drop_duplicates(['list1_listing_url'],inplace=True)
        columns = {'list1_listing_url':'listing_url','list1_listing_todays_date':'listing_todays_date',
                   'list1_listing_name': 'listing_name'}
        raw_df.rename(columns=columns, inplace=True)
        csv_file = raw_df.to_csv()

        # create file name for s3
        from time import gmtime, strftime
        time = strftime("%Y-%m-%d-%h-%m-%s", gmtime())
        file_name = "url_extraction_{}.csv".format(time)

        logger.info('file transformation complete')

        # upload to S3
        import boto3

        s3 = boto3.resource('s3')
        object = s3.Object('*************', "********************".format(file_name))
        object.put(Body=csv_file)
        logging.info('s3 upload complete')


    else:
        logger.error('event error')


    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
