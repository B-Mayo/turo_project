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
    elif "data_ready=1" and "**********" in body:
        logger.info('event complete')
        logger.info('begin s3 upload process')

        ## begin S3 upload of turo host urls scrapped via *******
        import pandas as pd
        import numpy as np
        import requests
        import io
        import re

        logger.info('packages uploaded')
        # GET data from *******
        params = {"api_key": "tbZ-ZsudrQsL",
        "format": "json"}

        r = requests.get('https://www.*******.com/api/v2/projects/********/last_ready_run/data', params=params)
        urlData = r.json()

        raw_df = pd.DataFrame(r.json()['host'])
        # methods for conditional statements

        h_lang = raw_df.Languages.str.contains
        verified_info = raw_df.verified_info.str.contains

        # conditional statements for language and verified info

        ## language conditional statements
        english = h_lang('ENGLISH') | h_lang('English') | h_lang('english')
        spanish = h_lang('SPANISH') | h_lang('Spanish') | h_lang('spanish')
        chinese = h_lang('CHINESE') | h_lang('Chinese') | h_lang('chinese')
        nan = raw_df.Languages.isnull()
        speaks_another_lang = h_lang('Arabic') | h_lang('Russian') | h_lang('Italian') | h_lang('Hungarian') | h_lang('French')

        ## verified info statements
        approved_to_drive = verified_info('Approved to drive')
        email_address = verified_info('Email')
        phone_number = verified_info('Phone')
        facebook = verified_info('Facebook')

        # add columns based on conditional statments

        ## language conditional statements
        raw_df['speaks_english'] = np.where(english,1,0)
        raw_df['speaks_spanish'] = np.where(spanish,1,0)
        raw_df['speaks_chinese'] = np.where(chinese,1,0)
        raw_df['speaks_nan'] = np.where(nan,1,0)
        raw_df['speaks_another_lang'] = np.where(speaks_another_lang,1,0)

        ## approved info conditional statements
        raw_df['facebook_verified'] = np.where(facebook,1,0)
        raw_df['email_verified'] = np.where(email_address,1,0)
        raw_df['phone_number_verified'] = np.where(phone_number,1,0)
        raw_df['approved_to_drive'] = np.where(approved_to_drive,1,0)


        for index, row in raw_df.response_time.iteritems():
            if type(row) is float:
                raw_df.loc[index, 'response_time']
            else:
                if 'min' in row:
                    minute = int(re.findall('\d+', row)[0])
                    raw_df.loc[index, 'response_time'] = minute
                else:
                    hour = int(re.findall('\d+', row)[0])*60
                    raw_df.loc[index, 'response_time'] = hour


        col = ['Stars']
        for column in col:
            for index, row in raw_df[column].iteritems():
                if type(row) is float:
                    raw_df.loc[index, column] = np.nan
                else:
                    raw_df.loc[index, column] = float(re.findall(r"([^\s]+)", row)[0])


        col = ['Languages','verified_info']
        raw_df.drop(col,axis=1,inplace=True)

        columns = {'page_url': 'page_url', 'about_me_text':'about', 'response_time': 'host_response_mins',
                   'todays_date':'host_todays_date',
                   'response_rate': 'host_response_percentage', 'Stars': 'stars', 'join_date':'host_join_date',
                   'Name': 'names', 'School': 'host_school', 'Works': 'host_works'}

        raw_df.rename(columns=columns,inplace=True)

        columns = ['page_url', 'host_school', 'host_works', 'host_response_mins', 'host_response_percentage', 'stars', 'names',
                   'about', 'host_join_date', 'host_todays_date', 'speaks_english', 'speaks_spanish', 'speaks_chinese',
                   'speaks_nan', 'speaks_another_lang', 'facebook_verified', 'email_verified', 'phone_number_verified', 'approved_to_drive']

        csv_file = raw_df.to_csv(index=False, columns=columns, header=columns)


        logger.info('file transformation complete')

        # upload to S3
        import boto3

        from time import gmtime, strftime
        time = strftime("%Y-%m-%d_%H-%M-%S", gmtime())

        s3 = boto3.resource('s3')
        object = s3.Object('turo-project', "host_info_extractions/raw/data-file{}.csv".format(time))
        object.put(Body=csv_file)
        logging.info('s3 upload complete')


    else:
        logger.error('event error')


    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }
