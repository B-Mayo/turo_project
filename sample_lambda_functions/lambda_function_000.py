import json

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    # TODO implement

    ## Logging info in #AWS CloudWatch Logs
    logger.info('event starting')

    ## load packages for time
    from time import gmtime, strftime
    from datetime import datetime
    from datetime import timedelta
    import json

    dates = [146]
    urls = #List of URLS to scrape
    url_list = []
    # run projects based on dates
    for url in urls:
        for days in dates:
            start = datetime.now() + timedelta(days=days)
            end = start + timedelta(days=4)

            start_year = start.strftime("%Y")
            start_month = start.strftime("%m")
            start_day = start.strftime("%d")

            end_year = end.strftime("%Y")
            end_month = end.strftime("%m")
            end_day = end.strftime("%d")

            startDate = "startDate={}%2F{}%2F{}".format(start_month,start_day,start_year)
            endDate = "endDate={}%2F{}%2F{}".format(end_month,end_day,end_year)
            start_url = url.format(endDate,startDate)
            url_list.append(start_url)
            
    logger.info('time variables set')
    logger.info('url variable set')
    url_list = json.dumps(url_list)

    #start third party webscraper via it's api
    import requests

    params = {
            "api_key": "tbZ-ZsudrQsL",
            "start_value_override" : "{\"urls\":" + url_list + "}",
            "send_email": "1"
            }
    #third party web scraper
    r = requests.post("************", data=params)

    text = r.json
    text()

    logger.info('project started')


    logger.info('lambda_function_00 complete')
    return {
            'statusCode': 200,
            'body': json.dumps(event)
        }
