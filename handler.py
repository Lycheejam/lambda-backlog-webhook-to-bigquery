"""
Lambdaを使ってBacklogのWebhookを受け取り、BigQueryに保存するfunctionです。

以下のコードを拝借しました。
https://github.com/carlos-jenkins/python-github-webhooks/tree/master
"""
import json
import datetime
import logging
import os

from ipaddress import ip_address, ip_network

import pandas as pd


def validate_request_ip(request):
    """Function to validate that request comes from a known backlog ip"""

    # get ip of request
    request_ip_address = ip_address(u'{}'.format(request.access_route[0]))

    # https://support-ja.backlog.com/hc/ja/articles/360035645534
    backlog_ip_whitelist = [
        "54.64.128.240"
        "54.178.233.194"
        "13.112.1.142"
        "13.112.147.36"
        "54.238.175.47"
        "54.168.25.33"
        "52.192.156.153"
        "54.178.230.204"
        "52.197.88.78"
        "13.112.137.175"
        "34.211.15.3"
        "35.160.57.23"
        "54.68.48.106"
        "52.88.47.69"
        "52.68.247.253"
        "18.182.251.152"
    ]

    # check if ip is a valid one from backlog
    for valid_ip in backlog_ip_whitelist:
        if request_ip_address in ip_network(valid_ip):
            break
    else:
        error_msg = 'IP {} not allowed.'.format(request_ip_address)
        logging.error(error_msg)
        raise ValueError(error_msg)


def backlog_event(request):
    """Function to handle incoming event from backlog webhook and save event data to BigQuery."""

    # validate request ip
    validate_request_ip(request)

    # request_timestamp
    request_timestamp = str(datetime.datetime.now())

    # get relevant env vars
    gcp_project_id = os.environ.get('GCP_PROJECT_NAME')
    bq_dataset_name = os.environ.get('BQ_DATASET_NAME')
    bq_table_name = os.environ.get('BQ_TABLE_NAME')
    bq_if_exists = os.environ.get('BQ_IF_EXISTS')
    partition_date = request_timestamp[0:8]

    # get json from request
    request_json = request.get_json()

    # make pandas df
    data = [
        partition_date,
        request_timestamp,
        request_json.get("created"),
        request_json.get("project"),
        request_json.get("id"),
        request_json.get("type"),
        request_json.get("content"),
        request_json.get("notifications"),
        request_json.get("createdUser")
    ]
    columns = [
        'partition_date',
        'timestamp',
        'created',
        'project',
        'id',
        'type',
        'content',
        'notifications',
        'createdUser'
    ]
    df = pd.DataFrame(data=[data], columns=columns)

    # display df.head() in logs
    logging.info(df.head())

    # save to big query
    df.to_gbq(
        destination_table=f'{bq_dataset_name}.{bq_table_name}',
        project_id=gcp_project_id, if_exists=bq_if_exists
    )

    # build response
    response = {
        "statusCode": 200,
        "body": "OK"
    }

    logging.info(response)

    return json.dumps(response, indent=4)
