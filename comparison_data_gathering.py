import boto3
from boto3 import client
import io
import pandas as pd
import os
import time
import json
from pandas.io.json import json_normalize


def execute_query(query,database,s3bucket):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation':'s3://'+s3bucket+'/',
        }
    )

    return 'query successfully'

def get_keys_in_folder(bucket_name,prefix):
    clients = client('s3')
    keys = clients.list_objects(Bucket=bucket_name, Prefix=prefix)
    key_list=[]
    for key in keys['Contents']:
        key_list.append(key['Key'])
    return key_list

def from_json(value: str) -> list[dict]:
    """
    Converts a string of json objects to a list of dictionaries
    :return list of dictionaries matching the converted json object.
    """
    if value[0] == "[" and value[-1] == "]":
        return _from_json_list(value)
    else:
        return _from_line_sep_json(value)


def _from_line_sep_json(value: str) -> list[dict]:
    json_str: str = value

    decode = json.JSONDecoder()
    data: list[dict] = []
    pos = 0

    while pos < len(json_str):
        try:
            json_str = json_str[pos:].strip()
            json_obj, pos = decode.raw_decode(json_str.strip())
            data.append(json_obj)
            return data
        except json.decoder.JSONDecodeError as e:
            raise

def _from_json_list(value: str) -> list[dict]:
    return json.loads(value)

def delete_folder(S3Bucket, foldername):#delete folder in s3 bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3Bucket)
    bucket.objects.filter(Prefix=foldername+'/').delete()
    return 'delete folder successfully'

def json_data_from_s3(database,s3bucket,folder_name,table_name,version):
    query = """ UNLOAD (SELECT * FROM "{table_name}" where day = '{version}')
           TO 's3://{S3Bucket}/{folder}'
           WITH (format='json', compression='none')""".format(table_name =table_name,S3Bucket =s3bucket, folder =folder_name,version = version)

    execute_query(query,database,s3bucket)

    keys = get_keys_in_folder(s3bucket,folder_name)

    json_list=[]
    for key in keys:
        s3 = boto3.resource('s3')
        obj = s3.Object(s3bucket, key)

        data_list = from_json(obj.get()['Body'].read().decode('utf-8'))
        json_list = json_list+data_list

    return json_list

def clena_dict(list_of_dictionaries):# clean repetitive rows
    new_list = []

    for dictionary in list_of_dictionaries:
        if dictionary not in new_list:
            new_list.append(dictionary)

    return new_list

def handler():
    Variables = {
        'DataBase': 'data_provisioning',
        'TableName': "imu_inference",
        'TableName_beta': 'imu_inference_beta',
        'version': '14',
        'S3FolderName': 'output-for-imu',
        'S3FolderName_beta': 'output-for-imu-beta',
        'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904',
    }

    data = json_data_from_s3(Variables['DataBase'],Variables['S3BucketNameSaveQuery'], Variables['S3FolderName'], Variables['TableName'], Variables['version'])
    data = clena_dict(data)
    beta_data = json_data_from_s3(Variables['DataBase'],Variables['S3BucketNameSaveQuery'], Variables['S3FolderName_beta'],Variables['TableName_beta'], Variables['version'])
    beta_data = clena_dict(beta_data)

    df = pd.json_normalize(data)
    transposed_data = df.melt(id_vars=["metadata.id", "requestid"], var_name="Field_Names", value_name="Prod_Values")

    df_beta = pd.json_normalize(beta_data)
    transposed_data_beta = df_beta.melt(id_vars=["metadata.id", "requestid"], var_name="Field_Names", value_name="Beta_Values")

    now = time.gmtime()
    nowt = time.strftime("%Y-%m-%d %H:%M:%S", now)
    nowt

    joined_comparision = transposed_data.merge(transposed_data_beta, on=["metadata.id", "requestid",
                                                                         "Field_Names"])  # add date/comparsiontime column for run time
    joined_comparision['RunTime'] = nowt

    return joined_comparision


print(handler())