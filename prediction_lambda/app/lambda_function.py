import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd 
import json
from prophet import Prophet
import pickle


def lambda_handler(event, context):

    date_list = []
    pred_list = []
    for j in range(1,31):
        date_list.append(f"2022-12-{j}")
    #date_list.append(f"2022-1-{j}")
    dynamodb = boto3.client('dynamodb')
    s3 = boto3.client('s3')
    bucket='bucket_name'
    result = s3.list_objects(Bucket = 'mthisyamondol', Prefix='/disney_rides_models/')
    for o in result.get('Contents'):
        s3.download_file(bucket, object_key, f'/tmp/{o}')
        # print(contents.decode("utf-8"))
        m = pickle.load(open(f"/tmp/{o}",'rb'))

        for pred_date in date_list:
            future_date = pd.DataFrame({'ds':[f'{pred_date} 8:00:00',
                                                    f'{pred_date} 9:00:00',
                                                    f'{pred_date} 10:00:00',
                                                    f'{pred_date} 11:00:00',
                                                    f'{pred_date} 12:00:00',
                                                    f'{pred_date} 13:00:00',
                                                    f'{pred_date} 14:00:00',
                                                    f'{pred_date} 15:00:00',
                                                    f'{pred_date} 16:00:00',
                                                    f'{pred_date} 17:00:00',
                                                    f'{pred_date} 18:00:00',
                                                    f'{pred_date} 19:00:00',
                                                    f'{pred_date} 20:00:00',
                                                    f'{pred_date} 21:00:00',
                                                    f'{pred_date} 22:00:00',]})
            forecast = m.predict(future_date)
            pred = {
                        "ds": {'S': forecast['ds']},
                        "yhat": {'S': forecast["yhat"]},
                        "ride_name": {'S': o}
                    }
        
            pred_list.append(pred)
    
    dynamodb.put_item(TableName='disneyridepreds', Item=pred_list)



 # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda successfully completed!')
    }