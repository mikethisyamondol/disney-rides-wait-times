import json
import boto3
import requests
from csv import reader
import os

def connect_to_endpoint(url):
    response = requests.request("GET", url)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def lambda_handler(event, context):
    
    url = 'https://queue-times.com/en-US/parks/6/queue_times.json'
    response = connect_to_endpoint(url)
    print(' ')
    
    rides = []

    for land in response["lands"]:
        for ride in land["rides"]:
            if ride["is_open"] == True:
                ride_dict = {}
                ride_dict['dt'] = {'S': str(ride['last_updated'][:10])+ ' ' + str(ride['last_updated'][11:13]) + ':00:00'}
                ride_dict['land'] = {'S': land["name"]}
                ride_dict['ride_name'] = {'S': ride['name']}
                ride_dict['wait_time'] = {'S': str(ride['wait_time'])}
                ride_dict['last_updated'] = {'S': ride['last_updated']}
                rides.append(ride_dict)
    
    dynamodb = boto3.client('dynamodb')
    
    for ride in rides:
        dynamodb.put_item(TableName='disneyridetimes', Item=ride)

#-------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------
    # S3

    # date = date.today()
    # filename = f'disneywaits_{date}.csv'
    # csv_file = open(filename, 'w')
    # csv_writer = csv.writer(csv_file)

    # count = 0
    # for land in response["lands"]:
    #     for ride in land["rides"]:
    #         if ride["is_open"] == True:
    #             ride_dict = {}
    #             ride_dict['key'] = land["name"].replace(" ", "")+ride['name'].replace(" ", "")+str(ride['last_updated'][:10])+str(ride['last_updated'][11:13])
    #             ride_dict['date'] = ride['last_updated'][:10]
    #             ride_dict['hour'] = ride['last_updated'][11:13]
    #             ride_dict['land'] = land["name"]
    #             ride_dict['ride_name'] = ride['name']
    #             ride_dict['wait_time'] = str(ride['wait_time'])
    #             ride_dict['last_updated'] = ride['last_updated']
    #             if count == 0:
    #                 header = ride_dict.keys()
    #                 csv_writer.writerow(header)
    #                 count += 1

    #             csv_writer.writerow(result.values())

    # csv_file.close()


    # s3 = boto3.resource('s3')        
    # s3.Bucket('mthisyamondol').upload_file(filename,f'disneydata/{filename}')
    
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda successfully completed!')
    }