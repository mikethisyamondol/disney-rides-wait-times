import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd 
from prophet import Prophet


def lambda_handler(event, context):
    ddb_client = boto3.client('dynamodb', region_name='us-east-2')
    ddb = boto3.resource('dynamodb', region_name='us-east-2')
    table = ddb.Table('disneyridetimes')

    rides = ['Disney Festival of Fantasy Parade',
            'The Barnstormer',
            'Dumbo the Flying Elephant',
            'Jungle Cruise',
            'Peter Pan''s Flight',
            'Haunted Mansion',
            'Meet Cinderella and Elena at Princess Fairytale Hall',
            'it''s a small world',
            'Prince Charming Regal Carrousel',
            '"Monsters, Inc. Laugh Floor"',
            'Space Mountain',
            'Mickey''s PhilharMagic',
            'Tomorrowland Transit Authority PeopleMover',
            'Mad Tea Party',
            'Astro Orbiter',
            'The Many Adventures of Winnie the Pooh',
            'Tomorrowland Speedway',
            'Meet Mickey Mouse at Town Square Theater',
            'Splash Mountain',
            'Pirates of the Caribbean',
            'Big Thunder Mountain Railroad',
            'Swiss Family Treehouse',
            'Under the Sea - Journey of The Little Mermaid',
            'Buzz Lightyear''s Space Ranger Spin',
            'Meet Rapunzel and Tiana at Princess Fairytale Hall',
            'The Magic Carpets of Aladdin']
    
    for ride in rides:
        response = table.get_item(
            Key={
                'ride_name': ride
            }
        )

        df = pd.read_json(response['Item'])
        print(df.head(10))



 # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda successfully completed!')
    }