# import boto3
import boto3
import json
import csv
import os

DYNAMODB_TABLE_NAME = 'vivekstable'
s3 = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_key = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    csv_content = response['Body'].read().decode('utf-8')
    
    file_path = '/tmp/logs.csv'
    with open(file_path, 'w') as temp_file:
        temp_file.write(csv_content)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader, None)
        for row in csv_reader:
            row = [None if value == "N/A" else value for value in row]
            item = {header[i]: row[i] for i in range(len(header))}
            table.put_item(Item=item)
    
    os.remove(file_path)

    return {
        'statusCode': 200,
        'body': json.dumps('Data successfully loaded into DynamoDB')
    }




