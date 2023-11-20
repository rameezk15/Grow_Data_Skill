import json
import boto3

sqs_client = boto3.client('sqs')

def process_sales_order(sales_order):
    print(sales_order)  # Replace with your actual processing logic.

def lambda_handler(event, context):
    # Loop through each message that triggered the lambda function
    for record in event['Records']:
        sales_order = json.loads(record['body'])
        process_sales_order(sales_order)

    return {
        'statusCode': 200,
        'body': json.dumps('Processed sales orders from SQS!')
    }