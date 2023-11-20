import json
import boto3
import random

sqs_client = boto3.client('sqs')
QUEUE_URL = 'YOUR_SQS_QUEUE_URL'  # replace with your SQS Queue URL

def generate_sales_order():
    return {
        "order_id": random.randint(1000, 9999),
        "product_id": random.randint(100, 999),
        "quantity": random.randint(1, 100),
        "price": round(random.uniform(10.0, 500.0), 2)
    }

def lambda_handler(event, context):
    sales_order = generate_sales_order()
    sqs_client.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(sales_order)
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Sales order published to SQS!')
    }