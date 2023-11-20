# This is a placeholder code, update this code as an exercise

import boto3

def fetch_messages_from_sqs(queue_url, max_messages=10, wait_time=20):
    """Fetch messages from SQS"""
    
    # Initialize the SQS client
    sqs_client = boto3.client('sqs')
    
    # Poll messages from SQS
    messages = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,  # Max number of messages to fetch in one call
        WaitTimeSeconds=wait_time,  # Long polling duration
    ).get('Messages')
    
    return messages

def delete_message_from_sqs(queue_url, receipt_handle):
    """Delete message from SQS after processing"""
    
    sqs_client = boto3.client('sqs')
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )

def process_messages(queue_url):
    """Main function to process SQS messages"""

    while True:
        # Fetch messages from SQS
        messages = fetch_messages_from_sqs(queue_url)

        if not messages:
            print("No messages available. Waiting for next poll...")
            continue
        
        for message in messages:
            # Your business logic to process each message
            # For instance:
            print("Processing message:", message['Body'])
            
            # After processing the message, delete it from the queue
            delete_message_from_sqs(queue_url, message['ReceiptHandle'])

if __name__ == '__main__':
    QUEUE_URL = 'YOUR_SQS_QUEUE_URL'
    process_messages(QUEUE_URL)
