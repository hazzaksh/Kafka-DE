import threading
import boto3
import json
from kafka import KafkaConsumer

# Set up the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAZIVCGE6ARU6GXGMR',
    aws_secret_access_key='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'
)

# Define the S3 bucket and folder paths for each topic
s3_bucket = 'kafka-server-bucket1'
s3_folder_paths = {
    'balancesheet': 'c1/balancesheet',
    'profitloss': 'c1/profitloss',
    'salesbyproduct': 'c1/salesbyproduct',
    'cashflow': 'c1/cashflow'
}

def consumer_thread(topic):
    # Set up the Kafka consumer
    bootstrap_servers = ['13.232.143.65:9092']  # change IP here
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                             auto_offset_reset='earliest')
    
    # Loop through the messages and write them to S3
    for message in consumer:
        data = message.value
        print(topic)

        # Construct the S3 object key from the topic and message ID
        object_key = f"{s3_folder_paths[topic]}/{message.offset}.json"
        
        # Write the message to S3
        response = s3.put_object(Bucket=s3_bucket, Key=object_key, Body=json.dumps(data))
        print(f"Message written to S3: {topic} - {object_key}")

# Create a list of topics to subscribe to
topics = ['balancesheet', 'profitloss', 'salesbyproduct', 'cashflow']

# Create a separate thread for each topic and start them
for topic in topics:
    thread = threading.Thread(target=consumer_thread, args=(topic,))
    thread.start()
