import boto3
import json
from kafka import KafkaConsumer
from yogesh.Balancesheet import start_my

# Convert the json into csv by calling the yogesh's methods also based on topics
def get_csv_from_json(data, topic):
    print(data)
    csv = None
    if (topic == 'balancesheet'):
        csv = start_my(data)
        print("This code is running", topic)
    elif topic == 'salesbyproduct':
        pass
    elif topic == 'cashflow':
        pass
    else:
        pass
    return csv

# Set up the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAZIVCGE6ARU6GXGMR',
    aws_secret_access_key='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'
)


# Set up the Kafka consumer
bootstrap_servers = ['65.0.129.155:9092']  # change IP here
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest')

# Define the S3 bucket and folder paths for each topic
s3_bucket = 'kafka-server-bucket1'
s3_folder_paths = {
    'balancesheet': 'c1/balancesheet',
    'profitloss': 'c1/profitloss',
    'salesbyproduct': 'c1/salesbyproduct',
    'cashflow': 'c1/cashflow'
}

# Subscribe to the Kafka topics
consumer.subscribe(topics=['balancesheet', 'profitloss', 'salesbyproduct', 'cashflow'])

# Loop through the messages and write them to S3
for message in consumer:
    topic = message.topic
    data = message.value
    print(topic)
    
    # csv_data = get_csv_from_json(data,topic)
    csv_data = start_my(data)
    # Calling the conversion method from 
    print(csv_data)
    #process_topic
    # Construct the S3 object key from the topic and message ID
    object_key = f"{s3_folder_paths[topic]}/{message.offset}.csv"
    
    # Convert the CSV data to bytes
    csv_bytes = csv_data.encode('utf-8')
    # Write the message to S3
    response = s3.put_object(Bucket=s3_bucket, Key=object_key, Body=csv_bytes)
    print(f"Message written to S3: {topic} - {object_key}")


