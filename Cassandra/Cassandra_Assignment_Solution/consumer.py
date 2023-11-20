from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import uuid
from cassandra import ConsistencyLevel

# Cassandra connection
cloud_config= {
  'secure_connect_bundle': 'secure-connect-cassandra-demo.zip'
}
auth_provider = PlainTextAuthProvider('YcFBXWbhJbGWbvvqFdIUmpvs', 'iAOq5Z_Z+DxYAurWtvTpwMApnFn6IDUGHigG2oQsxUspbTY,pLkP5NyQZoJsEjB87rewk-LP-a.OkOz+xOIr0PbeuMmMcsxLLWFSWaUagQZcy6FulrKMvzD.dsD6eNET')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-0ww79.australia-southeast2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '6LGTGYXCLT6MOOOR',
    'sasl.password': 'RRV0i94ilJOMQCS25Fj73C2D9h0ihhke3BbV3FTNt1lIaf/Uxcj5JBIrkD5SQy6A',
    'group.id': 'group16',
    'auto.offset.reset': 'earliest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-10wgj.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('VC5JAR4EETZAASKD', 'JEZ5p+uf7bi6P3vCOOOnt8jcLT6IzLSb18zS8ABxg59gY+FU6+eCpoxFJGoeMJsI')
})

# Fetch the latest Avro schema for the value
subject_name = 'ecommerce-orders-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset'],
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['ecommerce-orders'])

def process_message(message):
    # Process the Kafka message and derive the new columns
    key = message.key()  
    value = message.value()
    
    # Convert 'order_purchase_timestamp' string to datetime object
    order_purchase_timestamp_str = value['order_purchase_timestamp']
    order_purchase_timestamp = None
    if order_purchase_timestamp_str is not None:
        order_purchase_timestamp = datetime.strptime(order_purchase_timestamp_str, '%Y-%m-%d %H:%M:%S')
    
    # Convert 'order_approved_at' to a datetime object
    order_approved_at_str = value['order_approved_at']
    order_approved_at = None
    if order_approved_at_str is not None:
        order_approved_at = datetime.strptime(order_approved_at_str, "%Y-%m-%d %H:%M:%S")
    
    # Convert 'order_delivered_carrier_date' to a datetime object
    order_delivered_carrier_date_str = value['order_delivered_carrier_date']
    order_delivered_carrier_date = None
    if order_delivered_carrier_date_str is not None:
        order_delivered_carrier_date = datetime.strptime(order_delivered_carrier_date_str, "%Y-%m-%d %H:%M:%S")
    
    # Convert 'order_delivered_customer_date' to a datetime object
    order_delivered_customer_date_str = value['order_delivered_customer_date']
    order_delivered_customer_date = None
    if order_delivered_customer_date_str is not None:
        order_delivered_customer_date = datetime.strptime(order_delivered_customer_date_str, "%Y-%m-%d %H:%M:%S")
    
    # Convert 'order_estimated_delivery_date' to a datetime object
    order_estimated_delivery_date_str = value['order_estimated_delivery_date']
    order_estimated_delivery_date = None
    if order_estimated_delivery_date_str is not None:
        order_estimated_delivery_date = datetime.strptime(order_estimated_delivery_date_str, "%Y-%m-%d %H:%M:%S")
    
    purchase_hour = order_purchase_timestamp.hour
    purchase_day_of_week = order_purchase_timestamp.strftime('%A')
    
    # Convert 'order_id' to a valid UUID format
    try:
        order_id = uuid.UUID(value['order_id'])
    except ValueError:
        # If the 'order_id' is not a valid UUID, handle the error or skip the message
        print(f"Invalid 'order_id': {value['order_id']}")
        return
    
    # Convert 'customer_id' to a valid UUID format
    try:
        customer_id = uuid.UUID(value['customer_id'])
    except ValueError:
        # If the 'order_id' is not a valid UUID, handle the error or skip the message
        print(f"Invalid 'customer_id': {value['customer_id']}")
        return

    try:
        # Ingest the transformed data into the 'orders' table in Cassandra
        query = "INSERT INTO ecommerce.orders (order_id, customer_id, order_status, order_purchase_timestamp, " \
                "order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, " \
                "order_estimated_delivery_date, order_hour, Oorder_day_of_week) " \
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                
        prepared = session.prepare(query)
        
        bound_statement = prepared.bind((
        order_id,
        customer_id,
        value['order_status'],
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        purchase_hour,
        purchase_day_of_week
    ))
        
        bound_statement.consistency_level = ConsistencyLevel.QUORUM
        
        session.execute(bound_statement)
        
        print(f'Record {key} inserted successfully !!')
        
        # Manually commit the offset to Kafka
        consumer.commit(message)
        
    except Exception as err:
        
        print(f"Exception occured while inserting {key} into the table: {err}")
    

# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event, not an error
                print('Reached end of partition')
            else:
                print('Error while consuming: {}'.format(msg.error()))
        else:
            print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
            process_message(msg)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    cluster.shutdown()