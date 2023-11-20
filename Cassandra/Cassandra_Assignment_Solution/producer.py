import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

#Load csv into pandas dataframe
df = pd.read_csv('olist_orders_dataset.csv')

# Examine it's structure and contents
print(df.head())

# print(df.info())

# print(df.describe())

# print(df.columns)

# print(df.shape)

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-0ww79.australia-southeast2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '6LGTGYXCLT6MOOOR',
    'sasl.password': 'RRV0i94ilJOMQCS25Fj73C2D9h0ihhke3BbV3FTNt1lIaf/Uxcj5JBIrkD5SQy6A'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-10wgj.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('VC5JAR4EETZAASKD', 'JEZ5p+uf7bi6P3vCOOOnt8jcLT6IzLSb18zS8ABxg59gY+FU6+eCpoxFJGoeMJsI')
})

# Fetch the latest Avro schema for the value
subject_name = 'ecommerce-orders-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
# print(schema_str)

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)


# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

count = 0
# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    if count < 50:
        key = f"{row['customer_id']}_{row['order_id']}"
        # Replace NaN values with None in the DataFrame
        row = row.where(pd.notna(row), None)

        # Create a dictionary from the row values
        value = row.to_dict()
        # print(value)
        # Produce to Kafka
        producer.produce(topic='ecommerce-orders', key=str(key), value=value, on_delivery=delivery_report)
        producer.flush()
        count += 1

print("Data successfully published to Kafka")