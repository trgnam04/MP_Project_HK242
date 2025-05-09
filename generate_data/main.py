import json
import time
import csv
import uuid
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import Schema

# --- CONFIGURATION ---
DATA_PATH = "../system_data"
AVRO_SCHEMA_PATH = "../avro_schema"
SCHEMA_REGISTRY_URL = 'http://localhost:8082'
TOPIC_CONFIG = {
    'click_events': 'test_click_events',
    'buy_events': 'test_buy_events',
    'topup_events': 'test_topup_events'
}

# --- HELPERS TO LOAD CSV DATA ---
def load_topup_events(csv_file):
    events = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # direct string fields, nulls for empty
            events.append({
                'createdAt': row['createdAt'],
                'updatedAt': row['updatedAt'],
                'id': row['id'],
                'code': row['code'],
                'description': row['description'],
                'phoneNumber': row['phoneNumber'],
                'partnerTransId': row['partnerTransId'],
                'supplierTransId': row.get('supplierTransId') or None,
                'jobId': row.get('jobId') or None,
                'isPostpaid': int(row['isPostpaid']),
                'totalAmount': int(row['totalAmount']),
                'discountRate': int(row['discountRate']),
                'beforeCredit': int(row['beforeCredit']),
                'afterCredit': int(row['afterCredit']),
                'ip': row['ip'],
                'type': int(row['type']),
                'status': int(row['status']),
                'supplier': row['supplier'],
                'mobileData': row['mobileData'],
                'merchantUser': row['merchantUser'],
                'merchantSubscription': row['merchantSubscription']
            })
    return events


def load_click_events(csv_file):
    events = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for sid, ts, item, category in reader:
            events.append({
                'event': 'click',
                'sessionId': sid,
                'timestamp': ts,
                'itemId': item,
                'category': category
            })
    return events


def load_buy_events(csv_file):
    events = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for sid, ts, item, price, qty in reader:
            events.append({
                'event': 'buy',
                'sessionId': sid,
                'timestamp': ts,
                'itemId': item,
                'price': int(price),
                'quantity': int(qty)
            })
    return events

# --- SCHEMA LOADING & REGISTRY ---
def load_avro_schema(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.dumps(json.load(f))


def register_schema(client, subject, schema_str):
    subjects = client.get_subjects()
    if subject not in subjects:
        sid = client.register_schema(subject, Schema(schema_str, 'AVRO'))
        print(f"Registered {subject} : id={sid}")
    else:
        print(f"Schema {subject} already exists.")

# --- DELIVERY CALLBACK ---
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# --- MAIN ---
def main():
    # Initialize schema registry
    registry = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    # Load and register schemas
    schemas = {}
    serializers = {}
    for topic, subj in TOPIC_CONFIG.items():
        schema_str = load_avro_schema(f"{AVRO_SCHEMA_PATH}/{topic}.avsc")
        register_schema(registry, subj, schema_str)
        schemas[topic] = schema_str
        serializers[topic] = AvroSerializer(registry, schema_str)

    # Load data from CSV
    topup_data = load_topup_events(f"{DATA_PATH}/topup_events.csv")
    click_data = load_click_events(f"{DATA_PATH}/click_events.csv")
    buy_data = load_buy_events(f"{DATA_PATH}/buy_events.csv")

    # Setup producer
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': StringSerializer(),
        'value.serializer': None
    })

    # Round-robin or random publishing
    all_topics = list(TOPIC_CONFIG.keys())
    indexes = {'topup_events': 0, 'click_events': 0, 'buy_events': 0}

    while True:
        for topic in all_topics:
            # pick next record
            if topic == 'topup_events':
                data = topup_data[indexes[topic] % len(topup_data)]
            elif topic == 'click_events':
                data = click_data[indexes[topic] % len(click_data)]
            else:
                data = buy_data[indexes[topic] % len(buy_data)]
            indexes[topic] += 1

            # serialize and send
            serializer = serializers[topic]
            avro_payload = serializer(data, SerializationContext(topic, MessageField.VALUE))
            producer.produce(topic=topic, value=avro_payload, on_delivery=delivery_report)
            producer.poll(0)
            time.sleep(1)

    producer.flush()

if __name__ == '__main__':
    main()
