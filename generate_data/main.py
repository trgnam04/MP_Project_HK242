import json
import time
import random
import uuid
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import Schema

def generate_click_event(start_date="2024-01-01", end_date="2025-02-02"):
    category_type = random.choice(["S", "0", "category", "brand"])
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    category = "S" if category_type == "S" else "0" if category_type == "0" \
        else random.randint(1, 50) if category_type == "category" \
        else random.randint(100000000, 999999999)
    random_time = start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))
    return {
        'event': 'click',
        'sessionId': str(random.randint(0, 1000)),
        'timestamp': random_time.isoformat() + "Z",
        'category': str(category),
        'itemId': str(random.randint(100000, 999999))
    }

def generate_buy_event(start_date="2024-01-01", end_date="2025-02-02"):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    random_time = start + timedelta(seconds=random.uniform(0, (end - start).total_seconds()))
    return {
        'event': 'buy',
        'sessionId': str(random.randint(0, 1000)),
        'timestamp': random_time.isoformat() + "Z",
        'itemId': str(random.randint(100000, 999999)),
        'price': random.randint(1, 2000),
        'quantity': random.randint(1, 30)
    }

def generate_topup_event():
    # Customize this generator as needed or fetch from real source
    def random_uuid(): return str(uuid.uuid4())

    return {
        'id': random_uuid(),
        'code': random.choice(['cb6baa4e', 'a5357f26', 'xyz123']),
        'description': random.choice([
            'Topup package data 2H_TMDT to {num} successfully.',
            'Topup package data E10_TMDT to {num} successfully.'
        ]).format(num=random.randint(700000000, 799999999)),
        'phoneNumber': str(random.randint(700000000, 799999999)),
        'partnerTransId': str(random.randint(1000000000000000, 9999999999999999)),
        'supplierTransId': None,
        'jobId': None,
        'isPostpaid': random.choice([0, 2]),
        'totalAmount': random.choice([2000, 10000]),
        'discountRate': 0,
        'beforeCredit': random.randint(1000000, 5000000),
        'afterCredit': random.randint(1000000, 5000000),
        'ip': '{}.{}.{}.{}'.format(*[random.randint(1, 255) for _ in range(4)]),
        'type': 0,
        'status': random.choice([2, 3]),
        'supplier': random_uuid(),
        'mobileData': random_uuid(),
        'merchantUser': random_uuid(),
        'merchantSubscription': random_uuid()
    }

def load_avro_schema(file_path):
    try:
        with open(file_path, "r") as f:
            schema = json.load(f)
            if not schema:
                raise ValueError(f"Schema file {file_path} is empty!")
            return json.dumps(schema)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"Error: {e}")
        exit(1)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message sent to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def register_schema_if_needed(schema_registry_client, subject_name, schema_str):
    try:
        subjects = schema_registry_client.get_subjects()
        if subject_name in subjects:
            print(f"Schema '{subject_name}' already registered.")
            return schema_registry_client.get_latest_version(subject_name).version
        schema = Schema(schema_str, "AVRO")
        schema_id = schema_registry_client.register_schema(subject_name, schema)
        print(f"Registered schema '{subject_name}' with ID: {schema_id}")
        return schema_id
    except Exception as e:
        print(f"Error registering schema {subject_name}: {e}")
        return None

def main():
    topics = ['click_events', 'buy_events', 'topup_events']
    schema_registry_url = 'http://localhost:8082'
    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    avro_schemas_path = "../avro_schema"

    # Load schemas
    buy_schema_str = load_avro_schema(f"{avro_schemas_path}/buy_events.avsc")
    click_schema_str = load_avro_schema(f"{avro_schemas_path}/click_events.avsc")
    topup_schema_str = load_avro_schema(f"{avro_schemas_path}/topup_events.avsc")

    # Register schemas
    register_schema_if_needed(schema_registry_client, "test_buy_events", buy_schema_str)
    register_schema_if_needed(schema_registry_client, "test_click_events", click_schema_str)
    register_schema_if_needed(schema_registry_client, "test_topup_events", topup_schema_str)

    # Create serializers
    avro_serializer_buy = AvroSerializer(schema_registry_client, buy_schema_str)
    avro_serializer_click = AvroSerializer(schema_registry_client, click_schema_str)
    avro_serializer_topup = AvroSerializer(schema_registry_client, topup_schema_str)

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 100,
        "batch.size": 16384,
        "key.serializer": StringSerializer(),
        "value.serializer": None
    }

    producer = SerializingProducer(producer_config)
    start_time = datetime.now()

    while (datetime.now() - start_time).seconds < 10000:
        topic = random.choice(topics)
        if topic == 'buy_events':
            data = generate_buy_event()
            serializer = avro_serializer_buy
        elif topic == 'click_events':
            data = generate_click_event()
            serializer = avro_serializer_click
        else:
            data = generate_topup_event()
            serializer = avro_serializer_topup

        avro_bytes = serializer(data, SerializationContext(topic, MessageField.VALUE))
        try:
            print(f"Producing to {topic}: {data}")
            producer.produce(topic=topic, value=avro_bytes, on_delivery=delivery_report)
            producer.poll(0)
            time.sleep(2)
        except BufferError:
            print("Local producer queue is full, retrying...")
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")

    producer.flush()

if __name__ == "__main__":
    main()
