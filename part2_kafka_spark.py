import json
import datetime

from kafka import KafkaProducer

def on_success(metadata):
    print(f"Message produced with the offset: {metadata.offset}")

def on_error(error):
    print(f"An error occurred while publishing the message. {error}")



producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def kafka_stream(item_id, item_keywords, product_description):
    # message = {
    #     "item_id": "B074MBPL5F",  # 업데이트할 제품의 ID
    #     "item_keywords": ["new", "keyword", "list"],
    #     "product_description": "this is a new description"
    # }
    message = {
        "item_id": item_id,
        "item_keywords": item_keywords,
        "product_description": product_description
    }

    # Send updates to 'product_update' topic
    future = producer.send("product_update", message)
    future.add_callback(on_success)
    future.add_errback(on_error)

    # Ensure all messages are sent before exiting
    producer.flush()
    producer.close()