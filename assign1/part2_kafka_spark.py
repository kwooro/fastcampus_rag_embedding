import json
import datetime

from kafka import KafkaProducer

class KafkaSparkIntegration:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def on_success(self, metadata):
        print(f"Message produced with the offset: {metadata.offset}")

    def on_error(self, error):
        print(f"An error occurred while publishing the message. {error}")



    def kafka_stream(self, item_id, item_keywords, product_description):
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
        future = self.producer.send("product_update", message)
        future.add_callback(self.on_success)
        future.add_errback(self.on_error)

        # Ensure all messages are sent before exiting
        self.producer.flush()
    
    def close_kafka(self):
        self.producer.close()