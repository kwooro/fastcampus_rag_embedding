import os
import time
from part2_kafka_spark import KafkaSparkIntegration


def part3():
    kafka_spark = KafkaSparkIntegration()
    item_id = "B074MBPL5F"
    item_keywords = ["new", "keyword", "list"]
    product_description = "this is a new description"

    print('kafka stream 실행')
    kafka_spark.kafka_stream(item_id, item_keywords, product_description)

    item_id = "B07JZ7RK6W"
    item_keywords = ["amazon", "stone", "wood"]
    product_description = "this is a new description"
    print('kafka stream 실행')
    kafka_spark.kafka_stream(item_id, item_keywords, product_description)

    kafka_spark.close_kafka()


if __name__ == "__main__":
    # 1. 제품 listing 전체를 Elasticsearch에 인덱스
    # 2. 제품 인덱스의 ”item_keywords” 와 “product_description”를 업데이트 할 수 있는 API
    # 3. item_keyword와 product_description 정보로 제품을 검색

    # Index Update, Kafka -> Spark -> Elasticsearch
    part3()
