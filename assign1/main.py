import os
import time
from part1_read_preprocessing import preprocess_json_gz
from part1_input_es_through_spark import input_es_through_spark
from part2_spark_elastic import SparkElasticsearchIntegration
from part2_kafka_spark import KafkaSparkIntegration

def part1():

    start_time = time.time()
    # 1. 전처리
    list_of_files = os.listdir("abo-listings/listings/metadata")

    list_data = []
    for file in list_of_files:
        # 파일 경로 설정
        file_path = 'abo-listings/listings/metadata/' + file
        preprocessed_data = preprocess_json_gz(file_path)
        list_data.extend(preprocessed_data)
    
    end_time = time.time()
    print(f"전처리 소요 시간: {end_time - start_time} 초")
    
    # 2. spark를 통해 elasticsearch에 저장
    N = 20
    chunk_size = len(list_data) // N
    for i in range(N):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        input_es_through_spark(list_data[start_idx:end_idx])
    end_time = time.time()
    print(f"엘라스틱 서치 입력 소요 시간: {end_time - start_time} 초")

def part2():
    kafka_spark = KafkaSparkIntegration()
    spark_elastic = SparkElasticsearchIntegration()
    spark_elastic.spark_elastic()
    time.sleep(2)

    item_id = "B074MBPL5F"
    item_keywords = ["new", "keyword", "list"]
    product_description = "this is a new description"
    kafka_spark.kafka_stream(item_id, item_keywords, product_description)

    item_id = "B074MBPL5F"
    item_keywords = ["new", "keyword", "list"]
    product_description = "this is a new description"
    kafka_spark.kafka_stream(item_id, item_keywords, product_description)

    kafka_spark.close_kafka()

if __name__ == "__main__":
    # 1. 제품 listing 전체를 Elasticsearch에 인덱스
    # 2. 제품 인덱스의 ”item_keywords” 와 “product_description”를 업데이트 할 수 있는 API
    # 3. item_keyword와 product_description 정보로 제품을 검색

    # part 1
    # Batch Indexing , ABO-listings.json.gz -> Preprocessing -> Spark -> Elasticsearch
    #part1()

    # part 2
    # Index Update, Kafka -> Spark -> Elasticsearch
    part2()
