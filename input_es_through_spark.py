from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json
from elasticsearch import Elasticsearch
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

def input_es_through_spark(list_data):
    # Spark 세션 생성
    spark = SparkSession.builder \
    .appName("ElasticsearchInput") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0") \
    .getOrCreate()

    # spark = SparkSession.builder \
    #     .appName("ElasticsearchInput") \
    #     .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.15.2") \
    #     .getOrCreate()

    # 스키마 정의
    schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("item_keywords", ArrayType(StringType()), True),
        StructField("product_description", ArrayType(StringType()), True),
        StructField("brand", ArrayType(StringType()), True),
        StructField("color", ArrayType(StringType()), True),
        StructField("fabric_type", ArrayType(StringType()), True),
        StructField("finish_type", ArrayType(StringType()), True),
        StructField("item_name", ArrayType(StringType()), True),
        StructField("material", ArrayType(StringType()), True),
        StructField("model_name", ArrayType(StringType()), True),
        StructField("model_number", ArrayType(StringType()), True),
        StructField("model_year", ArrayType(StringType()), True),
        StructField("pattern", ArrayType(StringType()), True),
        StructField("style", ArrayType(StringType()), True),
        StructField("color_code", ArrayType(StringType()), True),
        StructField("country", StringType(), True),
        StructField("domain_name", StringType(), True),
        StructField("item_dimensions", MapType(StringType(), StringType()), True),
        StructField("item_weight", ArrayType(StringType()), True),
        StructField("main_image_id", StringType(), True),
        StructField("marketplace", StringType(), True),
        StructField("node", ArrayType(StringType()), True),
        StructField("product_type", StringType(), True),
        StructField("spin_id", StringType(), True),
        StructField("3dmodel_id", StringType(), True),
        StructField("other_image_id", ArrayType(StringType()), True),
    ])

    # 스키마를 사용하여 DataFrame 생성
    df = spark.createDataFrame(list_data, schema)


    # Elasticsearch 연결 설정
    es_write_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource": "products",  
        "es.mapping.id": "item_id",
        "es.net.http.auth.user": "elastic",
        "es.net.http.auth.pass": "password",
        "es.nodes.wan.only": "true"
    }

    # DataFrame을 Elasticsearch에 저장
    df.write.format("org.elasticsearch.spark.sql") \
        .options(**es_write_conf) \
        .mode("append") \
        .save()

    # Spark 세션 종료
    spark.stop()

    print("데이터가 Elasticsearch에 성공적으로 저장되었습니다.")