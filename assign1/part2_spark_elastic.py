import pyspark
from pyspark.sql import SparkSession

# Read from kafka and mask profane words from content
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, to_json, struct

# Update the DataFrame to an Elasticsearch index
from pyspark.sql.functions import UserDefinedFunction

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer

#https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.3/spark-sql-kafka-0-10_2.12-3.4.3.jar
#https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.4.3

# data = {
#     "COMMENT_ID" : "z123std54m2ozht10232efr5svb4vh0au04",
#     "CONTENT": "damn nvm what I said"
# }

# data = {
#     "product_id": "123456",  # 업데이트할 제품의 ID
#     "item_keywords": ["새로운", "키워드", "리스트"],
#     "product_description": "이 제품은 새로운 설명입니다."
# }



class SparkElasticsearchIntegration:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("KafkaSparkElasticsearchIntegration") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0") \
            .config("spark.executorEnv.PYTHONPATH", "/Users/hansangho/nltk_data") \
            .getOrCreate()

        self.kafka_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "product_update") \
            .option("startingOffsets", "latest") \
            .load()

        # 메시지 스키마 정의
        self.schema = StructType([
            StructField("item_id", StringType(), True),
            StructField("item_keywords", ArrayType(StringType()), True),
            StructField("product_description", StringType(), True)
        ])

        self.json_stream = self.kafka_stream.selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", self.schema).alias("data")) \
            .select("data.*")

        self.es_conf = {
            "es.nodes": "localhost",
            "es.port": "9200",
            "es.resource": "products",  
            "es.mapping.id": "item_id",
            "es.net.http.auth.user": "elastic",
            "es.net.http.auth.pass": "password",
            "es.nodes.wan.only": "true"
        }

        # self.es_conf = {
        #     "es.nodes.discovery": "false",
        #     "es.nodes.data.only": "false",
        #     "es.net.http.auth.user": "elastic",
        #     "es.net.http.auth.pass": "password",
        #     "es.nodes": "localhost",
        #     "es.port": "9200",
        #     "es.write.operation": "update",
        #     "es.mapping.id": "item_id",
        #     "es.mapping.exclude": "item_id",
        # }

    def preprocess_text(self, text):
        lemmatizer = WordNetLemmatizer()
        word_tokens = word_tokenize(text)
        stop_words = set(stopwords.words('english'))
        filtered_words = [w for w in word_tokens if not w.lower() in stop_words]
        lemmatized_words = [lemmatizer.lemmatize(word) for word in filtered_words]
        
        return lemmatized_words

    # Elasticsearch 업데이트 함수
    def update_elasticsearch(self, df, epoch_id):
        name = 'CONTENT'
        udf = UserDefinedFunction(lambda x: self.preprocess_text(x), StringType())
        new_df = df.select(*[udf(column).alias(name) if column == name else column for column in df.columns])

        df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**self.es_conf) \
            .mode("append") \
            .save("products")  # 'products'는 Elasticsearch 인덱스 이름입니다.

    def process_batch(self, df, epoch_id):
        print(f"=== 배치 {epoch_id} ===")
        print("입력 데이터:")
        df.show(truncate=False)
        
        # Elasticsearch 업데이트
        # name = 'product_description'
        # udf = UserDefinedFunction(lambda x: x, StringType())
        # new_df = df.select(*[udf(column).alias(name) if column == name else column for column in df.columns])

        # new_df.write \
        #     .format("org.elasticsearch.spark.sql") \
        #     .options(**es_conf) \
        #     .mode("append") \
        #     .save("products")  # 'products'는 Elasticsearch 인덱스 이름입니다.
        # Elasticsearch 업데이트를 위한 데이터 준비
        
        update_df = df.select(
            col("item_id"),
            to_json(struct(col("item_keywords").alias("keywords"))).alias("doc")
        )

        print("Elasticsearch로 전송될 데이터:")
        update_df.show(truncate=False)

        # Elasticsearch 업데이트
        update_df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**self.es_conf) \
            .mode("append") \
            .save("products")  # 'products'는 Elasticsearch 인덱스 이름입니다.

    def spark_elastic(self):
        # 스트리밍 쿼리 실행 (기존 코드 대체)
        query = self.json_stream.writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("append") \
            .start()

        query.awaitTermination()