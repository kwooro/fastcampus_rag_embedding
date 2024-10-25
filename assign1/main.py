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
    spark_elastic = SparkElasticsearchIntegration()
    print('spark elastic 실행')
    spark_elastic.spark_elastic()

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

    """
    {'B07JZ7RK6W': {'item_name': ['taylor logan distressed wood kitchen bar cart storage rack shelf light oak'], 'bullet_point': ['blending rustic charm industrial elements rolling serving bar storage cart right touches serve fine cuisines mixed drinks store additional dishware open frame bar cart act statement piece', 'distressed light oak laminate finish pull/push handle bar raised border stemware rack holds 6 glasses', "black powder coated frame finish iron metal construction four 3.5 '' pvc swivel casters two locking", 'contemporary style fits nicely living room kitchen dining room office', "product measurements overall size 42.5 '' w x 16 '' x 31 '' h top shelf size 38 '' w x 14.25 '' x 3 '' h |middle shelf size 39.5 '' w x 16 '' x 6.75 '' h |bottom shelf size 39.5 '' w x 16 '' x 14.75 '' h |bottom shelf panel height 6 '' h"]},
     
       'B07B7BM55W': {'item_name': ["amazon brand – stone beam reclaimed fir rustic wood dining kitchen table 78.8 '' l brown"], 'bullet_point': ['reclaimed fir wood ladder-rung pedestals give table simple rustic beauty warm natural wood tones make space inviting sturdy construction make piece last many family dinners', "37.4 '' w x 78.74 '' l x 29.9 '' h", 'made hardwood comprised 100 reclaimed fir', 'simple natural style work modern farmhouse décor', 'dining table double work craft space', 'assembly 30 minutes less attach base top', 'free returns 30 days 3-year warranty']}, 
    
    'B000XUKZHK': {'item_name': ['strathwood teak chaise lounge side table'], 'bullet_point': []}, 'B07RG78QJS': {'item_name': ['amazon brand solimo essential engineered wood coffee table wenge finish'], 'bullet_point': ['sturdy structure made premium quality engineered wood', 'made european standard particle board ensure durability', 'spacious table top shelves placing magazines ornaments etc', 'aesthetic easy-to-clean design smooth edges diminish chances scrapes cuts', 'rigorously tested safety quality underwent 15+ safety quality checks', 'long-lasting table resistant humidity stains hot water', 'includes 3-year warranty manufacturing defects']}, 'B07GSPVLZ4': {'item_name': ['civet home tb07-0202-001-sg-a04 kitchen cart natural'], 'bullet_point': ['long lasting touch sophistication', 'worthwhile option needs', 'light oak color', 'made rubber wood plastic/metal caster wheels']}, 'B07RKKSYJL': {'item_name': ['amazon brand solimo neptune engineered wood bedside table wenge finish'], 'bullet_point': ['premium-quality engineered wood bedside table undergone 15 safety quality tests', 'material european standard particle board', 'smooth edges beautiful aesthetics protection scrapes cuts', 'tested resistance humidity stains hot water', 'free harmful carcinogens like lead formaldehyde pentachlorophenol pcp', 'comes 3-year warranty manufacturing defects']}, 'B07L1DFGFL': {'item_name': ['amazon brand solimo tucana engineered wood chest drawers walnut durance finish'], 'bullet_point': ['premium-quality engineered wood chest drawers undergone 20 safety quality tests', 'material european standard particle board', 'smooth curvilinear edges protection scrapes cuts', 'earthy walnut durance finish gives warm look makes cleaning easy task', 'durable drawers tested 5,000 cycles', 'tested resistance humidity stains hot water', 'free harmful carcinogens like lead formaldehyde pentachlorophenol pcp', 'comes 3-year warranty manufacturing defects']}, 'B07G527L5S': {'item_name': ['ameriwood home franklin coffee table black'], 'bullet_point': ['get table space need living room ameriwood home franklin coffee table', 'made painted mdf solid wood legs black finish gives table modern feel', 'place remotes electronics table top keep books magazines extra throws lower open shelf 2 drawers perfect small items keep table top free clutter', 'finish space entire franklin collection sold separately', 'table ships flat door 2 adults recommended assemble table top hold 75 lbs lower shelf hold 20 lbs drawers hold 15 lbs assembled dimensions 18.5 ” h x 41.14 ” w x 20.6 ”']}, 'B07RKKHMXX': {'item_name': ['amazon brand solimo essential engineered wood coffee table walnut finish'], 'bullet_point': ['sturdy structure made premium quality engineered wood', 'made european standard particle board ensure durability', 'spacious table top shelves placing magazines ornaments etc', 'aesthetic easy-to-clean design smooth edges diminish chances scrapes cuts', 'rigorously tested safety quality underwent 15+ safety quality checks', 'long-lasting table resistant humidity stains hot water', 'includes 3-year warranty manufacturing defects']}, 'B0754K96NT': {'item_name': ['furniture 247 2 tier 4 cube shelving unit natural oak'], 'bullet_point': ['includes one furniture 247 4 cube shelving unit suitable room around home', 'material painted mdf clean sheen finish', 'modular style design used functional storage display', 'quick easy assemble full instructions fixings included', 'dimensions 78 x 79 x 39cm']}}

    
    """

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
