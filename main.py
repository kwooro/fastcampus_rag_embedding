import os
import time
from part1_read_preprocessing import preprocess_json_gz
from part1_input_es_through_spark import input_es_through_spark

if __name__ == "__main__":
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
    input_es_through_spark(list_data)
    end_time = time.time()
    print(f"엘라스틱 서치 입력 소요 시간: {end_time - start_time} 초")