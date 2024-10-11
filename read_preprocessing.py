import gzip
import json
import os 
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('punkt_tab')

def pretty_print_json(data):
    print(json.dumps(data, indent=4, ensure_ascii=False))
    
def filter_and_preprocess_language_tag(entries, target_tag='en_US'):
    return [preprocess_text(entry['value']) for entry in entries if entry.get('language_tag') == target_tag]

def preprocess_text(text):
    # 소문자 변환
    text = text.lower()
    # 토큰화
    words = word_tokenize(text)
    # 불용어 및 구두점 제거
    stop_words = set(stopwords.words('english'))
    words = [word for word in words if word not in stop_words and word not in string.punctuation]
    # 다시 문자열로 결합
    return ' '.join(words)

def process_entry(entry):
    processed_entry = {
        'item_id': entry.get('item_id'),
        'item_keywords': filter_and_preprocess_language_tag(entry.get('item_keywords', [])),
        'product_description': filter_and_preprocess_language_tag(entry.get('product_description', [])),
        'brand': filter_and_preprocess_language_tag(entry.get('brand', [])),
        'color': filter_and_preprocess_language_tag(entry.get('color', [])),
        'fabric_type': filter_and_preprocess_language_tag(entry.get('fabric_type', [])),
        'finish_type': filter_and_preprocess_language_tag(entry.get('finish_type', [])),
        'item_name': filter_and_preprocess_language_tag(entry.get('item_name', [])),
        'material': filter_and_preprocess_language_tag(entry.get('material', [])),
        'model_name': filter_and_preprocess_language_tag(entry.get('model_name', [])),
        'model_number': filter_and_preprocess_language_tag(entry.get('model_number', [])),
        'model_year': filter_and_preprocess_language_tag(entry.get('model_year', [])),
        'pattern': filter_and_preprocess_language_tag(entry.get('pattern', [])),
        'style': filter_and_preprocess_language_tag(entry.get('style', [])),
        # 다른 필드들은 원본 그대로
        'color_code': entry.get('color_code', []),
        'country': entry.get('country', ''),
        'domain_name': entry.get('domain_name', ''),
        'item_dimensions': entry.get('item_dimensions', {}),
        'item_weight': entry.get('item_weight', []),
        'main_image_id': entry.get('main_image_id', ''),
        'marketplace': entry.get('marketplace', ''),
        'node': entry.get('node', []),
        'product_type': entry.get('product_type', ''),
        'spin_id': entry.get('spin_id', ''),
        '3dmodel_id': entry.get('3dmodel_id', ''),
        'other_image_id': entry.get('other_image_id', []),
    }
    return processed_entry


# .json.gz 파일을 불러오고 전처리하는 함수
def preprocess_json_gz(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        # data = json.load(f)
        data = []
        for line in f:
            data.append(json.loads(line))
    
    # 전처리 작업 (예시: 필요한 필드만 추출)
    processed_data = []
    for i, entry in enumerate(data):
        # 예: 필요한 필드만 추출
        # pretty_print_json(entry)
        processed_entry = process_entry(entry)
        processed_data.append(processed_entry)
    
    return processed_data

if __name__ == "__main__":

    # 1. 전처리
    list_of_files = os.listdir("abo-listings/listings/metadata")

    list_data = []
    for file in list_of_files:
        # 파일 경로 설정
        file_path = 'abo-listings/listings/metadata/' + file
        preprocessed_data = preprocess_json_gz(file_path)
        list_data.extend(preprocessed_data)
    
    # 2. spark를 통해 elasticsearch에 저장
