from input_embeddings_vectordb import Milvus
from elasticsearch import Elasticsearch

from pprint import pprint
import requests
import json


def part1():
    milvus_client = Milvus()
    milvus_client.create_collection("abo_listings")

def search_api(type, q):
    url = f"http://localhost:8000/{type}"
    params = {"q": q}
    response = requests.get(url, params=params)
    return json.loads(response.text)
    #print(response.text)

if __name__ == "__main__":
    make_new_db = False
    if make_new_db:
        part1()

    # search_api("vector", "is there a dinning table made with oak?")
    # search_api("keyword", "is there a dinning table made with oak?")
    results = search_api("keyword", "chair")
    print('키워드 검색 결과:')
    pprint(results, indent=2, width=100, sort_dicts=False)

    results = search_api("vector", "is there a dinning table made with steel?")
    print('\n벡터 검색 결과:')
    pprint(results, indent=2, width=100, sort_dicts=False)

    results = search_api("search", "is there a dinning table made with oak?")
    print('\n통합 검색 결과:')
    pprint(results, indent=2, width=100, sort_dicts=False)