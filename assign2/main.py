from input_embeddings_vectordb import Milvus
from elasticsearch import Elasticsearch

import requests


def part1():
    milvus_client = Milvus()
    milvus_client.create_collection("abo_listings")

def search_api(type, q):
    url = f"http://localhost:8000/{type}"
    params = {"q": q}
    response = requests.get(url, params=params)
    #print(response.text)

if __name__ == "__main__":
    make_new_db = False
    if make_new_db:
        part1()

    # search_api("vector", "is there a dinning table made with oak?")
    # search_api("keyword", "is there a dinning table made with oak?")
    results = search_api("keyword", "chair")
    print('keyword search results:', results)
    results = search_api("search", "is there a dinning table made with oak?")
    print('search search results:', results)