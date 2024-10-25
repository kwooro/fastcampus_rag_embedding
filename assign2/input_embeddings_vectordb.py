from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType
from pymilvus.client.types import ExtraList

import numpy as np
from pymilvus import utility
import time
from make_embeddings import MakeEmbeddings
from pymilvus import MilvusClient
import random
import json


def extract_from_results(results: ExtraList):
    extracted_data = []
    # ExtraList의 첫 번째 요소에 접근하고, 문자열을 파싱합니다.
    data = results[0]
    
    for item in data:
        extracted = {
            "item_id": item.get("id"),
            "distance": item.get("distance")
        }
        extracted_data.append(extracted)
    return extracted_data

collection_name = "abo_listings"
class Milvus:
    def __init__(self, host="localhost", port="19530"):
        uri = f"http://{host}:{port}"
        try:
            connections.connect("default", uri=uri)
            self.client = connections.add_connection()
            #self.client = connections.get_connection("default")
        except Exception as e:
            print(f"Milvus 연결 오류: {e}")
            raise
    
        self.client = MilvusClient(uri)

        self.embeddings_model = MakeEmbeddings()
        # print(client)
        # self.client = MilvusClient(host, port)

    def create_collection(self, collection_name):
        response = self.client.has_collection(collection_name)
        if not response:
            self.client.create_collection(collection_name)

        make_new_db = False
        # # 기존 컬렉션이 존재하면 삭제
        if make_new_db:
            if self.client.has_collection(collection_name):
                self.client.drop_collection(collection_name)

            fields = [
                FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=15, is_primary=True),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384)
            ]
            schema = CollectionSchema(fields, "임베딩을 저장할 컬렉션")
            response = self.client.get_collection("abo_listings")
            if response is None:
                self.client.create_collection("abo_listings", schema)

            embeddings, keys, values = self.embeddings_model.create_embeddings()
            # 예시 임베딩 데이터

            start_time = time.time()
            N = 100
            chunk_size = len(values) // N
            for i in range(N):
                start_idx = i * chunk_size
                end_idx = start_idx + chunk_size

                # keys와 embeddings를 별도의 리스트로 준비
                # keys_chunk = keys[start_idx:end_idx]
                # embeddings_chunk = [emb.tolist() if isinstance(emb, np.ndarray) else emb for emb in embeddings[start_idx:end_idx]]
                     
                data = []
                for j, (key, emb) in enumerate(zip(keys[start_idx:end_idx], embeddings[start_idx:end_idx]), start=start_idx):
                    emb_list = emb.tolist() if isinstance(emb, np.ndarray) else emb
                    data.append({
                        "id": key,
                        "embedding": emb_list,
                    })
                print(f"each time taken: {time.time() - start_time} seconds")
                # Milvus에 데이터 삽입
                self.client.insert(collection_name, data)


            # 인덱스 생성
            # index_type : FLAT, IVF_FLAT, IVF_SQ8, IVF_PQ, HNSW
            # metric_type : L2, IP, COSINE
            index_params = {
                "index_type": "HNSW",
                "metric_type": "COSINE",   
                "params": {
                    "M": 16,
                    "efConstruction": 100
                }
            }
            self.client.create_index(collection_name=collection_name, field_name="embedding", index_params=index_params)

    def search_collection(self, collection_name, query):
        self.client.load_collection(collection_name)
        query_embedding  = self.embeddings_model.embeddings_row(query)

        # 검색
        search_params = {"metric_type": "COSINE", "params": {"ef": 100}}
        results = self.client.search(collection_name =collection_name, 
                                data=query_embedding,
                                search_params=search_params,
                                limit=10 )
        #output_fields=["id", "embedding"]  
        extracted_results = extract_from_results(results)
        #print('vector search results:', results)

        return extracted_results

if __name__ == "__main__":
    milvus = Milvus()