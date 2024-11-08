from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from input_embeddings_vectordb import Milvus
from elasticsearch_search import ElasticsearchSearch
import json
from sklearn.feature_extraction.text import TfidfVectorizer

app = FastAPI()
milvus_client = Milvus()
es_search = ElasticsearchSearch()


def extract_from_elastic_results(results):
    json_results = []
    for row in results:
        source = row.get("_source", {})
        extracted = {
            "item_id": source.get("item_id"),
            "bullet_point": source.get("bullet_point"),
            "item_name": source.get("item_name")
        }
        json_results.append(extracted)
    return json_results

def extract_from_elastic_results_dict_format(results):
    json_results = {}
    for row in results:
        source = row.get("_source", {})
        item_id = source.get("item_id")
        if item_id:
            json_results[item_id] = {
                "item_name": source.get("item_name"),
                "bullet_point": source.get("bullet_point")
            }
    return json_results


async def rerank_with_tfidf(query, results):
    # 모든 결과의 텍스트를 수집합니다.
    documents = [' '.join(result['bullet_point']) for result in results]
    
    # TF-IDF 벡터라이저를 초기화합니다.
    vectorizer = TfidfVectorizer()
    
    # 결과와 쿼리를 벡터화합니다.
    tfidf_matrix = vectorizer.fit_transform(documents + [query])
    
    # 쿼리 벡터를 추출합니다.
    query_vector = tfidf_matrix[-1]
    
    # 각 결과와 쿼리 간의 코사인 유사도를 계산합니다.
    from sklearn.metrics.pairwise import cosine_similarity
    similarities = cosine_similarity(tfidf_matrix[:-1], query_vector)
    
    # 유사도에 따라 결과를 정렬합니다.
    sorted_indices = similarities.flatten().argsort()[::-1]
    sorted_results = [results[i] for i in sorted_indices]
    
    return sorted_results[:10]


# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Public API
@app.get("/search")
async def search(q: str = Query(..., description="검색어")):
    keyword_results = await keyword_search(q)
    vector_results = await vector_search(q)

    combined_results = vector_results['result'] + keyword_results['result']
    #print('combined_results:', combined_results)

    # query와 결과에서 키워드 검색 결과를 더 높게 매칭
    sorted_results = await rerank_with_tfidf(q, combined_results)
    #print('sorted_results:', sorted_results)
    return {"type": "search", "query": q, "result": sorted_results}

# Internal API
@app.get("/keyword")
async def keyword_search(q: str = Query(..., description="키워드 검색어")):    
    results = es_search.get_item_name_by_keyword(keyword=q)
    json_results = extract_from_elastic_results(results)
    return {"type": "keyword", "query": q, "result": json_results}

@app.get("/vector")
async def vector_search(q: str = Query(..., description="벡터 검색어")):
    vector_results = milvus_client.search_collection(collection_name='abo_listings', query=q)    
    #print('vector search results:', vector_results)

    item_ids = ' '.join([result['item_id'] for result in vector_results])
    results = es_search.get_item_name_by_ids(item_ids)
    dict_results = extract_from_elastic_results_dict_format(results)
    #print('dict results:', dict_results)

    combined_results = []
    for item in vector_results:    
        if item['distance'] < 0.2:
            continue
        item_id = item['item_id']
        if item_id in dict_results:
            combined_item = {
                'item_id': item_id,
                'item_name': dict_results[item_id].get('item_name'),
                'bullet_point': dict_results[item_id].get('bullet_point')
            }
            combined_results.append(combined_item)

    return {"type": "vector", "query": q, "result": combined_results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fastapi_server:app", host="0.0.0.0", port=8000, reload=True)