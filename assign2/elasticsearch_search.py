from elasticsearch import Elasticsearch

class ElasticsearchSearch:
    def __init__(self, host="localhost", port=9200, username="elastic", password="password"):
        
        self.es = Elasticsearch([f"http://{host}:{port}"], basic_auth=(username, password) )

    def get_item_name_by_id(self, item_id):
        # 쿼리 실행
        query = {
            "query": {
                "match": {
                    "item_id": item_id
                }
            }
        }
        result = self.es.search(index="products", body=query, _source=["item_name"])

        # 결과 처리
        if result['hits']['total']['value'] > 0:
            item = result['hits']['hits'][0]['_source']
            return item
        else:
            return None
        
    def get_item_name_by_keyword(self, keyword):
        # 쿼리 실행
        query = {
            "query": {
                "match": {
                    "item_keywords": keyword
                }
            }
        }
        result = self.es.search(index="products", body=query, _source=["item_name"])

        # 결과 처리
        if result['hits']['total']['value'] > 0:
            item = result['hits']['hits']
            return item
        else:
            return None
        
    def get_item_name_by_ids(self, item_ids):
        # 쿼리 실행
        query = {
            "query": {
                "match": {
                    "item_id":{
                        "query": item_ids,                    
                        "operator": "or"
                        }
                    }
            }
        }
        result = self.es.search(index="products", body=query, _source=["item_name"])

        # 결과 처리
        if result['hits']['total']['value'] > 0:
            items = result['hits']['hits']
            return items
        else:
            return None
        
# 사용 예시
if __name__ == "__main__":
    es_search = ElasticsearchSearch()
    keyword = "chair"
    results = es_search.get_item_name_by_keyword(keyword)
    print(results)
    # item_id = "B07RKKHMXX"
    # item_name = es_search.get_item_name_by_id(item_id)

    # if item_name:
    #     print(f"아이템 이름: {item_name['item_name']}")
    # else:
    #     print("아이템을 찾을 수 없습니다.")

    # item_ids = ["B07GSPVLZ4", "B07JZ7RK6W", "B07RKKHMXX"]
    # item_name = es_search.get_item_name_by_ids(' '.join(item_ids))

    # if item_name:
    #     print(f"아이템 이름: {item_name}")
    # else:
    #     print("아이템을 찾을 수 없습니다.")