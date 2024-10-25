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

    #search_api("vector", "is there a dinning table made with oak?")
    #search_api("keyword", "is there a dinning table made with oak?")
    #search_api("keyword", "chair")
    search_api("search", "is there a dinning table made with oak?")

"""

"result":[{"_index":"products","_id":"B07HSG8DPZ","_score":15.590523,"_ignored":["bullet_point.keyword"],
"_source":{
"item_id":"B07HSG8DPZ",
"item_keywords":["sliding","small","rotating","game","albums","electronics","movie","48","entertainment","records","turntable","book","racks","1000","mid","a/v","video","three","crosley","dvds","shelves","capacity","trestlewood","multi","towers","stands","dressers","bar","dvd","low","furniture","record","oak","dark","tall","stand","tower","spinner","strain","rack","large","profile","century"],
"bullet_point":["update home décor keeping media devices organized attractive contemporary-style media console made solid mango sealed french gray finish media table blends perfectly room geometric design cabinet doors adds interest piece","13.7 '' w x 57 '' x 20.8 '' h","solid hardwood wooden honeycomb design door fronts french gray finish","modern styling blend well home décor","tv cabinet fixed shelf right side removable shelf behind doors","assemble 15 minutes less","care clean required soft damp cloth wipe completely dry avoid placing furniture near heat vents open windows long periods heat excessive dampness cause splitting warping swelling solid wood protect fine furniture finishes hot dishes alcohol excessive moisture dampness","free returns 30 days 1-year warranty"],
"product_description":[],
"brand":["rivet"],
"color":[],
"fabric_type":[],
"finish_type":[],
"item_name":["amazon brand – rivet emerly contemporary media console center storage cabinet 57 '' w wood"],
"material":[],
"model_name":[],
"model_number":[],"model_year":[],"pattern":[],"style":[],"color_code":[],"country":"US","domain_name":"amazon.com",
"item_dimensions":{"width":"{unit=inches, value=14.14, normalized_value={value=14.14, unit=inches}}","length":"{unit=inches, value=57.45, normalized_value={value=57.45, unit=inches}}","height":"{unit=inches, value=15.72, normalized_value={value=15.72, unit=inches}}"},
"item_weight":["{unit=pounds, value=72.85, normalized_value={value=72.85, unit=pounds}}"],
"main_image_id":"816cWTn1XVL","marketplace":"Amazon","node":["{node_name=/Categories/Furniture/Living Room Furniture/TV & Media Furniture/Media Storage, node_id=3733321}"],"product_type":"[{value=HOME_FURNITURE_AND_DECOR}]","spin_id":"ee5048ae","3dmodel_id":"B07HSG8DPZ","other_image_id":["A1LwzUlsAyL","915wKOv4fvL","813AZUA-XtL","71bNZUb1TJL","81vW6hTgDUL"]}},

{"_index":"products","_id":"B075YV7TMR","_score":15.573798,"_source":{"item_id":"B075YV7TMR","item_keywords":["dining-chairs","dining chairs","dining room chairs","accent chair","chair","chairs","kitchen chairs","dining chair","dining chairs set 4","dining table set","accent chair set 2","rivet","mid century","modern","light gray","grey","light gray","dinning table set","dinning table","round dinning table","din
"""