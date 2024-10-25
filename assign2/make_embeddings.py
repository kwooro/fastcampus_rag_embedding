import os, gzip, json, time
from sentence_transformers import SentenceTransformer, models
import torch


class MakeEmbeddings:
    def __init__(self):
        # MPS가 사용 가능한지 확인
        device = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")
        print('device:', device)
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

    def locale_filter(self, data, locale='en_US'):
        return [data['value'] for data in data if data.get('language_tag') == locale]

    def get_product_item_id(self, product):
        return product.get('item_id', '')

    def get_product_descriptions(self, product, locale='en_US'):
        descriptions = self.locale_filter(product.get('product_description', []), locale)
        return descriptions

    def get_product_bullet_points(self, product, locale='en_US'):
        bullet_points = self.locale_filter(product.get('bullet_point', []), locale)
        return bullet_points

    def get_product_name(self, product, locale='en_US'):
        item_name = self.locale_filter(product.get('item_name', []), locale)
        return item_name[0] if item_name else ''

    def file_iterator(self, file_path):
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)

    # convert to structured product document
    def get_product_doc(self, product):
        item_id = self.get_product_item_id(product)
        name = self.get_product_name(product)
        bullet_points = self.get_product_bullet_points(product)
        descriptions = self.get_product_descriptions(product)
        doc = {}
        if name:
            text = []
            text.append("Product Name: %s" % name)
            if bullet_points:
                text.append("- bullet points: %s" % ','.join(bullet_points))
            if descriptions:
                text.append("- description: %s" % ','.join(descriptions))
            doc[item_id] = '\n'.join(text)
        return doc

    def process_json_gz(self, file_path):
        text = []
        products = self.file_iterator(file_path)
        for product in products:
            doc = self.get_product_doc(product)
            if doc: text.append(doc)
        return text

    def create_embeddings(self):
        all_files = os.listdir("abo-listings/listings/metadata")

        list_data = []
        for file in all_files:
            file_path = 'abo-listings/listings/metadata/' + file
            data = self.process_json_gz(file_path)
            list_data.extend(data)
        
        
        # list_data[0]  = {'Wickedly Prime Mustard, White Wine Jalapeno, 11.75 Ounce': "Product Name: Wickedly Prime Mustard, White Wine Jalapeno, 11.75 Ounce\n- bullet points: One 11.75-ounce pastic squeeze bottle,Spice Level: Medium,Packed on shared equipment with egg, wheat, soy, milk, fish,Shown as serving suggestion,Satisfaction Guarantee: We're proud of our products. If you aren't satisfied, we'll refund you for any reason within a year of purchase. 1-877-485-0385,An Amazon brand"}

        print(f"created {len(list_data)} product documents")

        keys = []
        values = []
        embeddings = []
        # 모델 로드
        N = 100
        chunk_size = len(list_data) // N
        start_time = time.time()
        for i in range(N):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size
            for item in list_data[start_idx:end_idx]:
                for key, value in item.items():                
                    keys.append(key)
                    values.append(value)
                #print('key :' ,key)
            temp = self.embedding_model.encode(values[start_idx:end_idx])
            embeddings.extend(temp)
            print(f"each time taken: {time.time() - start_time} seconds")

        print(f"created {len(embeddings)} embeddings")

        return embeddings, keys, values
    
    def embeddings_row(self, query):
        embs = self.embedding_model.encode([query])
        return embs
# embeddings, keys, values = create_embeddings()

"""
test environment : MacBook Pro 16 M1, 16GB RAM

device: mps
each time taken: 1.175666093826294 seconds
each time taken: 2.333488941192627 seconds
each time taken: 3.3894028663635254 seconds
each time taken: 4.41292405128479 seconds
each time taken: 5.492260217666626 seconds

device: cpu
each time taken: 2.064741849899292 seconds
each time taken: 3.815886974334717 seconds
each time taken: 5.710629940032959 seconds
each time taken: 7.609477996826172 seconds
each time taken: 9.552083969116211 seconds
"""