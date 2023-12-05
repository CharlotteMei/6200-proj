from elasticsearch import Elasticsearch
from datetime import datetime

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

# Check if the connection was successful
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Could not connect to Elasticsearch")
    exit()

# Index a document
doc = {
    'author': 'John Doe',
    'text': 'Elasticsearch is great!',
    'timestamp': datetime.now(),
}

index_name = 'my_index'
doc_type = 'my_document'

res = es.index(index=index_name, body=doc)
print(res)

# Search for documents
search_query = {
    'query': {
        'match': {
            'text': 'Elasticsearch'
        }
    }
}

res = es.search(index=index_name, body=search_query)
print(res)
