# app.py
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
import os
import re

app = Flask(__name__)
es = Elasticsearch("http://localhost:9200")
doc_index = "wine_data"

# ==================== Elasticsearch ====================

    
# ==================== Flask ====================
@app.route('/')
def index():
    es.indices.refresh(index="doc_index")
    
    # Use the indices API to get information about the index
    index_info = es.indices.get(index=doc_index)
    document_count = es.count(index=doc_index)['count']

    # Print the retrieved information
    print(document_count)
    
    mapping = es.indices.get_mapping(index=doc_index)

    # Print the mapping
    print(mapping) 
    
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    query = request.form.get('query')
    # Use Elasticsearch to perform the search
    print("query: ", query)
    es_query = {
        "size": 20,
        "query": {
            "dis_max": {
                "queries": [
                    {"match": {"country": query}},
                    {"match": {"description": query}},
                    {"match": {"designation": query}},
                    {"match": {"province": query}},
                    {"match": {"region_1": query}},
                    {"match": {"variety": query}},
                ],
                "tie_breaker": 0.3
            }
        }
    }

    
    results = es.search(index=doc_index, body=es_query)
    print("Got result length: ", len(results['hits']['hits']))
    return render_template('results.html', results=results['hits']['hits'])

# ==================== Main ====================
    
if __name__ == '__main__':
    
    # prepare elastic search instance
    app.run(debug=True)
