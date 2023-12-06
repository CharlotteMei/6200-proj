import pandas as pd
from elasticsearch import Elasticsearch, helpers

csv_file_path = './raw_data/wine_review/winemag-data-130k-v2.csv'  # Replace with the actual path to your CSV file
index_name = 'wine_data'

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv(csv_file_path)

# Elasticsearch configuration
es = Elasticsearch("http://localhost:9200")  # Replace with your Elasticsearch server information

# Define the index mapping
index_mapping = {
    "mappings": {
        "properties": {
            "row_id": {"type": "integer"},
            "country": {"type": "keyword"},
            "description": {"type": "text"},
            "designation": {"type": "keyword"},
            "points": {"type": "integer"},
            "price": {"type": "float"},
            "province": {"type": "keyword"},
            "region_1": {"type": "keyword"},
            "region_2": {"type": "keyword"},
            "taster_name": {"type": "keyword"},
            "taster_twitter_handle": {"type": "keyword"},
            "title": {"type": "text"},
            "variety": {"type": "keyword"},
            "winery": {"type": "keyword"}
        }
    }
}


# Delete the existing index
es.indices.delete(index=index_name, ignore=[400, 404])

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_mapping)

# Prepare data for bulk indexing
data = df.to_dict(orient='records')
actions = [
    {
        "_index": index_name,
        "_source": doc
    }
    for doc in data
]

# Perform bulk indexing
helpers.bulk(es, actions)

print(f"Data indexed successfully into '{index_name}' index.")
