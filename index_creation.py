import time
import pandas as pd
from elasticsearch import Elasticsearch
  
csv_file_path = './raw_data/wine_review/winemag-data_first150k.csv'

index_name = 'wine_data'

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv(csv_file_path)

# Elasticsearch configuration
es = Elasticsearch("http://localhost:9200")  # Replace with your Elasticsearch server information

# Define the index mapping
index_mapping = {
    "settings": {
        "analysis": {
        "normalizer": {
            "my_normalizer": {
            "type": "custom",
            "char_filter": [],
            "filter": ["lowercase", "asciifolding"]
            }
        }
        }
    },
    "mappings": {
        "properties": {
            "row_id": {"type": "integer"},
            "country": {"type": "keyword", "normalizer": "my_normalizer"},
            "description": {"type": "text", "analyzer": "standard"},
            "designation": {"type": "keyword", "normalizer": "my_normalizer"},
            "points": {"type": "integer"},
            "price": {"type": "float"},
            "province": {"type": "keyword", "normalizer": "my_normalizer"},
            "region_1": {"type": "keyword", "normalizer": "my_normalizer"},
            "region_2": {"type": "keyword", "normalizer": "my_normalizer"},
            "variety": {"type": "keyword", "normalizer": "my_normalizer"},
            "winery": {"type": "keyword", "normalizer": "my_normalizer"}
        },
    },
    
}

# Delete the existing index
es.indices.delete(index=index_name, ignore=[400, 404])

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_mapping)

# Iterate through each row and insert into Elasticsearch
count = 0
for index, row in df.head(2000).iterrows():
    # parsed_row = json.loads(row, parse_float=float, parse_int=int, parse_constant=lambda x: x if x != 'NaN' else None)
    document = row.to_dict()
    document = {k: v for k, v in document.items() if v == v}  # Remove NaN values

    try:
        es.index(index=index_name, body=document)
        # print(f"Row {index} successfully indexed.")
    except Exception as e:
        print(f"Error indexing row {index}: {row}; document = {document}")
        
    count += 1
    if count % 1000 == 0:
        print(f"TS {time.time()}: Indexed {count} documents.")

print(f"Data indexed successfully into '{index_name}' index.")
