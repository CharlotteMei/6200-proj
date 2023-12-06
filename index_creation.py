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
    "mappings": {
        "properties": {
            "row_id": {"type": "integer"},
            "country": {"type": "text"},
            "description": {"type": "text"},
            "designation": {"type": "text"},
            "points": {"type": "integer"},
            "price": {"type": "float"},
            "province": {"type": "text"},
            "region_1": {"type": "text"},
            "region_2": {"type": "text"},
            "variety": {"type": "text"},
            "winery": {"type": "text"}
        },
    },
    
}

# Delete the existing index
es.indices.delete(index=index_name, ignore=[400, 404])

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=index_mapping)

# Iterate through each row and insert into Elasticsearch
count = 0
for index, row in df.iterrows():
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
