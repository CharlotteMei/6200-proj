# 6200-proj
wine searching

## elastic search local host
```
cd /Users/charlottemei/Downloads/elasticsearch-8.11.1
./bin/elasticsearch
```
and wait for a couple minutes

## run search engine
### prepare the environment
```
source venv/bin/activate
pip install Flask elasticsearch pandas
```

### prepare the index
```
python3 index_creation.py
```
Ensure the index is created by checking `http://localhost:9200/wine_data`

### run the app
```
python3 app.py
```
to run the app. Go to `http://127.0.0.1:5000/` to use the app
