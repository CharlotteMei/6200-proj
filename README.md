# 6200-proj
wine searching

## elastic search local host
```
cd /Users/charlottemei/Downloads/elasticsearch-8.11.1
./bin/elasticsearch
```
and wait for a couple minutes

## run search engine
```
source venv/bin/activate
pip install Flask elasticsearch pandas
```
to prepare the environment

```
python3 index_creation.py
```
to prepare the index 
```
python3 app.py
```
to run the app
