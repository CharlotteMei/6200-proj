import os
from elasticsearch import Elasticsearch
import os
import re
es = Elasticsearch("http://localhost:9200")
index = "doc_index"
folder_path = "./AP_DATA/ap89_collection"

def create_index(documents, es):
    """
    Function to create index and add documents
    documents: an array of doc text
    {
        "DOCNO": "value",
        "FILEID": "value",
        "HEAD": "value",
        "BYLINE": "value",
        "DATELINE": "value",
        "TEXT": "value"
    }
    """
    # Define index mapping
    index_mapping = {
        "mappings": {
            "properties": {
                "DOCNO": {"type": "text"},
                "FILEID": {"type": "text"},
                "HEAD": {"type": "text"},
                "BYLINE": {"type": "text"},
                "DATELINE": {"type": "text"},
                "TEXT": {"type": "text"}
            }
        }
    }
    
    # Create index (empty)

    es.indices.create(index=index, body=index_mapping, ignore=400)

    # Add documents to index
    for doc_text in documents:
        parsed_doc = parse_document(doc_text)
        es.index(index=index, id=parsed_doc["DOCNO"], body=parsed_doc)

def loop_through_folder(folder_path):
    """
    Loop through all documents in the AP89 collection and parse them
    return: an array of doc_texts
    """
    print("start processing the folder")
    documents_collection = []
    for filename in os.listdir(folder_path):
        with open(os.path.join(folder_path, filename), 'r', encoding='latin-1') as f:
            print("parsing file: ", filename, " ...")
            file_text = f.read()
            parse_file(file_text=file_text, documents_collection=documents_collection)
            
    return documents_collection

def parse_file(file_text, documents_collection):
    """
    parse one file, add the multiple doc_text to the document collections
    
    """
    # Use regular expressions to find all occurrences of <DOC>...</DOC>
    doc_matches = re.finditer(r'<DOC>(.*?)</DOC>', file_text, re.DOTALL)
    print("found ", len(list(doc_matches)), " documents in this file")

    # Process each document match
    for doc_match in doc_matches:
        # Extract the text between <DOC> and </DOC>
        doc_text = doc_match.group(1)
        documents_collection.append(doc_text)


def parse_document(doc_text):
    """
    from one <doc></doc> content to a dictionary of mapped fields
    can be used in the es.index() function
    """
    # Use regular expressions to extract information from the document
    docno_match = re.search(r'<DOCNO>(.*?)</DOCNO>', doc_text)
    fileid_match = re.search(r'<FILEID>(.*?)</FILEID>', doc_text)
    head_matches = re.findall(r'<HEAD>(.*?)</HEAD>', doc_text)
    byline_matches = re.findall(r'<BYLINE>(.*?)</BYLINE>', doc_text)
    dateline_match = re.search(r'<DATELINE>(.*?)</DATELINE>', doc_text)
    text_match = re.search(r'<TEXT>(.*?)</TEXT>', doc_text, re.DOTALL)

    # Return a dictionary with the extracted fields
    return {
        "DOCNO": docno_match.group(1) if docno_match else None,
        "FILEID": fileid_match.group(1) if fileid_match else None,
        "HEAD": head_matches if head_matches else None,
        "BYLINE": byline_matches if byline_matches else None,
        "DATELINE": dateline_match.group(1) if dateline_match else None,
        "TEXT": text_match.group(1) if text_match else None
    }
    
documents = loop_through_folder(folder_path)
print("finished processing the folder with ", len(documents), " documents")
print("start creating index")
create_index(documents, es)
