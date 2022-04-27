from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import os, uuid
import json
try:
    ELASTIC_PASSWORD = "JyjtRAAGBQNH8e6Ex7ftveXK"
    CLOUD_ID = "nothing:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDczNzYzMjg5MDczOTRmZWE4ZDlhNWFmZjE5ZDA1ODQzJDUzZTc3OTdmMmE1MTRlMjY4ZmU0Y2Y5MmY0NTM2NDYy"
    client = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    client.info()
except Exception as e:
    print(e)    


def load_jsonl(input_path) -> list:
    """
    Read list of objects from a JSON lines file.
    """
    data = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line.rstrip('\n|\r')))
    print('Loaded {} records from {}'.format(len(data), input_path))
    return data 
alldata = load_jsonl('E:\MyData\Data\Brandsego2.com.jsonl')    
print(len(alldata))

actions = [
    {
        "_index":"brandsego",
        # "_type":"doc",
        "_id":uuid.uuid1(),
        "_source":{
            "title":str(i['product']['title']),
            "published_at":str(i['product']['published_at']),
            "image":str(i['product']['image']['src']),
            "options":str(i['product']['options']),
            "timestamp":datetime.now()
        }
    }
    for i in alldata
] 
 



try:
    response = helpers.bulk(client, actions)
    print ("\nRESPONSE:", response)
except Exception as e:
    print("\nERROR:", e)
