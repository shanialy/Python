from turtle import title
from elasticsearch import Elasticsearch, helpers
import os, uuid
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
# try:
#     ELASTIC_PASSWORD = "JyjtRAAGBQNH8e6Ex7ftveXK"
#     CLOUD_ID = "nothing:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDczNzYzMjg5MDczOTRmZWE4ZDlhNWFmZjE5ZDA1ODQzJDUzZTc3OTdmMmE1MTRlMjY4ZmU0Y2Y5MmY0NTM2NDYy"
#     client = Elasticsearch(
#         cloud_id=CLOUD_ID,
#         basic_auth=("elastic", ELASTIC_PASSWORD)
#     )
#     client.info()
# except Exception as e:
#     print(e)  

normalizeProduct ={'jean': ['Jean','Boys Jeans', 'Men Jeans', 'Women Jeans','DENIM',
"Women's Denim","Men's Denim", "Girl's Denim"],
 'tshirt': ['Tee Shirt','Girls T-Shirt', 'M Boys T-Shirt', 'M Girls T-Shirt','Women T-Shirt',
  'Men T-Shirt', 'Boys T-Shirt',"Men's Tee Shirt","Girl's Tee Shirt","Women's Tee Shirt",
  'Boys T-Shirts','Boys T-shirt', 'T-Shirt', 'Girls T-Shirt',"Boy's Tee Shirt",'Girls T-shirt'],
   'trouser': ['Trouser','TROUSER','Men Trouser', 'Boys Trousers','Girls Trousers',
   'Girls Trouser', 'Boys Trouser', 'Woman Trouser',
   "Women's Trousers", "Boy's Trousers", "Men's Trousers", "Girl's Trousers"], 
'polo': ['Polo','POLO','polo','Men Polo Shirt', 'Boys Polo Shirt','Men Polo', 'Boys Polo',
"Men's Polo Shirt", "Boy's Polo Shirt", "Girl's Polo Shirt", "Women's Polo Shirt"]}

arr1 = os.listdir('E:/MyData/DataForTask')
print(arr1)

es=Elasticsearch(['http://43.251.253.107:1200'])    

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
for item in arr1:
    webpage_data = load_jsonl('E:/MyData/DataForTask/'+item)
    bulkChunk=[]
    for i in webpage_data:
        for value in normalizeProduct.values():
            try:
                if str(i['product']['product_type']) in value:
                    
                    try:
                        if 'women' in str(i['product']['title']).lower() or 'woman' in str(i['product']['title']).lower():
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":value[0],
                            "gender":"women",
                            "domain":".com"
                            #domain lgana h list pe loop lgny se.
                            })
                            bulkChunk.append(doc)
                        if ' men' in str(i['product']['title']).lower() or " men's" in str(i['product']['title']).lower() or " man" in str(i['product']['title']).lower():
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":value[0],
                            "gender":"men",
                            "domain":".com"
                            #domain lgana h list pe loop lgny se.
                            })
                            bulkChunk.append(doc)    
                        if 'girl' in str(i['product']['title']).lower():
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":value[0],
                            "gender":"girls",
                            "domain":".com"
                            #domain lgana h list pe loop lgny se.
                            })
                            bulkChunk.append(doc)    
                        if 'boy' in str(i['product']['title']).lower():
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":value[0],
                            "gender":"boys",
                            "domain":".com"
                            #domain lgana h list pe loop lgny se.  
                            })
                            bulkChunk.append(doc)
                        else:
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":value[0],
                            "gender":"",
                            "domain":".com" })
                            bulkChunk.append(doc)  

                    except Exception as e:
                        print(e)        
            except Exception as e:
                print(e)    
        if len(bulkChunk)>=5:
            try:
                print("\n--------------------------------------------->\n")
                bulk(es, bulkChunk, index="pricechoice_v2",request_timeout=400)
                bulkChunk=[]
            except Exception as e:
                print(e)

    
