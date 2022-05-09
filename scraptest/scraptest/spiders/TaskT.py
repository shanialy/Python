from turtle import title
from elasticsearch import Elasticsearch, helpers
import os, uuid
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

# normalizeProduct = {'jean': ['Jeans','Men Jeans', 'Women Jeans'],
#  'tshirt': ['Tshirt','Boys T-shirt', 'Men T-Shirt', 'Girls T-shirt', 'Women T-shirt'],
#   'trouser': ['Trouser','Boys Trouser', 'Men Trouser', 'Girls Trousers'], 
#   'polo': ['Polo','Men Polo', 'Boys Polo']}

# normalizeProduct = {'jean': ["jean","Women's Denim", "Men's Denim", "Girl's Denim"],
#  'tshirt': ["Tshirt","Men's Tee Shirt","Girl's Tee Shirt","Women's Tee Shirt"], 
#  'trouser': ["trouser","Women's Trousers", "Boy's Trousers", "Men's Trousers", "Girl's Trousers"],
# 'polo': ["polo","Men's Polo Shirt", "Boy's Polo Shirt", "Girl's Polo Shirt", "Women's Polo Shirt"]}


# normalizeProduct ={ 'tshirt': ['Boys T-Shirts', 'T-Shirt', 'Girls T-Shirt'], 
# 'trouser': ['Trouser', 'Girls Trouser', 'Boys Trouser', 'Woman Trouser'], 'polo': []}

# normalizeProduct ={'jean': ['Jean','DENIM'],
#  'trouser': ['trouser','TROUSER'], 'polo': ['POLO']}

normalizeProduct ={'jean': ['Jean','Boys Jeans', 'Men Jeans', 'Women Jeans'],
 'tshirt': ['Tshirt','Girls T-Shirt', 'M Boys T-Shirt', 'M Girls T-Shirt', 'Women T-Shirt',
  'Men T-Shirt', 'Boys T-Shirt'],
   'trouser': ['trouser','Men Trouser', 'Boys Trousers'], 
'polo': ['polo','Men Polo Shirt', 'Boys Polo Shirt']}

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
alldata = load_jsonl('E:\MyData\Data\Brandsriver.com.jsonl')  
productTypes=[]  
for item in alldata:
    try:
        productTypes.append(item['product']['product_type'])
    except Exception as e:
        print(e)   
productTypes = set(productTypes)
productTypes = list(productTypes)
product_types_normalyzed={}
print(productTypes)

def normalyzeJeans(productTypes):
    listJeansVeriations=[]   
    for j in productTypes:
     if('jean' in j.lower() or 'denim' in j.lower()):
        listJeansVeriations.append(j)
    return listJeansVeriations
def normalyzetshirt(productTypes):
    listtshirtVeriations=[]   
    for j in productTypes:
     if('tshirt' in j.lower() or 't shirt' in j.lower() or 't-shirt' in j.lower()):  
        listtshirtVeriations.append(j)
    return listtshirtVeriations  
def normalyzettrousers(productTypes):
    listtrousersVeriations=[]   
    for j in productTypes:
        if('trouser' in j.lower()):
            listtrousersVeriations.append(j)
    return listtrousersVeriations   
def normalyzetpolo(productTypes):
    listpoloVeriations=[]   
    for j in productTypes:
        if('polo' in j.lower()):
            listpoloVeriations.append(j)
    return listpoloVeriations                             

alljeansTypes=normalyzeJeans(productTypes)
product_types_normalyzed['jean']=alljeansTypes
alltshirtTypes=normalyzetshirt(productTypes)
product_types_normalyzed['tshirt']=alltshirtTypes
alltrousersTypes=normalyzettrousers(productTypes)
product_types_normalyzed['trouser']=alltrousersTypes
allpoloTypes=normalyzetpolo(productTypes)
product_types_normalyzed['polo']=allpoloTypes
print(product_types_normalyzed)
bulkChunk=[]
for i in alldata:
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
                        "domain":"Brandsriver.com"
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
                        "domain":"Brandsriver.com"
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
                        "domain":"Brandsriver.com"
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
                        "domain":"Brandsriver.com"
                        #domain lgana h list pe loop lgny se.
                        })
                        bulkChunk.append(doc)     
                except Exception as e:
                    print(e)        
        except Exception as e:
            print(e)    
    if len(bulkChunk)>=5:
        try:
            print("\n--------------------------------------------->\n")
            bulk(es, bulkChunk, index="pricechoice_v1",request_timeout=400)
            bulkChunk=[]
        except Exception as e:
            print(e)

 
