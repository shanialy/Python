from turtle import title
from elasticsearch import Elasticsearch, helpers
import os, uuid
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import tldextract
import redis

# def findWholeWord(w,s):

#                     result = re.search(r'\b({0})\b'.format(w), s,flags=re.IGNORECASE)
#                     if result:
#                         return True

#                     else:
#                         return False


#                 print(findWholeWord("women",s))


normalizeProduct ={'jean': ['jean','boys jeans', 'men jeans', 'women jeans','denim',
"women's denim","men's denim", "girl's denim"],
 'tshirt': ['tshirt','tee shirt','girls t-Shirt','t.shirt', 'm boys t-Shirt', 'm girls t-Shirt','women t-shirt',
  'men t-shirt', 'boys t-shirt',"men's tee shirt","girl's tee shirt","women's tee shirt",
  'boys t-shirts','boys t-shirt', 't-shirt', 'girls t-shirt',"boy's tee shirt",'girls t-shirt'],
   'trouser': ['trouser','men trouser', 'boys trousers','girls trousers',
   'girls trouser', 'boys trouser', 'woman trouser',
   "women's trousers", "boy's Trousers", "men's Trousers", "girl's trousers"], 
'polo': ['polo','men polo shirt', 'boys polo shirt','men polo', 'boys polo',
"men's polo shirt", "boy's polo shirt", "girl's polo shirt", "women's polo shirt",'tee polo']}




arr1 = os.listdir('D:/data/')
print(arr1)

es=Elasticsearch(['http://43.251.253.107:2500'])    

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
for item in arr1[:1]:
    webpage_data = load_jsonl('D:/data/'+item)
    ext = tldextract.extract(item)
    url = ext.subdomain
    bulkChunk=[]
    productsfoundperfile=0

    for i in webpage_data[:50]:
            # print(value)     
         
        try:
            titlewithproductType = i['product']['title']+ " "+i['product']['product_type']
            ptypeFound = False
            for value in normalizeProduct.values():  
                if ptypeFound == True:
                    break 
                print(value)
                print((str.lower()).split())
                print("================")
                # if value in (str.lower()).split():
                for ptype in value:
                    if any(ptype in s for s in (str.lower()).split()):
                        print(ptype)
                        print("++++++++++++++++++++++++++++++")
                        ptypeFound = True
                        break

                       
                
                    # try:
                    #     pTypenotFound = True
                    #     productType=""
                    #     gendernotFound = True
                    #     gender=""
                    #     if 'women' in titlewithproductType.lower() or 'woman' in titlewithproductType.lower():
                    #         gendernotFound = False
                    #         gender="women"
                    
                    #     if ' men' in titlewithproductType.lower() or " men's" in titlewithproductType.lower() or "Men" in titlewithproductType.lower() or " man" in titlewithproductType.lower():
                    #         gender="men"
                    #         gendernotFound = False

                    #     if 'girl' in titlewithproductType.lower():
                    #         gender="girl"
                    #         gendernotFound = False
                    #     if 'boy' in titlewithproductType.lower():
                    #         gender="boy"
                    #         gendernotFound = False
                        
                    #     if 'tshirt' in titlewithproductType.lower() or 'teeshirt' in titlewithproductType.lower() or 't-shirt' in titlewithproductType.lower() or 'tee shirt' in titlewithproductType.lower() or 't shirt' in titlewithproductType.lower() or 't.shirt' in titlewithproductType.lower():
                    #         pTypenotFound = False
                    #         productType="tshirt"
                    
                    #     if 'trouser' in titlewithproductType.lower() or "men trouser" in titlewithproductType.lower() or "girl trouser" in titlewithproductType.lower() or "boy trouser" in titlewithproductType.lower():
                    #         productType="trouser"
                    #         pTypenotFound = False

                    #     if 'jean' in str(i['product']['title']).lower()  or "denim" in str(i['product']['title']).lower():
                    #         productType="jean"
                    #         pTypenotFound = False
                    #     if 'polo' in titlewithproductType.lower()  or "men polo" in titlewithproductType.lower()  or "boy polo" in titlewithproductType.lower()  or "girl polo" in titlewithproductType.lower() or 'tee polo' in titlewithproductType.lower():
                    #         productType="polo"
                    #         pTypenotFound = False
                    #     if  gendernotFound == True:
                    #         gender=""    
                    #     if  pTypenotFound == True:
                    #         productType=""    
                    # except Exception as e:

                    #     pass  
                    

                    # else:
                        # print("")
        except Exception as e:
            pass    
