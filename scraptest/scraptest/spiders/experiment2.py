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


normalizeProduct ={'jean': ['jean','boys jeans', 'men jeans', 'women jeans','denim',
"women's denim","men's denim", "girl's denim"],
 'tshirt': ['tshirt','tee shirt','girls t-Shirt', 'm boys t-Shirt', 'm girls t-Shirt','women t-shirt',
  'men t-shirt', 'boys t-shirt',"men's tee shirt","girl's tee shirt","women's tee shirt",
  'boys t-shirts','boys t-shirt', 't-shirt', 'girls t-shirt',"boy's tee shirt",'girls t-shirt'],
   'trouser': ['trouser','men trouser', 'boys trousers','girls trousers',
   'girls trouser', 'boys trouser', 'woman trouser',
   "women's trousers", "boy's Trousers", "men's Trousers", "girl's trousers"], 
'polo': ['polo','men polo shirt', 'boys polo shirt','men polo', 'boys polo',
"men's polo shirt", "boy's polo shirt", "girl's polo shirt", "women's polo shirt"]}

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
for item in arr1:
    webpage_data = load_jsonl('D:/data/'+item)
    ext = tldextract.extract(item)
    url = ext.subdomain
    bulkChunk=[]
    productsfoundperfile=0

    for i in webpage_data:
            
        try:
            titlewithproductType = i['product']['title']+ " "+i['product']['product_type']
            
            try:
                pTypenotFound = True
                productType=""
                gendernotFound = True
                gender=""
                if 'women' in titlewithproductType.lower() or 'woman' in titlewithproductType.lower():
        
                    gendernotFound = False
                    gender="women"
            
                if ' men' in titlewithproductType.lower() or " men's" in titlewithproductType.lower() or "Men" in titlewithproductType.lower() or " man" in titlewithproductType.lower():
                    gender="men"
                    gendernotFound = False

                if 'girl' in titlewithproductType.lower():
                    gender="girl"
                    gendernotFound = False
                if 'boy' in titlewithproductType.lower():
                    gender="boy"
                    gendernotFound = False
                if  gendernotFound == True:
                    gender=""
                if 'tshirt' in titlewithproductType.lower() or 'teeshirt' in titlewithproductType.lower() or 't-shirt' in titlewithproductType.lower() or 'tee shirt' in titlewithproductType.lower() or 't shirt' in titlewithproductType.lower() or 't.shirt' in titlewithproductType.lower():
                    pTypenotFound = False
                    productType="tshirt"
            
                if 'trouser' in titlewithproductType.lower() or "men trouser" in titlewithproductType.lower() or "girl trouser" in titlewithproductType.lower() or "boy trouser" in titlewithproductType.lower():
                    productType="trouser"
                    pTypenotFound = False

                if 'jean' in str(i['product']['title']).lower()  or "denim" in str(i['product']['title']).lower():
                    productType="jean"
                    pTypenotFound = False
                if 'polo' in titlewithproductType.lower()  or "men polo" in titlewithproductType.lower()  or "boy polo" in titlewithproductType.lower()  or "girl polo" in titlewithproductType.lower() or 'tee polo' in titlewithproductType.lower():
                    productType="polo"
                    pTypenotFound = False
                if  pTypenotFound == True:
                    productType=""    

            

                        
                
                try:
                    allPrices =[]
                    allComparePrice=[]
                    if len(i['product']['variants']) > 1:
                        for j in i['product']['variants']:
                            if j['compare_at_price'] == "":
                                allPrices.append(float(j['price']))
                            else:
                                allPrices.append(float(j['price']))
                                allComparePrice.append(float(j['compare_at_price']))
                        if len(allComparePrice)>0:    # matlab sale lagi ha in that case compare price contains old price and price has sale price
                            maxallPrices = max(allPrices)
                            maxallComparePrice = max(allComparePrice)  #price_before_sale
                            
                                                        
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":productType,
                            "gender":gender,
                            "domain":url,
                            "price":maxallPrices,
                            "price_before_sale":maxallComparePrice,
                            "is_on_sale":True})
                            bulkChunk.append(doc)
                            productsfoundperfile=productsfoundperfile+1


                        else: #product is not on sale and it has varients
                            maxallPrices = max(allPrices)
                            doc=json.dumps({"title":i['product']['title'],
                            "vendor":i['product']['vendor'],
                            "updatedAt":i['product']['updated_at'],
                            "image":i['product']['image']['src'],
                            "product_type":productType,
                            "gender":gender,
                            "domain":url,
                            "price":maxallPrices,
                            "price_before_sale":0,
                            "is_on_sale":False})
                            bulkChunk.append(doc)
                            productsfoundperfile=productsfoundperfile+1

        


                    else:  #if product has no varients
                        isOnSale=False
                        price= i['product']['variants'][0]['price']
                        price_before_sale =0
                        if i['product']['variants'][0]['compare_at_price']=="":
                            price_before_sale = 0
                            isOnSale=False
                        else:
                            price_before_sale = float(i['product']['variants'][0]['compare_at_price'])
                            isOnSale=True
                        doc=json.dumps({"title":i['product']['title'],
                        "vendor":i['product']['vendor'],
                        "updatedAt":i['product']['updated_at'],
                        "image":i['product']['image']['src'],
                        "product_type":productType,
                        "gender":gender,
                        "domain":url,
                        "price":price,
                        "price_before_sale":price_before_sale,
                        "is_on_sale":isOnSale
                        })
                        bulkChunk.append(doc)    
                        productsfoundperfile=productsfoundperfile+1
    
                except Exception as e:
                    print("at varient check",e)    

            except Exception as e:
                print("product type check",e)    
                    
                # else:
                    # print("")
        except Exception as e:
            # print("around loop",e)
            pass    
    if len(bulkChunk)>=5:
        try:
            # print("\n--------------------------------------------->\n")
            bulk(es, bulkChunk, index="experiment2",request_timeout=400)
            bulkChunk=[]
        except Exception as e:
            print(e)

print(productsfoundperfile,item)
print("\n\n--------------------------------\n")