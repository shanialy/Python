import json
import redis
import os, uuid
arr1 = os.listdir('E:/MyData/DataForTask')
print(arr1)
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
list1 = []
for i in arr1:
    webpage_data = load_jsonl('E:/MyData/DataForTask/'+i)
    for item in webpage_data:
        try:
            list1.append(str(item['product']['product_type']))    
        except:
            pass        
print(len(list1))        
list1 = set(list1)
list1 = list(list1)
print(len(list1))
for Ptype in list1: 
    with open ('ProductTypesof5Sites.txt','a') as f:
        f.write(Ptype)
        f.write("\n")
        f.close()