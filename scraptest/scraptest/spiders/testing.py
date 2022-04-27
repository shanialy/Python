import os
import json
import redis
arr1 = os.listdir('D:/test')
client = redis.Redis(host = '43.251.253.107', port=1500,db=1)
# print(arr1)
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
for i in arr1:
    webpage_data = load_jsonl('D:/test/'+i)
    for item in webpage_data:
        try:
            client.sadd('Product',item['product']['product_type'])
        except Exception as e:
            print(e)     

