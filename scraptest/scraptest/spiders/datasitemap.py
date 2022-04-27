import requests
import json
import redis
import tldextract

client3 = redis.Redis(host = '43.251.253.107', port=1500,db=3)

sitenames = ['http://www.Brandspopper.com','http://www.Brandsego.com','http://www.Exportleftovers.com','http://jalpariclothing.com','http://www.Diners.com.pk','http://haseebsarwarclothing.com']
def dump_jsonl(data, output_path, append=False):
    """
    Write list of objects to a JSON lines file.
    """
    mode = 'a+' if append else 'w'
    with open(output_path, mode, encoding='utf-8') as f:
        for line in data:
            json_record = json.dumps(line, ensure_ascii=False)
            f.write(json_record + '\n')
    print('Wrote {} records to {}'.format(len(data), output_path))

for domain in sitenames:
    try:  
        ext = tldextract.extract(domain)
        url = ext.domain+'.'+ext.suffix
        allinks=client3.smembers(url)
        for link in list(allinks):
            link = link.decode("utf-8")
            if (link.find('/products/') != -1) or (link.find('/collections/') != -1):
                try:  
                    link = link+'.json'
                    response = requests.get(link)
                    json_data = json.loads(response.text)
                    dump_jsonl([json_data], 'new'+url+'.jsonl',append=True)
                except Exception as e:
                    print(e)    

    except Exception as e:
        print(e)




