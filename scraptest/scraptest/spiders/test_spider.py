from unicodedata import name
import scrapy

class testSpider(scrapy.Spider):
    name = "test"
    start_urls = [
        'https://blog.scrapinghub.com',
        # 'https://blog.scrapinghub.com/page/2/'
    ]
    def parse(self,response):
        # page = response.url.split('/')[-1]
        filename = 'tests.html' 
        with open(filename,'wb') as f:
            f.write(response.body)
