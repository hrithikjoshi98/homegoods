import hashlib
from typing import Iterable
from urllib.parse import urlparse
import parsel
import scrapy
import json
from scrapy import Request
from scrapy.cmdline import execute
from homegoods.items import HomegoodsItem
from homegoods.db_config import config
import pymysql
from datetime import datetime
import os
import gzip
from parsel import Selector
import re


def remove_extra_space(row_data):
    # Remove any extra spaces or newlines created by this replacement
    value = re.sub(r'\s+', ' ', row_data).strip()
    # Update the cleaned value back in row_data
    return value


def generate_hashid(url: str) -> str:
    # Parse the URL and use the netloc and path as a unique identifier
    parsed_url = urlparse(url)
    unique_string = parsed_url.netloc + parsed_url.path
    # Create a hash of the unique string using SHA-256 and take the first 8 characters
    hash_object = hashlib.sha256(unique_string.encode())
    hashid = hash_object.hexdigest()[:8]  # Take the first 8 characters
    return hashid

class HomegodSpider(scrapy.Spider):
    name = "homegod"
    start_urls = ["https://www.homegoods.com/all-stores"]

    def __init__(self, start_id, end_id, **kwargs):
        super().__init__(**kwargs)
        self.start_id = start_id
        self.end_id = end_id

        self.conn = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            db=config.database,
            autocommit=True
        )
        self.cur = self.conn.cursor()

        self.domain = self.start_urls[0].split('://')[1].split('/')[0]
        self.date = datetime.now().strftime('%d_%m_%Y')

        self.folder_name = self.domain.replace('.', '_').strip()
        config.file_name = self.folder_name

        self.html_path = 'C:\page_source\\' + self.date + '\\' + self.folder_name + '\\'
        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)
        # print(self.domain, self.folder_name, self.sql_table_name)
        self.sql_table_name = self.folder_name + f'_{self.date}' + '_USA'

        config.db_table_name = self.sql_table_name

        # print(self.sql_table_name)
        field_list = []
        value_list = []
        item = ('store_no', 'name', 'latitude', 'longitude', 'street', 'city',
                  'state', 'zip_code', 'county', 'phone', 'open_hours', 'url',
                  'provider', 'category', 'updated_date', 'country', 'status',
                  'direction_url', 'pagesave_path')
        for field in item:
            field_list.append(str(field))
            value_list.append('%s')
        config.fields = ','.join(field_list)
        config.values = ", ".join(value_list)

        self.cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.sql_table_name}(id int AUTO_INCREMENT PRIMARY KEY,
                                    store_no varchar(100) DEFAULT 'N/A',
                                    name varchar(100) DEFAULT 'N/A',
                                    latitude varchar(100) DEFAULT 'N/A',
                                    longitude varchar(100) DEFAULT 'N/A',
                                    street varchar(500) DEFAULT 'N/A',
                                    city varchar(100) DEFAULT 'N/A',
                                    state varchar(100) DEFAULT 'N/A',
                                    zip_code varchar(100) DEFAULT 'N/A',
                                    county varchar(100) DEFAULT 'N/A',
                                    phone varchar(100) DEFAULT 'N/A',
                                    open_hours varchar(500) DEFAULT 'N/A',
                                    url varchar(500) DEFAULT 'N/A',
                                    provider varchar(100) DEFAULT 'N/A',
                                    category varchar(100) DEFAULT 'N/A',
                                    updated_date varchar(100) DEFAULT 'N/A',
                                    country varchar(100) DEFAULT 'N/A',
                                    status varchar(100) DEFAULT 'N/A',
                                    direction_url varchar(500) DEFAULT 'N/A',
                                    pagesave_path varchar(500) DEFAULT 'N/A'
                                    )""")

    cookies = {
        's_fid': '33EFED89642DADB8-24CF11560BC75F7F',
        '_ga': 'GA1.1.46407469.1732627632',
        'akaalb_www_homegoods_com_alb': '~op=:~rv=45~m=~os=698f3717b47f60b3161d8805543209a1~id=e9e51b8b269463a9af6e247d4caa0b5d',
        'x-tjx-CBTC': '6fb755b9a88d1630716885b553ca1ab2',
        '_abck': '8DC54F4B1BFD84A7718C3F24BE9AA443~-1~YAAQNHLBF6aK9jaTAQAAtDm2hQzINwLlnLwBx2SIxj1Sqh5s+En8EoceA9k3C/2IZQ1s7PjzhiF5M18kJhdltlnn8BrPoTJ/919Rv0CmLIUG9X6fUBmCvg4eiV5S8TdKosyrKQ690Ev+IREH87LsSby6ohWVQTpNtWr7s8hYW/WBKYhV4dM0oQ2suJbIFL3TyMqah5vDzlkRM9cWqQlzN73ts06ow/oS10o6zp3TgrUU8HCknQsKRQqweXlKRrEnkhTEf+tib2rG86ywxUM1Hx4T9vaAgSoVUcfzcKeXKufq8xUHVrwLOjDXykZlNWUPrc5jwYGbttAohfDhEMFPjS7Hv1cGAYKSlQM+oW0DMsAs8zZ16jMWbcrVUvawMr8HJkrJy5j0AuY8/qYUt4GX+bOuaqfN3XEd2AD2Tg4xStgyuKGUkC4kRtnrpYp75w==~-1~-1~-1',
        's_cc': 'true',
        'bm_mi': '49F71B68CEFCF58BAA412786F43F2E27~YAAQN3LBFwrzz0CTAQAAq9a2hRlLGXOrRzKntncbGgczg25bWSK30xpvEvIZ2gFmnKzoI/40mgMmZ8fIu2FePVebuuo9MYoQamsaGVpnbbB1DJ4zyQc83R2KdzRcUpzaOv3jhbBctgb+Rs82We7lwmAv289H4k95SuscDoxCO2g9PpNBwvv/GAZx80m5dldTz2b1RyLtxzRPdwzBmouVPryDKN+5saPYmgw5rAJGyzbewJ4tCOc3gL43pQLv7aB+9hmPcEralQycw0pwrwqetWn4yde1UjSxz+VfzczMQ51p8+ZYIfzldLoWzNYqpw9JVSVV7qF/LDcw08I=~1',
        's_sq': '%5B%5BB%5D%5D',
        'ak_bmsc': '8147F444595BF9B5E733F7FA3899EBEF~000000000000000000000000000000~YAAQNHLBF1Wu9jaTAQAAiCy3hRm+/5BPziw+S6S5sdozf6EilcQz2Tz3Fm91xHgSpkyI90gzia3BNgcqXHe/TBZxPm3Cjw04KQefJJpjc4kgT22fQIYEX/2HV1PgFWROALHeTCEf/r9l6FJo0F1FkP9rvAPbKjHWVSXok0MszXB65jwzj/7YQG07C5puUVSf3PQHjcmyWWU7kAEtQ0447BZZlFTQQHG1tgzqIZg/tYW7P61vpagLTAWX00SUYJZIQ5780OUIn3WkAEG8shUNTp+XP5ekdzv4+muC2VtPcKp6N8wqQUaSwi4cl2krjNOvhSaMyy5YayiYE5OoCIUJVz57hbU1LRCJUO2oV8ozHCb8Mp0JrRoCo1h0JmsQABE/QK30T7pZwu1isJO1Adz/kyujGc49dmzbttEshUdAjvXLWDgdM/NKEC4fByUy3+GDxg==',
        'bm_sz': 'EF6F55349A783418B53A618171DFF003~YAAQNHLBFzXJ9jaTAQAABt23hRmQTJRShkguN6D60X9RdmwKTVoTgPd32SiwBrPp9zDHAa3uNoV/E+IW6X77jDLZdUJkEGNZ+tdaScEt406NeAvBcut3KjcdskJsqNortoj51jFL66Cq78ZHsUZ+EFo83cO9PVCahNEPHn/lfJHB4oQpjcvIaACvWHWkfXH0rp16YoMljzyL9rlOG6sr2QhWVe7bidG1xzGohQmmALS5l4gs0bUrHjBEsGfpaqTfwOmkxn69+w48EwaUfBZxhkmXbhw6uC6SMn3dZbxoMoexM4TsGb19UJrGwDrOjQ3Si19KtFWJW/z5wg3M7POWbCb4CVyWspLhekIcamEkAc/5GA+WhFVSdYyUZTWbiJPOKFVUo9QgdLLzwcC4XZA4ChNgsV2qUD4NUOZBPw8MfhwFL+91kkMmVlbr~3359539~3422260',
        'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Dec+02+2024+10%3A24%3A01+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=f5107e6a-2ee2-4a14-ac41-cd5cbc0f93fb&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=2%3A1%2CC0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false',
        '_ga_E4PCS1FX8R': 'GS1.1.1733115133.2.1.1733115277.60.0.0',
        'akavpau_www_homegoods_vp': '1733115605~id=9eb3473c9f658c7b130349be394c0ad2',
        'bm_sv': '97057A408686D548B91EA5648F16875E~YAAQNHLBF3Lu9jaTAQAAx924hRlD7uJM/1iCrGwCP8ud/mY5RACp8aM8V220izQoxGoeJN9cuBdLYYDxnAbVXXqHq9305ctaAfR5XlEPYPxFllS2gUuCiTvsnitGgf5wMjirBxZkUpp4qWEYMm6G/1eL0404mATIh6WGIara3yywuy1fGfzuUP8ot6fAikE7+UQV3n1vCtjrBBI0rsgIPI3Wxc0HcuQSRKNOi+212YoNBbYwTTNrOycHM5lLnL+l8CQyFA==~1',
        's_nr30': '1733115355390-Repeat',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,tr;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 's_fid=33EFED89642DADB8-24CF11560BC75F7F; _ga=GA1.1.46407469.1732627632; akaalb_www_homegoods_com_alb=~op=:~rv=45~m=~os=698f3717b47f60b3161d8805543209a1~id=e9e51b8b269463a9af6e247d4caa0b5d; x-tjx-CBTC=6fb755b9a88d1630716885b553ca1ab2; _abck=8DC54F4B1BFD84A7718C3F24BE9AA443~-1~YAAQNHLBF6aK9jaTAQAAtDm2hQzINwLlnLwBx2SIxj1Sqh5s+En8EoceA9k3C/2IZQ1s7PjzhiF5M18kJhdltlnn8BrPoTJ/919Rv0CmLIUG9X6fUBmCvg4eiV5S8TdKosyrKQ690Ev+IREH87LsSby6ohWVQTpNtWr7s8hYW/WBKYhV4dM0oQ2suJbIFL3TyMqah5vDzlkRM9cWqQlzN73ts06ow/oS10o6zp3TgrUU8HCknQsKRQqweXlKRrEnkhTEf+tib2rG86ywxUM1Hx4T9vaAgSoVUcfzcKeXKufq8xUHVrwLOjDXykZlNWUPrc5jwYGbttAohfDhEMFPjS7Hv1cGAYKSlQM+oW0DMsAs8zZ16jMWbcrVUvawMr8HJkrJy5j0AuY8/qYUt4GX+bOuaqfN3XEd2AD2Tg4xStgyuKGUkC4kRtnrpYp75w==~-1~-1~-1; s_cc=true; bm_mi=49F71B68CEFCF58BAA412786F43F2E27~YAAQN3LBFwrzz0CTAQAAq9a2hRlLGXOrRzKntncbGgczg25bWSK30xpvEvIZ2gFmnKzoI/40mgMmZ8fIu2FePVebuuo9MYoQamsaGVpnbbB1DJ4zyQc83R2KdzRcUpzaOv3jhbBctgb+Rs82We7lwmAv289H4k95SuscDoxCO2g9PpNBwvv/GAZx80m5dldTz2b1RyLtxzRPdwzBmouVPryDKN+5saPYmgw5rAJGyzbewJ4tCOc3gL43pQLv7aB+9hmPcEralQycw0pwrwqetWn4yde1UjSxz+VfzczMQ51p8+ZYIfzldLoWzNYqpw9JVSVV7qF/LDcw08I=~1; s_sq=%5B%5BB%5D%5D; ak_bmsc=8147F444595BF9B5E733F7FA3899EBEF~000000000000000000000000000000~YAAQNHLBF1Wu9jaTAQAAiCy3hRm+/5BPziw+S6S5sdozf6EilcQz2Tz3Fm91xHgSpkyI90gzia3BNgcqXHe/TBZxPm3Cjw04KQefJJpjc4kgT22fQIYEX/2HV1PgFWROALHeTCEf/r9l6FJo0F1FkP9rvAPbKjHWVSXok0MszXB65jwzj/7YQG07C5puUVSf3PQHjcmyWWU7kAEtQ0447BZZlFTQQHG1tgzqIZg/tYW7P61vpagLTAWX00SUYJZIQ5780OUIn3WkAEG8shUNTp+XP5ekdzv4+muC2VtPcKp6N8wqQUaSwi4cl2krjNOvhSaMyy5YayiYE5OoCIUJVz57hbU1LRCJUO2oV8ozHCb8Mp0JrRoCo1h0JmsQABE/QK30T7pZwu1isJO1Adz/kyujGc49dmzbttEshUdAjvXLWDgdM/NKEC4fByUy3+GDxg==; bm_sz=EF6F55349A783418B53A618171DFF003~YAAQNHLBFzXJ9jaTAQAABt23hRmQTJRShkguN6D60X9RdmwKTVoTgPd32SiwBrPp9zDHAa3uNoV/E+IW6X77jDLZdUJkEGNZ+tdaScEt406NeAvBcut3KjcdskJsqNortoj51jFL66Cq78ZHsUZ+EFo83cO9PVCahNEPHn/lfJHB4oQpjcvIaACvWHWkfXH0rp16YoMljzyL9rlOG6sr2QhWVe7bidG1xzGohQmmALS5l4gs0bUrHjBEsGfpaqTfwOmkxn69+w48EwaUfBZxhkmXbhw6uC6SMn3dZbxoMoexM4TsGb19UJrGwDrOjQ3Si19KtFWJW/z5wg3M7POWbCb4CVyWspLhekIcamEkAc/5GA+WhFVSdYyUZTWbiJPOKFVUo9QgdLLzwcC4XZA4ChNgsV2qUD4NUOZBPw8MfhwFL+91kkMmVlbr~3359539~3422260; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Dec+02+2024+10%3A24%3A01+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=f5107e6a-2ee2-4a14-ac41-cd5cbc0f93fb&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=2%3A1%2CC0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false; _ga_E4PCS1FX8R=GS1.1.1733115133.2.1.1733115277.60.0.0; akavpau_www_homegoods_vp=1733115605~id=9eb3473c9f658c7b130349be394c0ad2; bm_sv=97057A408686D548B91EA5648F16875E~YAAQNHLBF3Lu9jaTAQAAx924hRlD7uJM/1iCrGwCP8ud/mY5RACp8aM8V220izQoxGoeJN9cuBdLYYDxnAbVXXqHq9305ctaAfR5XlEPYPxFllS2gUuCiTvsnitGgf5wMjirBxZkUpp4qWEYMm6G/1eL0404mATIh6WGIara3yywuy1fGfzuUP8ot6fAikE7+UQV3n1vCtjrBBI0rsgIPI3Wxc0HcuQSRKNOi+212YoNBbYwTTNrOycHM5lLnL+l8CQyFA==~1; s_nr30=1733115355390-Repeat',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        yield scrapy.Request(
            'https://www.homegoods.com/all-stores',
                             # cookies=self.cookies,
                             headers=self.headers,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        list_of_stores = selector.xpath('//ul[@class="states-list"]//a[text()="Store info and directions"]/@href').getall()

        for store in list_of_stores:
            yield scrapy.Request(
                'https://www.homegoods.com' + store,
                headers=self.headers,
                # cookies=self.cookies,
                callback=self.get_store_detail
            )

    def get_store_detail(self, response):
        selector = Selector(response.text)
        item = HomegoodsItem()
        store_no = str(response.url).split('/')[-1].strip()

        name = remove_extra_space(selector.xpath('//div[@class="store-info"]/h1//text()').get('N/A'))

        latitude = 'N/A'
        longitude = 'N/A'

        street = remove_extra_space(' '.join(selector.xpath('//div[@class="store-info"]/h2//text()').getall()))

        city = street.split(',')[0].split(' ')[-1].strip()
        state, zip_code = street.split(',')[-1].strip().split(' ')

        county = 'N/A'
        phone = remove_extra_space(selector.xpath('//div[@class="store-info"]//a[@data-link="Phone Number:Call"]/text()').get('N/A'))

        url = response.url

        open_hours = remove_extra_space(' '.join(selector.xpath('//*[@id="store-info-container"]/div/p[1]//text()').getall())).strip()
        open_hours = ' '.join(open_hours.split(' ')[1:])

        provider = 'HomeGoods'
        category = 'Apparel And Accessory Stores'

        updated_date = datetime.now().strftime("%d-%m-%Y")
        country = 'USA'
        status = 'Open'

        direction_url = selector.xpath('//*[@id="store-info-container"]//a[@class="link directions"]/@href').get()

        page_id = generate_hashid(response.url)
        pagesave_path = self.html_path + fr'{page_id}' + '.html.gz'

        gzip.open(pagesave_path, "wb").write(response.body)

        item['store_no'] = store_no
        item['name'] = name
        item['latitude'] = latitude
        item['longitude'] = longitude
        item['street'] = street
        item['city'] = city
        item['state'] = state
        item['zip_code'] = zip_code
        item['county'] = county
        item['phone'] = phone
        item['open_hours'] = open_hours
        item['url'] = url
        item['provider'] = provider
        item['category'] = category
        item['updated_date'] = updated_date
        item['country'] = country
        item['status'] = status
        item['direction_url'] = direction_url
        item['pagesave_path'] = pagesave_path
        yield item

if __name__ == '__main__':
    # execute("scrapy crawl kia".split())
    execute(f"scrapy crawl homegod -a start_id=0 -a end_id=100 -s CONCURRENT_REQUESTS=6".split())