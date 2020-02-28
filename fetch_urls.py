import bs4
import requests
import re
from datetime import datetime
import time
from pandas import DataFrame as df

resp_pattern = re.compile(r'<a href="/services/responses/bank/response/\d+/')
supalen = len('<a href="/services/responses/bank/response/')

ids = []

def fetch_url(url):
    while(1):
        try:
            resp = requests.get(url)
        except:
            print('sleep', page, 'th')
            time.sleep(10)
            continue
        else:
            break
    result = resp_pattern.findall(resp.text)
    #print(tmp_result)
    ids.extend([int(elem[supalen:-1]) for elem in result])

def fetch_urls(urls):
    for url in urls:
        fetch_url(url)
        #print('Fetching ', url)