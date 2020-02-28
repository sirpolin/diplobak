import bs4
import json
import logging
import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pandas import DataFrame as df

logging.basicConfig(
    filename='2020-02-28_140k.log',
    format = '%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO)

class MyException(Exception):
    pass

def parse_product(soup, url):
    try:        
        tmp_list = re.split('DFP.default\(document,', soup.findAll('script')[-6].text)
        '''
        f = open('soup.txt', 'a')
        for elem in tmp_list:
            f.write(elem)
        f.write(url)
        f.close()
        '''
        tmp_json = json.loads(re.split('\);', tmp_list[1], maxsplit=1)[0])['allPagesHeader2']['targets']
        ''' f2 = open('json.txt', 'a')
        json.dump(tmp_json, f2)
        f2.write(url)
        f2.close()
        '''
    except:
        logging.info('in {} was trouble with parse_product {}'.format(datetime.now().time(), url))
        raise MyException(Exception)
    else:
        ans = ()
        for elem in tmp_json:
            if elem['name'] == 'NRproduct':
                if type(elem['value']) == list:
                    ans = tuple(elem['value'])
                else:
                    ans = (elem['value'],)
        return ans

def get_responce_by_id(response_id):
    tag_bank_name = 'header-h2 display-inline margin-right-x-small'
    tag_city = 'response-page__bank-meta font-size-medium color-gray-burn'
    tag_title = 'header-h0 response-page__title'
    tag_fulltext = 'article-text response-page__text markup-inside-small markup-inside-small--bullet'
    tag_num_views = 'icon-font icon-eye-16 icon-font--size_small'
    tag_num_comments = 'link-with-icon__text color-gray-blue--alpha-60'
    
    # dynamic data extracting
    #'status': mark_tag[-1].text.strip('\n\t')
    #'num_views': int(soup.find('span', tag_num_views).next_element)
    #'num_comments': int(soup.find('span', tag_num_comments).text)
    
    url = 'https://www.banki.ru/services/responses/bank/response/{}/'.format(response_id)
    #print(url)
    while(1):
        try:
            resp = requests.get(url)
            if (resp.status_code == 404):
                logging.info('in {} resp.status_code was 404 with {}'.format(datetime.now().time(), response_id))
                return 
            
            soup = bs4.BeautifulSoup(resp.text, 'lxml')
            if (soup.find('h1', 'header-h0 margin-bottom-large') is not None):
                logging.info('in {} header-h0 margin-bottom-large was not None {}'.format(datetime.now().time(), response_id))
                return
            
            parse_product(soup,url)            
        except:
            logging.info('in {} was trouble with get-parse {}'.format(datetime.now().time(), response_id))
            time.sleep(2)
            continue
        else:
            break

    # (soup.find("h1", "header-h0 margin-bottom-large").text == 'Ошибка')
    mark = 0
    mark_tag = soup.find_all('span', 'text-label')
    if (mark_tag[0].next_element == 'Оценка:'):
        mark = int(mark_tag[0].next_element.next_element.next_element.text)
    db_elem = {
        'product': parse_product(soup,url), \
        'bank_name': soup.find('div', tag_bank_name).text.strip('\n\t'), \
        'city': soup.find('div', tag_city).text.split(',')[-1].strip('\n\t'), \
        'title': soup.find('h0', tag_title).text.strip('\n\t'), \
        'mark': mark, \
        'fulltext': soup.find('div', tag_fulltext).text.strip('\n\t'), \
        'datetime': soup.find('time').attrs['datetime']}
    #print('push', db_elem)
    db[response_id] = db_elem
        
def update_responce_by_id(response_id):
    url = 'https://www.banki.ru/services/responses/bank/response/{}/'.format(response_id)
    while(1):
        try:
            resp = requests.get(url)
        except:
            time.sleep(2)
            continue
        else:
            break
    soup = bs4.BeautifulSoup(resp.text, 'lxml')    
    if (soup.find("h1", "header-h0 margin-bottom-large") is None):
        db[response_id]['status'] = soup.find_all("span", "text-label")[-1].text.strip('\n\t')
        db[response_id]['num_views'] = int(soup.find("span", "icon-font icon-eye-16 icon-font--size_small").next_element)
        db[response_id]['num_comments'] = int(soup.find("span", "link-with-icon__text color-gray-blue--alpha-60").text)
    else:
        return
        #may be remove id from db?
        #ask Albina
        

def fetch_response(response_id):
    if response_id in db:
        logging.info('in {} start upd id {}'.format(datetime.now().time(), response_id))
        update_responce_by_id(response_id)
        logging.info('in {} ended upd id {}'.format(datetime.now().time(), response_id))
    else:
        logging.info('in {} start get id {}'.format(datetime.now().time(), response_id))
        get_responce_by_id(response_id)
        logging.info('in {} ended get id {}'.format(datetime.now().time(), response_id))    
        
set_ids = set()
f = open("after2017_try4.txt", "r")
for line in f:
    _id = int(line)
    if _id > 10041337: # меньшие этого id нас не интересуют 
        set_ids.add(_id)
new_ids = list(set_ids)

db = {}
with ThreadPoolExecutor(max_workers=4) as executor:
    for _ in executor.map(fetch_response, new_ids[:140000]):
        pass
        
print(len(db))
with open ('from_dict_after2017_try4_first140k_ver4.pkl', 'wb') as handle:
    pickle.dump(db, handle, protocol=4)
