#!/usr/bin/python3

# ! pip3 install beautifulsoup4
import bs4
# ! pip3 install requests
import requests
# ! pip3 install pandas
from pandas import DataFrame

# standard library modules
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import argparse
import sys
import subprocess
import pickle

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--verbose', '-v', action='store_true')
arg_parser.add_argument('--debug', '-d', action='store_true')
arg_parser.add_argument('--log', nargs='?', type=str, default=None)
arg_parser.add_argument('--min-id', type=int, default=10041337)  # 2017 year
arg_parser.add_argument('--input', '-i', type=argparse.FileType('r'), required=True)
arg_parser.add_argument('--output', '-o', type=argparse.FileType('wb'), required=True)
arg_parser.add_argument('--max-count', '-c', type=int, default=None)
arg_parser.add_argument('--gzip-output', '-z', action='store_true')
arguments = arg_parser.parse_args(sys.argv[1:])

log_config = {
    'format': '%(threadName)s %(name)s %(levelname)s: %(message)s',
    'level': logging.INFO if arguments.verbose else logging.ERROR,
}

if arguments.log:
    log_config['filename'] = arguments.log

logging.basicConfig(**log_config)

errors_count = 0


class MyException(Exception):
    pass


all_product_types = set()


def parse_product(soup, url):
    scripts = soup.findAll('script')
    matching_script = None
    for script in scripts:
        if "allPagesHeader2" in script.text:
            matching_script = script
            break
    if matching_script is None:
        logging.error("%s document has no 'allPagesHeader2' data in <script>", url)
        raise MyException()
    script_parts = re.split(r'DFP.default\(document,', matching_script.text)
    if len(script_parts) < 2:
        logging.error("%s document has no 'DFP.default(document,' data in <script>", url)
        raise MyException()
    script_tail = script_parts[1]
    script_parts = re.split(r'\);', script_tail, maxsplit=1)
    all_pages_header_2_text = script_parts[0]
    try:
        json_data = json.loads(all_pages_header_2_text)
    except Exception as e:
        logging.error("%s document has no JSON data after 'DFP.default(document,' data in <script>", url)
        raise MyException()
    if 'allPagesHeader2' not in json_data:
        logging.error("%s document has no 'allPagesHeader2' in JSON data in <script>", url)
        raise MyException()
    all_pages_header_2 = json_data['allPagesHeader2']
    if 'targets' not in all_pages_header_2:
        logging.error("%s document has no 'targets' in JSON['allPagesHeader2'] data in <script>", url)
        raise MyException()
    targets = all_pages_header_2['targets']
    ans = ()
    for elem in targets:
        if elem['name'] == 'NRproduct':
            if type(elem['value']) == list:
                ans = set(elem['value'])
            else:
                ans = set([elem['value']])
    global all_product_types
    all_product_types |= set(ans)
    return ans


def get_responce_by_id(response_id):
    tag_bank_name = 'header-h2 display-inline margin-right-x-small'
    tag_city = 'response-page__bank-meta font-size-medium color-gray-burn'
    tag_title = 'header-h0 response-page__title'
    tag_fulltext = 'article-text response-page__text markup-inside-small markup-inside-small--bullet'
    tag_num_views = 'icon-font icon-eye-16 icon-font--size_small'
    tag_num_comments = 'link-with-icon__text color-gray-blue--alpha-60'

    # dynamic data extracting
    # 'status': mark_tag[-1].text.strip('\n\t')
    # 'num_views': int(soup.find('span', tag_num_views).next_element)
    # 'num_comments': int(soup.find('span', tag_num_comments).text)

    url = 'https://www.banki.ru/services/responses/bank/response/{}/'.format(response_id)
    # print(url)
    while True:
        try:
            resp = requests.get(url)
            if resp.status_code == 404:
                logging.info('in {} resp.status_code was 404 with {}'.format(datetime.now().time(), response_id))
                return
            if resp.status_code != 200:
                logging.error("Error getting %s: %d", url, resp.status_code)
                return
            soup = bs4.BeautifulSoup(resp.text, 'lxml')
            if soup.find('h1', 'header-h0 margin-bottom-large') is not None:
                logging.info(
                    'in {} header-h0 margin-bottom-large was not None {}'.format(datetime.now().time(), response_id))
                return

            parse_product(soup, url)
        except Exception as e:
            if e is not MyException:  # might be already logged as error
                logging.error('in {} was trouble with get-parse {}: {}'.
                              format(datetime.now().time(), response_id, e))
            if arguments.debug:
                # stop and keep calm coding
                sys.exit(1)
            else:
                # just notify and do as much work as possible
                time.sleep(2)
                continue
        else:
            break

    # (soup.find("h1", "header-h0 margin-bottom-large").text == 'Ошибка')
    mark = 0
    mark_tag = soup.find_all('span', 'text-label')
    if mark_tag[0].next_element == 'Оценка:':
        mark = int(mark_tag[0].next_element.next_element.next_element.text)
    db_elem = {
        'product': parse_product(soup, url),
        'bank_name': soup.find('div', tag_bank_name).text.strip('\n\t'),
        'city': soup.find('div', tag_city).text.split(',')[-1].strip('\n\t'),
        'title': soup.find('h0', tag_title).text.strip('\n\t'),
        'mark': mark,
        'fulltext': soup.find('div', tag_fulltext).text.strip('\n\t'),
        'datetime': soup.find('time').attrs['datetime']
    }
    # print('push', db_elem)
    db[response_id] = db_elem


def update_responce_by_id(response_id):
    url = 'https://www.banki.ru/services/responses/bank/response/{}/'.format(response_id)
    while True:
        try:
            resp = requests.get(url)
        except:
            time.sleep(2)
            continue
        else:
            break
    soup = bs4.BeautifulSoup(resp.text, 'lxml')
    if soup.find("h1", "header-h0 margin-bottom-large") is None:
        db[response_id]['status'] = soup.find_all("span", "text-label")[-1].text.strip('\n\t')
        db[response_id]['num_views'] = int(
            soup.find("span", "icon-font icon-eye-16 icon-font--size_small").next_element)
        db[response_id]['num_comments'] = int(soup.find("span", "link-with-icon__text color-gray-blue--alpha-60").text)
    else:
        return


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
input_file = arguments.input
min_id = arguments.min_id
for line in input_file:
    _id = int(line)
    if _id > min_id:  # меньшие этого id нас не интересуют
        set_ids.add(_id)
new_ids = list(set_ids)

db = {}
if arguments.max_count is not None:
    new_ids = new_ids[:arguments.max_count]

with ThreadPoolExecutor(max_workers=4) as executor:
    for _ in executor.map(fetch_response, new_ids):
        pass


def update_entry_products(entry: dict):
    for prod in all_product_types:
        field_name = "prod_" + prod
        entry[field_name] = 1 if prod in entry["product"] else 0
    del entry["product"]


for entry in db.values():
    update_entry_products(entry)

print(len(db))

df = DataFrame(db)
transposed_df = df.T
pickle.dump(transposed_df, arguments.output, protocol=4)
if arguments.gzip_output:
    subprocess.run(["gzip", "-9", arguments.output.name])
