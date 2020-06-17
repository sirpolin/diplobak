#!/usr/bin/python3

# ! pip3 install beautifulsoup4, lxml
import bs4
# ! pip3 install sqlalchemy, psycopg2
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func

# standard library modules
import re
import math
from datetime import datetime
import logging
import argparse
import sys

from response import Response

import http
from http import get_from_url

with open('_product_names', 'r') as product_names_file:
    product_names = [line.rstrip('\n') for line in product_names_file]


# _db_string pattern:
# postgres://user:password@server:port/table_name
with open('_db_string', 'r') as db_string_file:
    db_string = db_string_file.readline()

engine = create_engine(db_string, pool_recycle=60)
Session = sessionmaker(bind=engine)
session = Session()


def fetch_product(product_name, last_id, arguments):
    with open('_resp_suffix', 'r') as resp_suffix_file:
        resp_suffix_str = resp_suffix_file.readline()

    resp_pattern_href = f'<a href="/{resp_suffix_str}/'
    resp_pattern_regexp = re.compile(fr'{resp_pattern_href}\d+/')
    pattern_prefix_len = len(resp_pattern_href)

    with open('_site_prefix', 'r') as site_prefix_file:
        site_prefix_str = site_prefix_file.readline()

    with open('_product_suffix', 'r') as product_suffix_file:
        product_suffix_str = product_suffix_file.readline()

    url = f'{site_prefix_str}/{product_suffix_str}/{product_name}/'

    soup = bs4.BeautifulSoup(http(url, arguments.debug), 'lxml')

    count_responses = int(soup.find('div', 'margin-top-default').attrs['data-options'].split(';')[2].split(':')[1])
    print('responses=', count_responses)
    total_pages = math.ceil(int(count_responses) / 25)
    print('pages=', total_pages)

    for page in range(total_pages):
        tmp_url = f'{site_prefix_str}/{product_suffix_str}/{product_name}/?page={page + 1}'
        tmp_resp = http(tmp_url, arguments.debug)
        tmp_result = resp_pattern_regexp.findall(tmp_resp)
        # removing duplicates
        del tmp_result[::2]
        for elem in tmp_result:
            tmp_id = int(elem[pattern_prefix_len:-1])
            if tmp_id < last_id:
                continue
            __response = session.query(Response).filter_by(response_id=tmp_id).first()
            try:
                # update dynamic response fields
                setattr(__response, product_name, True)
            except AttributeError:
                # getting all available response data
                session.add(Response(tmp_id, product_name))

            session.commit()


def fetch_products(arguments):
    for curr_product_name in product_names:
        # get last stored id from database
        max_curr_id = session.query(func.max(Response.response_id)).filter_by(**{curr_product_name: True}).scalar()
        if max_curr_id is None:
            max_curr_id = arguments.min_id
            # default value for 2017 year
        fetch_product(curr_product_name, max_curr_id)


