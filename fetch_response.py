#!/usr/bin/python3

# ! pip3 install beautifulsoup4, lxml
import bs4
# ! pip3 install requests
import requests
# ! pip3 install pandas
from pandas import DataFrame
# ! pip3 install sqlalchemy, psycopg2
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from contextlib import contextmanager

# standard library modules
import logging
import time
from datetime import datetime
import sys

from response import Response
from http_requests import get_from_url

with open('_resp_suffix', 'r') as resp_suffix_file:
    resp_suffix_str = resp_suffix_file.readline()

with open('_site_prefix', 'r') as site_prefix_file:
    site_prefix_str = site_prefix_file.readline()


class MyException(Exception):
    pass


def fetch_response(session, response_id, arguments):
    logging.info(f'in {datetime.now().time()}  start get/upd id {response_id}')
    tag_bank_name = 'header-h2 display-inline margin-right-x-small'
    tag_city = 'response-page__bank-meta font-size-medium color-gray-burn'
    tag_title = 'header-h0 response-page__title'
    tag_fulltext = 'article-text response-page__text markup-inside-small markup-inside-small--bullet'
    tag_num_views = 'icon-font icon-eye-16 icon-font--size_small'
    tag_num_comments = 'link-with-icon__text color-gray-blue--alpha-60'

    url = f'{site_prefix_str}/{resp_suffix_str}/{response_id}/'
    get_from_url(url, arguments.debug)
    while True:
        try:
            resp = requests.get(url)
            if resp.status_code == 404:
                logging.info(f'in {datetime.now().time()} resp.status_code was 404 with {response_id}')
                return
            if resp.status_code != 200:
                logging.error("Error getting %s: %d", url, resp.status_code)
                return
            soup = bs4.BeautifulSoup(resp.text, 'lxml')
            if soup.find('h1', 'header-h0 margin-bottom-large') is not None:
                logging.info(f'in {datetime.now().time()} header-h0 margin-bottom-large was not None {response_id}')
                return

        except Exception as e:
            if e is not MyException:  # might be already logged as error
                logging.error(f'in {datetime.now().time()} was trouble with get {response_id}: {e}')
            if arguments.debug:
                # stop and keep calm coding
                sys.exit(1)
            else:
                # just notify and do as much work as possible
                time.sleep(2)
                continue
        else:
            break

    mark = 0
    mark_tag = soup.find_all('span', 'text-label')
    if mark_tag[0].next_element == 'Оценка:':
        mark = int(mark_tag[0].next_element.next_element.next_element.text)

    __response = session.query(Response).filter_by(response_id=response_id).first()
    try:
        if session.query(Response.bank_name).filter_by(response_id=response_id).scalar() is None:
            setattr(__response, 'last_update', datetime.now())
            setattr(__response, 'bank_name', soup.find('div', tag_bank_name).text.strip('\n\t'))
            setattr(__response, 'city', soup.find('div', tag_city).text.split(',')[-1].strip('\n\t'))
            setattr(__response, 'title', soup.find('h0', tag_title).text.strip('\n\t'))
            setattr(__response, 'mark', mark)
            setattr(__response, 'fulltext', soup.find('div', tag_fulltext).text.strip('\n\t'))
            setattr(__response, 'datetime', soup.find('time').attrs['datetime'])
        # dynamic data extracting
        setattr(__response, 'status', mark_tag[-1].text.strip('\n\t'))
        setattr(__response, 'num_views', int(soup.find('span', tag_num_views).next_element))
        setattr(__response, 'num_comments', int(soup.find('span', tag_num_comments).text))
    except AttributeError:
        logging.error(f'in {datetime.now().time()} was trouble with get-push {response_id}')
    logging.info(f'in {datetime.now().time()}  ended get-upd id {response_id}')