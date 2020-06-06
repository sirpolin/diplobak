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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import argparse
import sys
import subprocess
import pickle

from response import Response

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--verbose', '-v', action='store_true')
arg_parser.add_argument('--debug', '-d', action='store_true')
arg_parser.add_argument('--log', nargs='?', type=str, default=None)
arg_parser.add_argument('--min-id', type=int, default=10041337)  # 2017 year
# arg_parser.add_argument('--output', '-o', type=argparse.FileType('wb'), required=True)
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


with open('_resp_suffix', 'r') as resp_suffix_file:
    resp_suffix_str = resp_suffix_file.readline()

with open('_site_prefix', 'r') as site_prefix_file:
    site_prefix_str = site_prefix_file.readline()

# _db_string pattern:
# postgres://user:password@server:port/table_name
    with open('_db_string', 'r') as db_string_file:
        db_string = db_string_file.readline()


def fetch_response(session, response_id):
    logging.info(f'in {datetime.now().time()}  start get/upd id {response_id}')
    tag_bank_name = 'header-h2 display-inline margin-right-x-small'
    tag_city = 'response-page__bank-meta font-size-medium color-gray-burn'
    tag_title = 'header-h0 response-page__title'
    tag_fulltext = 'article-text response-page__text markup-inside-small markup-inside-small--bullet'
    tag_num_views = 'icon-font icon-eye-16 icon-font--size_small'
    tag_num_comments = 'link-with-icon__text color-gray-blue--alpha-60'

    url = f'{site_prefix_str}/{resp_suffix_str}/{response_id}/'
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


engine = create_engine(db_string, pool_recycle=60)
Main_Session = sessionmaker(bind=engine)
main_session = Main_Session()
new_ids = [result.response_id for result in main_session.query(Response.response_id).filter_by(**{'bank_name': None})]

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def thread_worker(response_id):
    # We're using the session context here.
    with session_scope() as session:
        fetch_response(session, response_id)


start = datetime.now()
with ThreadPoolExecutor(max_workers=8) as executor:
    for _ in executor.map(thread_worker, new_ids):
        pass
end = datetime.now()
print(end-start)

#
# df = DataFrame(db)
# transposed_df = df.T
# pickle.dump(transposed_df, arguments.output, protocol=4)
# if arguments.gzip_output:
#     subprocess.run(["gzip", "-9", arguments.output.name])
