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
from fetch_response import fetch_response

# standard library modules
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import argparse
import sys


from response import Response
from fetch_urls import fetch_product, fetch_products

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


# _db_string pattern:
# postgres://user:password@server:port/table_name
with open('_db_string', 'r') as db_string_file:
    db_string = db_string_file.readline()

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
        fetch_response(session, response_id, arguments)


# for example, getting of leasing

engine = create_engine(db_string, pool_recycle=60)
Main_Session = sessionmaker(bind=engine)
main_session = Main_Session()
new_ids = [result.response_id for result in main_session.query(Response.response_id).filter_by(**{'bank_name': None})]

min_id = arguments.min_id
start = datetime.now()
#fetch_products(arguments)
fetch_product('leasing', 370938, arguments)
end = datetime.now()
print(end-start)


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
