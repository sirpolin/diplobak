#!/usr/bin/python3

# ! pip3 install requests
import requests

# standard library modules
from datetime import datetime
import logging
import sys
import time


class MyException(Exception):
    pass


def get_from_url(url, is_debug):
    while True:
        try:
            resp = requests.get(url)
            if resp.status_code == 404:
                logging.info(f'in {datetime.now().time()} resp.status_code was 404 with {url}')
                return
            if resp.status_code != 200:
                logging.error("Error getting %s: %d", url, resp.status_code)
                return

        except Exception as e:
            if e is not MyException:  # might be already logged as error
                logging.error(f'in {datetime.now().time()} was trouble with get {url}: {e}')
            if is_debug:
                # stop and keep calm coding
                sys.exit(1)
            else:
                # just notify and do as much work as possible
                time.sleep(2)
                continue
        else:
            break
    return resp.text

