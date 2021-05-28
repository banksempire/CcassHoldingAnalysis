from datetime import datetime, timedelta
from typing import Callable

import requests

from pyccass.handlers.crawler import initialize_crawler, get_stock_list
from pyccass.handlers.database import DBHandler

from pyccass.utils.proxies import get_proxies
from requests import Session, get

from queue import Queue

from pyccass.tasks.update import async_update


def main():
    async_update(0.1)


if __name__ == '__main__':
    main()
