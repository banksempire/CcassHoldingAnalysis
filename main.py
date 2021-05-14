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
    # get_data = initialize_crawler()
    # get_data(datetime(2020, 4, 27), ('01413', '廣聯工程控股'))

    # # get('https://www.baidu.com', proxies=get_proxies())
    async_update()


if __name__ == '__main__':
    main()
