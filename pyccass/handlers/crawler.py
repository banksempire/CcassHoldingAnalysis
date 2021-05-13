from datetime import datetime

from requests import Session, get
from bs4 import BeautifulSoup as bs

import .crawler_settings as const


def get_stock_list(date: datetime) -> list:
    stock_list_url = f"{const.LIST_URL}{date.strftime('%Y%m%d')}"

    response = get(stock_list_url)

    bsc = bs(response.content)
    stock_list = [
        {
            'code': tr.td.text.rstrip().lstrip(),
            'name': tr.a.text.rstrip().lstrip()
        } for tr in bsc.tbody.find_all('tr')
    ]
    return stock_list
