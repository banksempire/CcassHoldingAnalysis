from datetime import datetime
from typing import List, Tuple, Callable
from urllib.request import getproxies

from requests import Session, get
from bs4 import BeautifulSoup as bs

import pyccass.handlers.crawler_settings as consts


def get_stock_list(date: datetime) -> List[Tuple[str, str]]:
    stock_list_url = f"{consts.LIST_URL}{date.strftime('%Y%m%d')}"

    response = get(stock_list_url, proxies=getproxies())

    bsc = bs(response.content)
    stock_list = [
        (
            tr.td.text.rstrip().lstrip(),  # code
            tr.a.text.rstrip().lstrip()  # name
        ) for tr in bsc.tbody.find_all('tr')
    ]
    return stock_list


def initialize_crawler() -> Callable[[datetime, Tuple[str, str]], List[Tuple]]:
    '''
    initialize crawler
    '''

    # Initialize session
    session = Session()
    session.headers.update(consts.HEADER)
    session.proxies.update(getproxies())

    url = consts.DATA_URL

    # Get VIEWSTATE from website
    response = session.post(url)
    bsc = bs(response.content, features="html.parser")

    __VIEWSTATEGENERATOR = bsc.find(
        'input', id='__VIEWSTATEGENERATOR').get('value')
    __VIEWSTATE = bsc.find(
        'input', id='__VIEWSTATE').get('value')

    def _parse_row(tr, date: datetime, stockcode: str):
        row = tr.find_all('td')
        content = (
            int(date.timestamp()),
            stockcode,
            # custodian_code
            row[0].find('div', class_="mobile-list-body").text,
            # custodian
            row[1].find('div', class_="mobile-list-body").text,
            # holding
            int(row[3].find('div', class_="mobile-list-body").text.replace(',', '')),
            # holding_pct
            float(row[4].find(
                'div', class_="mobile-list-body").text.replace('%', '')),
        )
        return content

    def get_data(qdate: datetime, stock: Tuple[str, str]) -> List[Tuple]:
        # Generate payload
        code, name = stock

        payload = {
            '__EVENTTARGET': 'btnSearch',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': __VIEWSTATE,
            '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
            'today': f"{datetime.now().strftime('%Y%m%d')}",
            'sortBy': 'shareholding',
            'sortDirection': 'desc',
            'txtShareholdingDate': f"{qdate.strftime('%Y/%m/%d')}",
            'txtStockCode': code,
            'txtStockName': name,
        }

        # Get response
        response = session.post(url, data=payload)
        bsc = bs(response.content, features="html.parser")
        # Parse response
        tbody = bsc.tbody
        tr = tbody.find_all('tr')[0]
        table = [_parse_row(tr, qdate, code) for tr in tbody.find_all('tr')]
        return table

    return get_data
