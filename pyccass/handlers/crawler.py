from datetime import datetime
from typing import List, Tuple, Callable

from requests import Session, get
from bs4 import BeautifulSoup as bs

from pyccass.utils.proxies import get_proxies

LIST_URL = "https://www.hkexnews.hk/sdw/search/stocklist_c.aspx?sortby=stockcode&shareholdingdate="

DATA_URL = 'https://www.hkexnews.hk/sdw/search/searchsdw_c.aspx'

HEADER = {
    'dnt': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36 Edg/90.0.818.46',
    'origin': 'https://www.hkexnews.hk',
    'referer': 'https://www.hkexnews.hk/sdw/search/searchsdw_c.aspx',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="90", "Microsoft Edge";v="90"',
    'sec-ch-ua-mobile': '?0',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
}


def get_stock_list(date: datetime) -> List[Tuple[str, str]]:
    stock_list_url = f"{LIST_URL}{date.strftime('%Y%m%d')}"

    response = get(stock_list_url, proxies=get_proxies())

    bsc = bs(response.content, features="html.parser")
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
    session.headers.update(HEADER)

    url = DATA_URL

    # Get VIEWSTATE from website
    response = session.post(url, proxies=get_proxies())
    bsc = bs(response.content, features="html.parser")

    __VIEWSTATEGENERATOR = bsc.find(
        'input', id='__VIEWSTATEGENERATOR').get('value')
    __VIEWSTATE = bsc.find(
        'input', id='__VIEWSTATE').get('value')

    def _parse_row(tr, date: datetime, stockcode: str):
        row = tr.find_all('td')
        try:
            holding_pct = float(row[4].find(
                'div', class_="mobile-list-body").text.replace('%', ''))
        except IndexError:
            holding_pct = 0.0

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
            holding_pct,
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
        response = session.post(url, data=payload, proxies=get_proxies())
        assert response.status_code == 200
        bsc = bs(response.content, features="html.parser")
        # Parse response
        tbody = bsc.tbody
        if tbody is None:
            table = []
        else:
            tr = tbody.find_all('tr')[0]
            table = [_parse_row(tr, qdate, code)
                     for tr in tbody.find_all('tr')]
        return table

    return get_data
