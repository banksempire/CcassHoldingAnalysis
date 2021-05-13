from requests import Session, get
from bs4 import BeautifulSoup as bs


def get_stock_list():
    stock_list_url = f"https://www.hkexnews.hk/sdw/search/stocklist_c.aspx?sortby=stockcode&shareholdingdate={(datetime.now() - timedelta(1)).strftime('%Y%m%d')}"

    response = get(stock_list_url)

    bsc = bs(response.content)
    stock_list = [
        {
            'code': tr.td.text.rstrip().lstrip(),
            'name': tr.a.text.rstrip().lstrip()
        } for tr in bsc.tbody.find_all('tr')
    ]
    return stock_list
