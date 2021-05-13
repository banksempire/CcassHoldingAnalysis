from datetime import datetime

from pyccass.handlers.crawler import initialize_crawler

if __name__ == '__main__':
    get_data = initialize_crawler()
    data = get_data(datetime(2021, 5, 2), ('90519', '貴州茅台'))
    print(data)
