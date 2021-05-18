from datetime import datetime, timedelta
from typing import Callable
from queue import Queue
from threading import Thread
from time import sleep

from pyccass.handlers.crawler import initialize_crawler, get_stock_list
from pyccass.handlers.database import DBHandler


def trafic_controler(permission: Queue, sleep_time: float):
    while True:
        permission.put(None)
        sleep(sleep_time)


def commander(date: datetime, out_q: Queue):
    while date < datetime.now():
        stocks = get_stock_list(date)
        for stock in stocks:
            out_q.put((date, stock))
        date += timedelta(1)
    out_q.put(None)
    print('Commander Out.')


def download_data(func: Callable, in_q: Queue, out_q: Queue, permission: Queue):
    while True:
        command = in_q.get()
        if command is None:
            in_q.put(None)
            break
        permission.get()
        product = func(*command)
        out_q.put(product)


def save2db(func: Callable, in_q: Queue):
    while True:
        command = in_q.get()
        if command is None:
            in_q.put(None)
            break
        func(command)


def async_update(sleep_time: float):
    db = DBHandler('database.db')
    date = db.query_max_date()
    date = datetime.now() - timedelta(365) if date is None else date

    requests_queue: Queue = Queue(20)
    responses_queue: Queue = Queue(10)
    permission: Queue = Queue(1)

    Thread(target=trafic_controler,
           args=(permission, sleep_time,),
           daemon=True).start()

    # Fire up crawlers threads
    get_data = initialize_crawler()
    crawlers = list()
    for _ in range(10):
        t = Thread(target=download_data,
                   args=(get_data,
                         requests_queue,
                         responses_queue,
                         permission)
                   )
        t.start()
        crawlers.append(t)

    # Fire up save to db threads
    insert_many = db.insert_many
    savers = list()
    for _ in range(2):
        t = Thread(target=save2db,
                   args=(insert_many,
                         responses_queue))
        t.start()
        savers.append(t)

    # Run main thread loop
    commander(datetime(2021, 5, 12), requests_queue)
    # commander(date, requests_queue)

    # Join all crawlers and send stop signal to consumers
    [t.join() for t in crawlers]
    responses_queue.put(None)

    # Join all savers
    [t.join() for t in savers]
