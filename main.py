from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve
from urllib.error import URLError
import pandas as pd
import os
import time
from queue import Queue
from threading import Thread
import logging


def download_link(directory, link):
    start = time.time()
    logging.info('Downloading {}'.format(link))
    while True:
        try:
            file = urlretrieve(directory + link)
            with open(file[0]) as data:
                counter = 0
                unit_row, loc_row, meas_row = 0, 0, 0
                for line in data:
                    if 'UNITS' in line:
                        unit_row = counter
                    if 'LOCATIONS' in line:
                        loc_row = counter
                    if 'MEASUREMENTS' in line:
                        meas_row = counter
                    counter += 1
                data.seek(0)
                units = pd.read_csv(data, sep=';', skiprows=[unit_row] + [x for x in range(loc_row, counter)])
                data.seek(0)
                locations = pd.read_csv(data, sep=';',
                                        skiprows=[x for x in range(loc_row + 1)] + [x for x in
                                                                                    range(meas_row, counter)])
                data.seek(0)
                measurements = pd.read_csv(data, sep=';', skiprows=[x for x in range(meas_row + 1)])
            os.remove(file[0])
            break
        except URLError as e:
            logging.debug('Error while downloading {}'.format(link))
            print('Error while downloading {}'.format(link))
            time.sleep(1)

    logging.info('{} downloaded in {:1.2f} seconds'.format(link, time.time() - start))
    return units, locations, measurements


class FileHandlerWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            directory, link = self.queue.get()
            download_link(directory, link)
            self.queue.task_done()


def main(worker_num):
    logging.basicConfig(filename='log.log', filemode='w', level=logging.DEBUG)
    start = time.time()
    url = 'http://data.aireas.com/csv/'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html5lib')
    table = soup.find('table')
    content = table.find_all('tr')
    folders = list()
    files = list()

    # Create list of files
    logging.info('Getting list of files to download')
    for item in content:
        try:
            if 'DIR' in item.find('img').get('alt'):
                folders.append(item.find('a').get('href'))
            elif 'TXT' in item.find('img').get('alt'):
                files.append(item.find('a').get('href'))
        except AttributeError:
            pass

    queue = Queue()
    # Create workers
    for x in range(worker_num):
        worker = FileHandlerWorker(queue)
        worker.daemon = True
        worker.start()
    # Fill queue with files to parse:
    for file in files:
        logging.info('Queueing {}'.format(file))
        queue.put((url, file))
    queue.join()
    execution_time = time.time() - start
    logging.info('Total execution time for {} files: {} seconds'.format(len(files), execution_time))
    return execution_time


if __name__ == "__main__":
    for x in range(8,12):
        print("workers: {}, time: {} seconds".format(x,main(x)))
