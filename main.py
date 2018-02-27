from bs4 import BeautifulSoup
import requests
from urllib.request import urlretrieve
import pandas as pd
import os
import time

start = time.time()
url = 'http://data.aireas.com/csv/'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html5lib')

table = soup.find('table')

content = table.find_all('tr')

folders = list()
files = list()

print('Getting files info')
for item in content:
    try:
        if 'DIR' in item.find('img').get('alt'):
            folders.append(item.find('a').get('href'))
        elif 'TXT' in item.find('img').get('alt'):
            files.append(item.find('a').get('href'))
    except AttributeError:
        pass

for item in files:
    file_start = time.time()
    print('Parsing file: {}'.format(item), end='\t')
    file = urlretrieve(url+item)

    with open(file[0]) as data:
        counter = 0
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
        locations = pd.read_csv(data, sep=';', skiprows=[x for x in range(loc_row + 1)] + [x for x in range(meas_row, counter)])
        data.seek(0)
        measurements = pd.read_csv(data, sep=';', skiprows=[x for x in range(meas_row + 1)])
    os.remove(file[0])
    print('{:1.2f} seconds'.format(time.time() - file_start))

print('Total execution time for {} files: {} seconds'.format(len(files), time.time() - start))



