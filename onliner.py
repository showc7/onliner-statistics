"""
Loads data from onliner
"""

import datetime
import os
import time
import json
import requests
from slack_logger import Logger

# https://www.onliner.by/sdapi/kurs/api/bestrate?currency=USD&type=nbrb

BASE_PATH = r'G:\onliner\\'
CURRENT_DIR = BASE_PATH + datetime.datetime.now().strftime('%m%d%Y_%H.%M.%S')

if __name__ == '__main__':
    logger = Logger()
    logger.infoNoError('started');
    BASE_URLS = [
        'https://catalog.api.onliner.by/search/videocard',
        'https://catalog.api.onliner.by/search/cpu',
        'https://catalog.api.onliner.by/search/motherboard',

        'https://catalog.api.onliner.by/search/dram',
        'https://catalog.api.onliner.by/search/fan',
        'https://catalog.api.onliner.by/search/ssd',

        'https://catalog.api.onliner.by/search/hdd',
        'https://catalog.api.onliner.by/search/chassis',
        'https://catalog.api.onliner.by/search/powersupply',

        'https://catalog.api.onliner.by/search/soundcard',
        'https://catalog.api.onliner.by/search/networkadapter',
        'https://catalog.api.onliner.by/search/optical',

        'https://catalog.api.onliner.by/search/tvtuner'
    ]

    os.makedirs(CURRENT_DIR)
    RATE_URL = 'https://www.onliner.by/sdapi/kurs/api/bestrate?currency=USD&type=nbrb'
    RATE = requests.get(RATE_URL).json()
    with open(os.path.join(CURRENT_DIR, 'currency'), 'w') as rateFile:
        rateFile.write(json.dumps(RATE))

    for url in BASE_URLS:
        filePath = os.path.join(CURRENT_DIR, url.replace("/", "").replace(":", ""))
        print(filePath)
        print(url)
        logger.infoNoError(url)
        response = requests.get(url)
        #print(response.json())
        pagesInfo = response.json()['page']
        print(pagesInfo)
        pagesCount = pagesInfo['last']

        products = []

        for pageNumber in range(1, pagesCount+1):
            print(str(pageNumber) + ' ', end='')
            params = {'page': str(pageNumber)}
            response = requests.get(url, params)
            jsonResponse = response.json()
            responseProducts = jsonResponse['products']
            productsCount = len(responseProducts)

            for productIndex in range(0, productsCount):
                products.append(responseProducts[productIndex])
            #print(len(products))
            time.sleep(1)

        print()
        print(url + ' total: ' + str(len(products)))
        with open(filePath, 'w') as outputFile:
            outputFile.write(json.dumps(products))

    logger.infoNoError('finished')
