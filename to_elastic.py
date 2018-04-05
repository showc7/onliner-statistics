# export ES_HEAP_SIZE=1g

from __future__ import print_function
import os
import json
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import zipfile

import url_mapper as UrlMapper

class Elastic:
    """ helper to work with elasticsearch """

    DEFAULT_TIMEOUT = 120

    def __init__(self):
        """ create elsaticsearch driver with predefined connection timeout """
        self.es = Elasticsearch(timeout=Elastic.DEFAULT_TIMEOUT)

    def save(self, index, document):
        """ save single log event """
        self.es.index(index=index, doc_type='logevent', body=document)

    def save_bulk(self, documents):
        """ save bulk documents """
        helpers.bulk(self.es, documents)

class Storage:
    """ source data storage """

    def __init__(self, rootDirectory):
        """ set root directory """
        self.rootDirectory = rootDirectory

    def load_each_file_data(self):
        """ walk threw all files and load each file content """
        for root, _, files in os.walk(self.rootDirectory):
            for file in files:
                file = os.path.join(root, file)
                directory_name = os.path.split(root)[1]
                with open(file, 'r') as open_file:
                    yield {
                        "fileData": open_file.read(),
                        "fileName": file,
                        "directoryName": directory_name
                    }

    @staticmethod
    def is_file(name):
        """ check wether file path is file or folder """
        return not name.endswith('/')

    @staticmethod
    def load_from_zip(rootFile):
        """ load eath file data from zip file """
        print('rootfile'+str(rootFile))
        zf = zipfile.ZipFile(rootFile)
        for file_info in zf.infolist():
            if not Storage.is_file(file_info.filename):
                continue
            #print(file_info.filename)
            file_data = zf.read(file_info.filename)
            file_name = file_info.filename
            directory_name = os.path.split(file_name)[0]
            directory_name = os.path.split(directory_name)[1]
            #print(directory_name)
            yield {
                "fileData": file_data,
                "fileName": file_name,
                "directoryName": directory_name
            }

class Transformer:
    """ Transforms helper """
    def __init__(self):
        pass

    @staticmethod
    def assemble(record):
        """ assemble data record to normal appearence, transform name and time"""
        file_content = record["fileData"]
        file_name = record["fileName"]
        directory_name = record["directoryName"]

        data = json.loads(file_content)
        url = UrlMapper.getUrlByFileName(file_name)
        datetime_object = datetime.strptime(directory_name, '%m%d%Y_%H.%M.%S')

        return {'data': data, 'url': url, 'time': datetime_object}

    @staticmethod
    def to_separate_items(record):
        """ splits record on seprate items """
        data = record['data']
        url = record['url']
        record_time = record['time']

        for data_item in data:
            yield {'data': data_item, 'url': url, 'time': record_time}

    @staticmethod
    def transfrom_types(record):
        """ transform data types """
        data = record["data"]
        #print(json.dumps(data, sort_keys=True, indent=2))
        Transformer.transform_dict_path(data, ['prices', 'price_min', 'amount'], float)
        Transformer.transform_dict_path(data, ['prices', 'price_max', 'amount'], float)
        Transformer.transform_dict_path(data, ['prices', 'price_min', 'converted', 'BYN', 'amount'], float)
        Transformer.transform_dict_path(data, ['prices', 'price_max', 'converted', 'BYN', 'amount'], float)
        Transformer.transform_dict_path(data, ['prices', 'price_min', 'converted', 'BYR', 'amount'], float)
        Transformer.transform_dict_path(data, ['prices', 'price_max', 'converted', 'BYR', 'amount'], float)

    @staticmethod
    def transform_time(record):
        """ transform time to elastic format """
        record_time = record["time"]
        record_time = record_time.strftime('%Y-%m-%dT%H:%M:%S.0Z')

    @staticmethod
    def transform_to_elastic_document(record):
        """ dmake end elastic document """
        time = record['time']
        index = 'onliner-'+time.strftime('%Y-%m-%d')

        document = {
            '_index': index,
            '_type': 'logevent',
            'TimeStamp': record["time"],
            'LoggerName': 'onliner.data',
            'Message': record["data"]
        }

        return index, document

    @staticmethod
    def transform_dict_path(data, path, func):
        """ transforms specified dictionary path using func """
        DEBUG = False

        if len(path) is 0:
            return

        ref = data

        for index in path:
            if DEBUG:
                print({'index': index})
            if (data is not None) and (index in data.keys()):
                ref = data
                data = data[index]
            else:
                if DEBUG:
                    print({'no index': index})
                return

        if type(data) is str:
            ref[index] = func(data)
            if DEBUG:
                print(ref[index])
        else:
            if DEBUG:
                print({'not a string': index})

def main():
    """ entrypoint """
    print('started ...')
    #storage = Storage('/home/show/bigData/raw_data')
    storage = Storage('d:\\Dropbox\\магистратура\\bigData\\raw_data\\')
    transformer = Transformer()
    elastic = Elastic()

    #for x in storage.loadEachFileData():
    for x in storage.load_from_zip('d:\\Dropbox\\магистратура\\bigData\\onliner.zip'):
        print(x['fileName'])
        #print(x['fileData'])
        processed = transformer.assemble(x)
        documents = []
        for item in transformer.to_separate_items(processed):
            #print(item['data'])
            transformer.transfrom_types(item)
            transformer.transform_time(item)
            index, document = transformer.transform_to_elastic_document(item)
            documents.append(document)
        #print('saving')
        elastic.save_bulk(documents)
    '''
    storage = Storage('d:\\Dropbox\\магистратура\\bigData\\onliner.zip')
    for x in storage.loadFromZip('d:\\Dropbox\\магистратура\\bigData\\onliner.zip'):
        pass
    '''

if __name__ == '__main__':
    main()

# find . -type f -name 'currency' -delete
# activate py36
# python separated_vsersion\to_elastic.py