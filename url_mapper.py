"""
map file name to url
"""

URL_MAP = {
    'videocard':      'https://catalog.api.onliner.by/search/videocard',
    'cpu':            'https://catalog.api.onliner.by/search/cpu',
    'motherboard':    'https://catalog.api.onliner.by/search/motherboard',
    'dram':           'https://catalog.api.onliner.by/search/dram',
    'fan':            'https://catalog.api.onliner.by/search/fan',
    'ssd':            'https://catalog.api.onliner.by/search/ssd',
    'hdd':            'https://catalog.api.onliner.by/search/hdd',
    'chassis':        'https://catalog.api.onliner.by/search/chassis',
    'powersupply':    'https://catalog.api.onliner.by/search/powersupply',
    'soundcard':      'https://catalog.api.onliner.by/search/soundcard',
    'networkadapter': 'https://catalog.api.onliner.by/search/networkadapter',
    'optical':        'https://catalog.api.onliner.by/search/optical',
    'tvtuner':        'https://catalog.api.onliner.by/search/tvtuner'
}

def getUrlByFileName(fileName):
    try:
        for key in URL_MAP.keys():
            if key in fileName:
                return URL_MAP[key]
        raise ValueError('url value was not found in keys: ' + str(fileName))
    except Exception:
        raise ValueError('url value was not found: ' + str(fileName))
