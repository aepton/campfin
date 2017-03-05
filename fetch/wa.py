from fetcher import *

STATE = 'WA'
DATA_URLS = {
  'contributions': 'https://data.wa.gov/api/views/kv7h-kjye/rows.csv?accessType=DOWNLOAD'
}

DATA_URLS['contributions'] = 'https://data.wa.gov/api/views/xhn7-64im/rows.csv?accessType=DOWNLOAD'

def download_data(download_headers=False):
  for url in DATA_URLS:
    print 'Downloading %s' % url
    client = Fetcher(
      download_url=DATA_URLS[url],
      state=STATE,
      data_type=url,
      file_type='csv')
    client.download_data_by_line()

if __name__ == '__main__':
  download_data()