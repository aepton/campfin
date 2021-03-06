import logging

from fetcher import *

STATE = 'WA'
DATA_URLS = {
  'contributions': 'https://data.wa.gov/api/views/kv7h-kjye/rows.csv?accessType=DOWNLOAD'
}

logger = logging.getLogger(__name__)

def download_data(download_headers=False):
  for url in DATA_URLS:
    logger.info('Downloading %s' % url)
    client = Fetcher(
      download_url=DATA_URLS[url],
      state=STATE,
      data_type=url,
      file_type='csv')
    client.download_data_by_line()
    return client.file_path

if __name__ == '__main__':
  download_data()