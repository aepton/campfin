from fetcher import *

STATE = 'WA'
URLS = {
  'contributions': 'https://data.wa.gov/api/views/kv7h-kjye/rows.csv?accessType=DOWNLOAD'
}

# URLS['contributions'] = 'https://data.wa.gov/api/views/xhn7-64im/rows.csv?accessType=DOWNLOAD'

def download_data():
  for url in URLS:
    print 'Downloading %s' % url
    client = Fetcher(
      download_url=URLS[url],
      state=STATE,
      data_type=url)
    client.download_data_by_line()

if __name__ == '__main__':
  download_data()