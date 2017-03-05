import zipfile

from fetcher import *

STATE = 'FEC'
DATA_URLS = {
  'candidate_committee_link': 'ftp://ftp.fec.gov/FEC/2016/ccl16.zip',
  'candidate_master': 'ftp://ftp.fec.gov/FEC/2016/cn16.zip',
  'committee_master': 'ftp://ftp.fec.gov/FEC/2016/cm16.zip',
  'committee_to_committee': 'ftp://ftp.fec.gov/FEC/2016/oth16.zip',
  'committee_to_candidate': 'ftp://ftp.fec.gov/FEC/2016/pas216.zip',
  'expenditures': 'ftp://ftp.fec.gov/FEC/2016/oppexp16.zip',
  'contributions': 'ftp://ftp.fec.gov/FEC/2016/indiv16.zip'
}

HEADER_URLS = {
  'committee_master_header': 'http://www.fec.gov/finance/disclosure/metadata/cm_header_file.csv',
  'contributions_header': 'http://www.fec.gov/finance/disclosure/metadata/indiv_header_file.csv'
}

def download_data(download_headers=False):
  for url in DATA_URLS:
    print 'Downloading %s' % url
    file_type = 'zip'
    client = Fetcher(
      download_url=DATA_URLS[url],
      state=STATE,
      data_type=url,
      file_type=file_type)
    client.download_data_ftp()

    zip_ref = zipfile.ZipFile(client.file_path, 'r')
    zip_dir = client.file_path[:-1 * len('.%s' % file_type)]
    zip_ref.extractall(zip_dir)
    zip_ref.close()

  if download_headers:
    for url in HEADER_URLS:
      print 'Downloading %s' % url
      client = Fetcher(
        download_url=HEADER_URLS[url],
        state=STATE,
        data_type=url,
        file_type='csv')
      client.download_data_by_line()

if __name__ == '__main__':
  download_data(download_headers=True)