import zipfile

from fetcher import *

STATE = 'FEC'
YEARS = ['2018', '2016', '2014', '2012', '2010', '2008', '2006', '2004', '2002', '2000']
DATA_URLS = {}
for YEAR in YEARS:
  CYCLE = YEAR[2:]
  DATA_URLS[YEAR] = {
    'candidate_committee_link': 'ftp://ftp.fec.gov/FEC/%s/ccl%s.zip' % (YEAR, CYCLE),
    'candidate_master': 'ftp://ftp.fec.gov/FEC/%s/cn%s.zip' % (YEAR, CYCLE),
    'committee_master': 'ftp://ftp.fec.gov/FEC/%s/cm%s.zip' % (YEAR, CYCLE),
    'committee_to_committee': 'ftp://ftp.fec.gov/FEC/%s/oth%s.zip' % (YEAR, CYCLE),
    'committee_to_candidate': 'ftp://ftp.fec.gov/FEC/%s/pas2%s.zip' % (YEAR, CYCLE),
    'expenditures': 'ftp://ftp.fec.gov/FEC/%s/oppexp%s.zip' % (YEAR, CYCLE),
    'contributions': 'ftp://ftp.fec.gov/FEC/%s/indiv%s.zip' % (YEAR, CYCLE)
  }

HEADER_URLS = {
  'committee_master_header': 'http://www.fec.gov/finance/disclosure/metadata/cm_header_file.csv',
  'contributions_header': 'http://www.fec.gov/finance/disclosure/metadata/indiv_header_file.csv'
}

def download_data(download_headers=False):
  for year in YEARS:
    print 'Year %s' % year
    for url_type, url in DATA_URLS[year].items():
      print 'Downloading %s' % url
      file_type = 'zip'
      client = Fetcher(
        download_url=url,
        state=STATE,
        data_type=url_type,
        file_type=file_type)
      client.download_data_pycurl()

      zip_ref = zipfile.ZipFile(client.file_path, 'r')
      zip_dir = client.file_path[:-1 * len('.%s' % file_type)]
      zip_ref.extractall(zip_dir)
      zip_ref.close()

      data_dir = os.path.dirname(client.file_path)
      latest_path = os.path.join(data_dir, 'latest_%s_%s' % (url_type, year))

      print 'Pointing %s to %s' % (latest_path, zip_dir)

      try:
        os.remove(latest_path)
      except:
        pass

      try:
        os.symlink(zip_dir, latest_path)
      except Exception, e:
        print 'Error pointing symlink %s to %s: %s' % (latest_path, zip_dir, e)

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