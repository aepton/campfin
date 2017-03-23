import os
import shutil
import zipfile

from fetch import fec as fec_fetch
from fetch import wa as wa_fetch
from settings import settings
from transform import fec as fec_transform
from transform import wa as wa_transform

STATE = 'FEC'
YEARS = ['2018', '2016', '2014', '2012', '2010', '2008', '2006', '2004', '2002', '2000']
DATA_URLS = {}
for YEAR in YEARS:
  CYCLE = YEAR[2:]
  DATA_URLS[YEAR] = [
    ('candidate_committee_link', 'ftp://ftp.fec.gov/FEC/%s/ccl%s.zip' % (YEAR, CYCLE)),
    ('candidate_master', 'ftp://ftp.fec.gov/FEC/%s/cn%s.zip' % (YEAR, CYCLE)),
    ('committee_master', 'ftp://ftp.fec.gov/FEC/%s/cm%s.zip' % (YEAR, CYCLE)),
    ('committee_to_committee', 'ftp://ftp.fec.gov/FEC/%s/oth%s.zip' % (YEAR, CYCLE)),
    ('committee_to_candidate', 'ftp://ftp.fec.gov/FEC/%s/pas2%s.zip' % (YEAR, CYCLE)),
    ('expenditures', 'ftp://ftp.fec.gov/FEC/%s/oppexp%s.zip' % (YEAR, CYCLE)),
    ('contributions', 'ftp://ftp.fec.gov/FEC/%s/indiv%s.zip' % (YEAR, CYCLE))
  ]

def cleanup_data_dirs():
  for state in settings.STATES_IMPLEMENTED:
    try:
      shutil.rmtree(os.path.join(settings.DATA_DIRECTORY, state))
    except:
      continue

def download_and_process_fec_data():
  fec_fetch.download_headers()

  for year in YEARS:
    print 'Year %s' % year
    for url_type, url in DATA_URLS[year]:
      file_path = fec_fetch.download_data(url, url_type, year)
      fec_transform.transform_data(file_path, url_type)
      fec_fetch.cleanup_data(url_type, year)

def download_and_process_wa_data():
  data_file_path = wa_fetch.download_data()
  wa_transform.transform_data(data_file_path)
  os.remove(data_file_path)

def upload_to_socrata():
  pass

def orchestrate():
  cleanup_data_dirs()
  download_and_process_fec_data()
  download_and_process_wa_data()
  upload_to_socrata()

if __name__ == '__main__':
  orchestrate()