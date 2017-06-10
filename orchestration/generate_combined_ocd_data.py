import logging
import os

from settings import settings

# Setting this up before other imports so everything logs correctly
logging.basicConfig(
  filename=os.path.join(settings.LOG_DIR, 'rebuild.log'),
  filemode='a',
  level=logging.INFO)

import shutil
import subprocess
import zipfile

from datetime import datetime
from fetch import fec as fec_fetch
from fetch import wa as wa_fetch
from transform import fec as fec_transform
from transform import wa as wa_transform
from utilities import utils

YEARS = ['2018', '2016', '2014', '2012', '2010', '2008', '2006', '2004']
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

logger = logging.getLogger(__name__)

def cleanup_data_dirs():
  for state in settings.STATES_IMPLEMENTED:
    try:
      shutil.rmtree(os.path.join(settings.DATA_DIRECTORY, state))
    except:
      continue

def download_and_process_fec_data():
  fec_fetch.download_headers()

  for year in YEARS:
    logger.info('Fetching FEC year %s' % year)
    for url_type, url in DATA_URLS[year]:
      file_path = os.path.join(fec_fetch.download_data(url, url_type, year), 'itcont.txt')
      if url_type == 'contributions':
        fec_fetch.cleanup_unnecessary_contribution_files(year)
        fec_transform.transform_data(file_path, url_type, year)
    fec_fetch.cleanup_data(year)

def download_and_process_wa_data():
  data_file_path = wa_fetch.download_data()
  wa_transform.transform_data(data_file_path)
  os.remove(data_file_path)

def upload_to_socrata():
  home = '/home/ubuntu'
  datasync = os.path.join(home, 'datasync', 'datasync-1.8.2.jar')
  config = os.path.join(home, 'code', 'campfin_admin', 'datasync_config.json')
  control = os.path.join(home, 'code', 'campfin', 'settings', 'datasync_control.json')
  csv = os.path.join(settings.OCD_DIRECTORY, 'WA.csv')
  dataset = 'rvjy-yeu3'
  logger.info(subprocess.Popen(
    "java -jar %s -c %s -f %s -h true -m upsert -ph true -cf %s -i %s" % (
      datasync, config, csv, control, dataset),
    shell=True,
    stdout=subprocess.PIPE).stdout.read())

def upload_to_s3():
  local_path = os.path.join(settings.OCD_DIRECTORY, 'WA.csv')
  utils.write_to_s3('WA.csv', local_path=local_path)

def orchestrate():
  logger.info('Starting cleanup')
  cleanup_data_dirs()

  logger.info('Starting FEC')
  download_and_process_fec_data()

  logger.info('Starting WA')
  download_and_process_wa_data()

  logger.info('Starting upload')
  upload_to_s3()
  upload_to_socrata()

  logger.info('Done with everything')

if __name__ == '__main__':
  orchestrate()
