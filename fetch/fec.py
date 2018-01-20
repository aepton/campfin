import logging
import shutil
import zipfile

from fetcher import *
from settings import settings

STATE = 'FEC'

HEADER_URLS = {
  'committee_master_header': 'http://www.fec.gov/finance/disclosure/metadata/cm_header_file.csv',
  'contributions_header': 'http://www.fec.gov/finance/disclosure/metadata/indiv_header_file.csv'
}

logger = logging.getLogger(__name__)

def download_headers():
  for header_url_type, header_url in HEADER_URLS.items():
    logger.info('Downloading header: %s' % header_url)
    client = Fetcher(
      download_url=header_url,
      state=STATE,
      data_type=header_url_type,
      file_type='csv',
      relative_name=header_url_type)
    client.download_data_by_line()


def download_data(url, url_type, year, download_headers=False):
  if download_headers:
    download_headers()

  logger.info('Downloading %s' % url)
  file_type = 'zip'
  client = Fetcher(
    download_url=url,
    state=STATE,
    year=year,
    generate_year_containers=True,
    data_type=url_type,
    file_type=file_type,
    relative_name=url_type)
  client.download_data_pycurl()

  zip_ref = zipfile.ZipFile(client.file_path, 'r')
  zip_dir = client.file_path[:-1 * len('.%s' % file_type)]
  zip_ref.extractall(zip_dir)
  zip_ref.close()
  os.remove(client.file_path)

  return zip_dir

def cleanup_data(year):
  shutil.rmtree(os.path.join(settings.DATA_DIRECTORY, STATE, year))
