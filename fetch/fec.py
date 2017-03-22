import zipfile

from fetcher import *

STATE = 'FEC'

HEADER_URLS = {
  'committee_master_header': 'http://www.fec.gov/finance/disclosure/metadata/cm_header_file.csv',
  'contributions_header': 'http://www.fec.gov/finance/disclosure/metadata/indiv_header_file.csv'
}

def create_latest_pointer(path, name):
  data_dir = os.path.dirname(path)
  latest_path = os.path.join(data_dir, name)

  print 'Pointing %s to %s' % (latest_path, path)

  try:
    os.remove(latest_path)
  except:
    pass

  try:
    os.symlink(path, latest_path)
  except Exception, e:
    print 'Error pointing symlink %s to %s: %s' % (latest_path, path, e)
    return path

  return latest_path

def download_headers():
  for header_url_type, header_url in HEADER_URLS.items():
    print 'Downloading header: %s' % header_url
    client = Fetcher(
      download_url=header_url,
      state=STATE,
      data_type=header_url_type,
      file_type='csv')
    client.download_data_by_line()

    create_latest_pointer(client.file_path, 'latest_%s' % header_url_type)

def download_data(url, url_type, year, download_headers=False):
  if download_headers:
    download_headers()

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
  os.remove(client.file_path)

  return create_latest_pointer(zip_dir, 'latest_%s_%s' % (url_type, year))

def cleanup_data(url_type, year):
  pass