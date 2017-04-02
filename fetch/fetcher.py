import logging
import os
import pycurl
import requests
import time
import urllib2

from csv import DictReader, DictWriter
from settings import settings

class Fetcher(object):
  def __init__(
      self,
      download_url='',
      state='',
      year='',
      data_type='',
      file_type='',
      relative_name='',
      generate_year_containers=False,
      retry_attempts=2):

    self.download_url = download_url
    self.state = state
    self.year = year
    self.data_type = data_type
    self.file_type = file_type
    self.relative_name = relative_name
    self.generate_year_containers = generate_year_containers
    self.retry_attempts = retry_attempts

    self.setup_file()

  def download_data_by_line(self, prepare_automatically=True):
    if not self.retry_attempts or not self.download_url or not self.file_path:
      return

    logging.info('Writing %s' % self.file_path)
    with open(self.file_path, 'w+') as file_handle:
      try:
        logging.info('Fetching %s' % self.download_url)
        self.retry_attempts -= 1
        self.error_counter = 0
        self.success_counter = 0
        self.download_response = requests.get(self.download_url, stream=True)

        if self.download_response.encoding is None:
          self.download_response.encoding = 'utf-8'

        for line in self.download_response.iter_lines():
          if line:
            try:
              file_handle.write('%s%s' % (unicode(line, errors='ignore'), os.linesep))
              self.success_counter += 1
            except Exception, e:
              logging.warning(e)
              self.error_counter += 1

      except Exception, e:
        logging.warning('Error downloading %s: %s' % (self.download_url, e))
        if self.retry_attempts:
          logging.info('Retrying %d more times' % self.retry_attempts)
          self.download_data()

      logging.info(
        'Finished with %d bad lines and %d successful ones' % (
          self.error_counter, self.success_counter))

  def download_data_pycurl(self):
    if not self.retry_attempts or not self.download_url or not self.file_path:
      return

    logging.info('Writing %s' % self.file_path)
    with open(self.file_path, 'wb') as fh:
      try:
        logging.info('Fetching %s' % self.download_url)
        self.retry_attempts -= 1

        c = pycurl.Curl()
        c.setopt(c.URL, self.download_url)
        c.setopt(c.WRITEDATA, fh)
        c.perform()
        c.close()

      except Exception, e:
        logging.warning('Error downloading %s: %s' % (self.download_url, e))
        if self.retry_attempts:
          logging.info('Retrying %d more times' % self.retry_attempts)
          self.download_data_pycurl()

  def download_data_ftp(self):
    if not self.retry_attempts or not self.download_url or not self.file_path:
      return

    logging.info('Writing %s' % self.file_path)
    with open(self.file_path, 'w+') as file_handle:
      try:
        logging.info('Fetching %s' % self.download_url)
        self.retry_attempts -= 1

        request = urllib2.Request(self.download_url)
        self.download_response = urllib2.urlopen(request)
        file_handle.write(self.download_response.read())

      except Exception, e:
        logging.warning('Error downloading %s: %s' % (self.download_url, e))
        if self.retry_attempts:
          logging.info('Retrying %d more times' % self.retry_attempts)
          self.download_data_ftp()

  def setup_file(self):
    if not os.path.isdir(settings.DATA_DIRECTORY):
      os.mkdir(settings.DATA_DIRECTORY)

    state_directory = os.path.join(settings.DATA_DIRECTORY, self.state)
    if not os.path.isdir(state_directory):
      os.mkdir(state_directory)

    if self.generate_year_containers:
      year_container = os.path.join(state_directory, self.year)
      if not os.path.isdir(year_container):
        os.mkdir(year_container)
    else:
      year_container = state_directory

    type_directory = os.path.join(year_container, self.data_type)
    if not os.path.isdir(type_directory):
      os.mkdir(type_directory)

    if not self.relative_name:
      self.relative_name = time.time()
    self.file_path = os.path.join(type_directory, '%s.%s' % (self.relative_name, self.file_type))
