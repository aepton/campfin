import os
import requests
import time

from csv import DictReader, DictWriter

ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')

class Fetcher(object):
  def __init__(
      self,
      download_url='',
      state='',
      data_type='',
      retry_attempts=2):

    self.download_url = download_url
    self.state = state
    self.data_type = data_type
    self.retry_attempts = retry_attempts

    self.setup_file()

  def download_data_by_line(self, prepare_automatically=True):
    if not self.retry_attempts or not self.download_url or not self.file_path:
      return

    print 'Writing %s' % self.file_path
    with open(self.file_path, 'w+') as file_handle:
      try:
        print 'Fetching %s' % self.download_url
        self.retry_attempts -= 1
        self.error_counter = 0
        self.download_response = requests.get(self.download_url, stream=True)

        if self.download_response.encoding is None:
          self.download_response.encoding = 'utf-8'

        for line in self.download_response.iter_lines():
          if line:
            try:
              file_handle.write('%s%s' % (unicode(line, errors='ignore'), os.linesep))
            except Exception, e:
              print e
              self.error_counter += 1

      except Exception, e:
        print 'Error downloading %s: %s' % (self.download_url, e)
        if self.retry_attempts:
          print 'Retrying %d more times' % self.retry_attempts
          self.download_data()

      print 'Finished with %d bad lines' % self.error_counter

  def setup_file(self):
    state_directory = os.path.join(DATA_DIRECTORY, self.state)
    if not os.path.isdir(state_directory):
      os.mkdir(state_directory)

    type_directory = os.path.join(state_directory, self.data_type)
    if not os.path.isdir(type_directory):
      os.mkdir(type_directory)

    self.file_path = os.path.join(type_directory, '%d.csv' % int(time.time()))
