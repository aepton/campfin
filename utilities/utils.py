import base64
import boto3
import json
import logging
import os
import tempfile
import uuid

from csv import DictReader, DictWriter
from decimal import *
from cStringIO import StringIO
from settings import settings

logger = logging.getLogger(__name__)

def load_alert_filehandles(alerts, header):
  fhs = {}
  for alert in alerts:
    for email in alert['emails']:
      if email not in fhs:
        fname = '%s.csv' % base64.urlsafe_b64encode(email)
        fhs[email] = DictWriter(
          open(os.path.join(settings.DATA_DIRECTORY, 'alerts', fname), 'w+'),
          header)
        fhs[email].writeheader()
  return fhs

def load_alert_filters(header):
  with open(os.path.join(settings.DATA_DIRECTORY, 'alerts.json')) as fh:
    filters = {}
    try:
      filters['alerts'] = json.loads(fh.read())
    except Exception, e:
      logger.info('No alert filters loaded: %s' % e)
      filters['alerts'] = []
    filters['filehandles'] = load_alert_filehandles(filters['alerts'], header)
    return filters

def write_to_s3(s3_path, local_path=None, contents=None, bucket=settings.S3_BUCKET):
  session = boto3.Session(profile_name='abe')
  s3 = session.resource('s3')
  if local_path:
    source = open(local_path, 'rb')
  else:
    source = StringIO(contents)
  s3.Object(bucket, s3_path).put(Body=source)

def load_from_s3(s3_path, bucket=settings.S3_BUCKET, encoding='utf-8'):
  session = boto3.Session(profile_name='abe')
  s3 = session.resource('s3')
  obj = s3.Object(bucket, s3_path)
  return obj.get()['Body']

def get_temp_filehandle_for_reading_s3_obj(s3_path, bucket=settings.S3_BUCKET, encoding='utf-8'):
  fh = tempfile.NamedTemporaryFile()
  logger.info('Created temp file %s' % fh.name)
  for line in load_from_s3(s3_path, bucket, encoding):
    fh.write(line)
  fh.seek(0)
  return fh
