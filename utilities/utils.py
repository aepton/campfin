import base64
import boto3
import json
import logging
import os
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

def write_to_s3(s3_path, local_path, bucket=settings.S3_BUCKET):
  session = boto3.Session(profile_name='abe')
  s3 = session.resource('s3')
  s3.Object(bucket, s3_path).put(Body=open(local_path, 'rb'))