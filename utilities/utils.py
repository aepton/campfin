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
from transform import ocd

def load_alert_filehandles(alerts):
  fhs = {}
  for alert in alerts:
    for email in alert['emails']:
      if email not in fhs:
        fname = '%s.csv' % base64.urlsafe_b64encode(email)
        fhs[email] = DictWriter(
          open(os.path.join(settings.DATA_DIRECTORY, 'alerts', fname), 'w+'),
          ocd.TRANSACTION_CSV_HEADER)
        fhs[email].writeheader()
  return fhs

def load_alert_filters():
  with open(os.path.join(settings.DATA_DIRECTORY, 'alerts.json')) as fh:
    filters = {}
    filters['alerts'] = json.loads(fh.read())
    filters['filehandles'] = load_alert_filters(filters['alerts'])
    return filters
