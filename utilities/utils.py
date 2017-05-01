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
    filters['alerts'] = json.loads(fh.read())
    filters['filehandles'] = load_alert_filehandles(filters['alerts'], header)
    return filters
