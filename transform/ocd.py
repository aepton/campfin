import base64
import boto3
import json
import logging
import os
import uuid

from csv import DictReader, DictWriter
from decimal import *
from deduplication import deduper
from cStringIO import StringIO
from settings import settings
from utilities import utils

logger = logging.getLogger(__name__)

TRANSACTION_COLUMNS = [
  {
    'name': 'Id',
    'datatype': 'text',
    'csv_name': 'id'
  },
  {
    'name': 'Regulator',
    'datatype': 'text',
    'csv_name': 'filing__recipient'
  },
  {
    'name': 'Filing Action',
    'datatype': 'text',
    'csv_name': 'filing__action__id'
  },
  {
    'name': 'Identifier',
    'datatype': 'text',
    'csv_name': 'identifier'
  },
  {
    'name': 'Classification',
    'datatype': 'text',
    'csv_name': 'classification'
  },
  {
    'name': 'URL',
    'datatype': 'text',
    'csv_name': 'sources__url'
  },
  {
    'name': 'Amount (value)',
    'datatype': 'number',
    'csv_name': 'amount__value'
  },
  {
    'name': 'Amount (currency)',
    'datatype': 'text',
    'csv_name': 'amount__currency'
  },
  {
    'name': 'Amount (is inkind)',
    'datatype': 'text',
    'csv_name': 'amount__is_inkind'
  },
  {
    'name': 'Sender (entity type)',
    'datatype': 'text',
    'csv_name': 'sender__entity_type'
  },
  {
    'name': 'Sender (name)',
    'datatype': 'text',
    'csv_name': 'sender__person__name'
  },
  {
    'name': 'Sender (employer)',
    'datatype': 'text',
    'csv_name': 'sender__person__employer'
  },
  {
    'name': 'Sender (occupation)',
    'datatype': 'text',
    'csv_name': 'sender__person__occupation'
  },
  {
    'name': 'Sender (location)',
    'datatype': 'text',
    'csv_name': 'sender__person__location'
  },
  {
    'name': 'Recipient (entity type)',
    'datatype': 'text',
    'csv_name': 'recipient__entity_type'
  },
  {
    'name': 'Recipient (name)',
    'datatype': 'text',
    'csv_name': 'recipient__organization__name'
  },
  {
    'name': 'Recipient (entity ID)',
    'datatype': 'text',
    'csv_name': 'recipient__organization__entity_id'
  },
  {
    'name': 'Recipient (state)',
    'datatype': 'text',
    'csv_name': 'recipient__organization__state'
  },
  {
    'name': 'Date',
    'datatype': 'calendar_date',
    'csv_name': 'date'
  },
  {
    'name': 'Description',
    'datatype': 'text',
    'csv_name': 'description'
  },
  {
    'name': 'Note',
    'datatype': 'text',
    'csv_name': 'note'
  },
  {
    'name': 'Donor Hash',
    'datatype': 'text',
    'csv_name': 'donor_hash'
  },
  {
    'name': 'Cluster ID',
    'datatype': 'text',
    'csv_name': 'cluster_id'
  },
  {
    'name': 'Related Cluster ID',
    'datatype': 'text',
    'csv_name': 'related_cluster_id'
  }
]

TRANSACTION_CSV_HEADER = [h['csv_name'] for h in TRANSACTION_COLUMNS]
TRANSACTION_BLUEPRINT_COLS = [
  {'name': h['name'], 'datatype': h['datatype']} for h in TRANSACTION_COLUMNS]

class Transaction(object):
  def __init__(
      self,
      row_id='',
      filing__recipient='',
      filing__action__id='',
      identifier='',
      classification='',
      sources__url='',
      amount__value=Decimal('0.00'),
      amount__currency='$',
      amount__is_inkind=False,
      sender__entity_type='',
      sender__person__name='',
      sender__person__employer='',
      sender__person__occupation='',
      sender__person__location='',
      recipient__entity_type='',
      recipient__organization__name='',
      recipient__organization__entity_id='',
      recipient__organization__state='',
      date='',
      description='',
      note='',
      csv_header=TRANSACTION_CSV_HEADER,
      alert_file_handles={},
      alert_filters=[]):

    self.alert_emails = set()
    self.alert_file_handles = alert_file_handles
    self.alert_filters = alert_filters
    self.csv_header = csv_header

    self.props = {
      'id': row_id,
      'filing__recipient': filing__recipient,
      'filing__action__id': filing__action__id,
      'identifier': identifier,
      'classification': classification,
      'sources__url': sources__url,
      'amount__value': amount__value,
      'amount__currency': amount__currency,
      'amount__is_inkind': amount__is_inkind,
      'sender__entity_type': sender__entity_type,
      'sender__person__name': sender__person__name,
      'sender__person__employer': sender__person__employer,
      'sender__person__occupation': sender__person__occupation,
      'sender__person__location': sender__person__location,
      'recipient__entity_type': recipient__entity_type,
      'recipient__organization__name': recipient__organization__name,
      'recipient__organization__entity_id': recipient__organization__entity_id,
      'recipient__organization__state': recipient__organization__state,
      'date': date,
      'description': description,
      'note': note,
      'donor_hash': '',
      'cluster_id': '',
      'related_cluster_id': ''
    }

    self.set_donor_hash()
    self.process_alert_filters()
    if self.alert_emails:
      self.update_alert_files()

  def process_alert_filters(self):
    if not self.alert_filters:
      self.alert_filters = utils.load_alert_filters(TRANSACTION_CSV_HEADER)

    for alert in self.alert_filters['alerts']:
      matching = True
      for key in alert.keys():
        if key == 'emails':
          continue
        if self.props.get(key, None) != alert[key]:
          matching = False
        else:
          logger.info('Matching alert: %s' % alert)
      if matching:
        logger.info(
          'Found match; alert: %s (%s); props: %s, alert type: %s, keys: %s' % (
            alert, self.alert_filters, self.props, type(alert), alert.keys()))
        if 'emails' not in alert:
          logger.info('No emails in alert')
          continue
        try:
          logger.info(
            'Storing alert on %s: %s for %s' % (key, self.props[key], alert['emails']))
          [self.alert_emails.add(em) for em in alert['emails']]
        except Exception, e:
          logger.info('Error storing alert %s for props %s: %s, %s, %s' % (
            alert, self.props, type(e).__name__, e.args, alert.keys()))


  def update_alert_files(self):
    for email in self.alert_emails:
      if email in self.alert_filters['filehandles']:
        self.alert_filters['filehandles'][email].writerow(self.to_csv_row())

  def set_donor_hash(self):
    record = {
      'Name': self.props['sender__person__name'],
      'Address': self.props['sender__person__location'],
      'Employer': self.props['sender__person__employer'],
      'Occupation': self.props['sender__person__occupation']
    }
    self.props['donor_hash'] = deduper.generate_donor_hash(record)


  def set_cluster_id(self, table=None):
    if not table:
      table = deduper.get_dedupe_table()
    response = table.get_item(
      Key={'donorHash': self.props['donor_hash']},
      ConsistentRead=False,
      ReturnConsumedCapacity='NONE'
    )
    try:
      self.props['cluster_id'] = response['Item']['clusterID']
    except Exception, e:
      pass

  def to_csv_row(self):
    row_output = StringIO()
    writer = DictWriter(row_output, self.csv_header)
    writer.writerow(self.props)
    return row_output.getvalue()
