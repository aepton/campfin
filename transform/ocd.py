from csv import DictReader, DictWriter
from decimal import *
from cStringIO import StringIO
from settings import settings

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
      csv_header=TRANSACTION_CSV_HEADER):

    self.csv_header = csv_header

    self.props = {
      'id': row_id,
      'filing__recipient': filing__recipient,
      'filing__action__id': filing_action,
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
      'note': note
    }

  def to_csv_row(self):
    row_output = StringIO()
    writer = DictWriter(row_output, self.csv_header)
    writer.writerow(self.props)
    return row_output.getvalue()
