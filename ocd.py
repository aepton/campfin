from csv import DictReader, DictWriter
from decimal import *
from cStringIO import StringIO

TRANSACTION_CSV_HEADER = [
  'id',
  'filing_action',
  'identifier',
  'classification',
  'url',
  'amount__value',
  'amount__currency',
  'amount__is_inkind',
  'sender__name',
  'sender__employer',
  'sender__occupation',
  'sender__location',
  'recipient',
  'recipient__state',
  'date',
  'description',
  'note'
]

class Transaction(object):
  def __init__(
      self,
      row_id='',
      filing_action='',
      identifier='',
      classification='',
      url='',
      amount__value=Decimal('0.00'),
      amount__currency='$',
      amount__is_inkind=False,
      sender__name='',
      sender__employer='',
      sender__occupation='',
      sender__location='',
      recipient='',
      recipient__state='',
      date='',
      description='',
      note='',
      csv_header=TRANSACTION_CSV_HEADER):

    self.csv_header = csv_header

    self.props = {
      'id': row_id,
      'filing_action': filing_action,
      'identifier': identifier,
      'classification': classification,
      'url': url,
      'amount__value': amount__value,
      'amount__currency': amount__currency,
      'amount__is_inkind': amount__is_inkind,
      'sender__name': sender__name,
      'sender__employer': sender__employer,
      'sender__occupation': sender__occupation,
      'sender__location': sender__location,
      'recipient': recipient,
      'recipient__state': recipient__state,
      'date': date,
      'description': description,
      'note': note
    }

  def to_csv_row(self):
    row_output = StringIO()
    writer = DictWriter(row_output, self.csv_header)
    writer.writerow(self.props)
    return row_output.getvalue()
