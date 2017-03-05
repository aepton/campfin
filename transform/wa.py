import errno
import locale
import os
import uuid

from csv import DictReader, DictWriter
from datetime import datetime
from decimal import *
from ocd import *

locale.setlocale(locale.LC_ALL, '')

jurisdiction = 'WA'

counter = 0
file_handles = {}
ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')
WA_DIRECTORY = os.path.join(DATA_DIRECTORY, 'WA')
OCD_DIRECTORY = os.path.join(DATA_DIRECTORY, 'OCD')

PDC_DATETIME_FORMAT = '%m/%d/%Y'

CONTRIBS_DIRECTORY = os.path.join(WA_DIRECTORY, 'contributions')
CONTRIBS_FILE = os.path.join(
  CONTRIBS_DIRECTORY, max(os.listdir(parent_directory), key=os.path.getctime))
print 'Loading %s' % CONTRIBS_FILE

missing_rows = {}
with open(CONTRIBS_FILE) as FH:
  reader = DictReader(FH)
  for row in reader:
    row_id = '%s-%s' % (row['ID'], row['Origin'])
    try:
      receipt_date = datetime.strptime(row['Receipt date'], PDC_DATETIME_FORMAT)
    except Exception, e:
      error = 'receipt date error: %s' % e
      if error not in missing_rows:
        missing_rows[error] = 0
      missing_rows[error] += 1
      continue

    try:
      ocd_row = Transaction(
        row_id='ocd-campaignfinance-transaction/%s' % uuid.uuid5(uuid.NAMESPACE_OID, row_id).hex,
        filing_action=None,
        identifier=row_id,
        classification='contribution',
        amount__value=Decimal(row['Amount'].replace('$', '')),
        amount__currency='$',
        amount__is_inkind=True if row['Cash or in-kind'] == 'Cash' else False,
        sender__entity_type='p',
        sender__person__name=row['Contributor name'],
        sender__person__employer=row['Contributor employer name'],
        sender__person__occupation=row['Contributor occupation'],
        sender__person__location=', '.join(
          [e for e in [
            row['Contributor address'],
            row['Contributor city'],
            row['Contributor state'],
            row['Contributor zip']
          ] if e]),
        recipient=row['Filer ID'],
        recipient__state='WA',
        url=row['URL'].replace('View report (', '')[:-1],
        regulator='PDC',
        date=receipt_date.strftime(OCD_DATETIME_FORMAT),
        description=row['Description'],
        note=row['Memo']
      )
    except Exception, e:
      error = 'ocd loading error: %s' % e
      if error not in missing_rows:
        missing_rows[error] = 0
      missing_rows[error] += 1
      continue

    path = os.path.join(OCD_DIRECTORY, 'WA.csv')

    if path not in file_handles:
      try:
        os.makedirs(OCD_DIRECTORY)
      except OSError as exception:
        if exception.errno != errno.EEXIST:
          raise

      if not os.path.exists(path):
        with open(path, 'w+') as FH:
          writer = DictWriter(FH, TRANSACTION_CSV_HEADER)
          writer.writeheader()
          FH.close()

      file_handles[path] = open(path, 'a')

    file_handles[path].write(ocd_row.to_csv_row())

    counter += 1
    if counter % 1000000 == 0:
      print locale.format('%d', counter, grouping=True)
  print locale.format('%d', counter, grouping=True)

print 'Errors:'
for error in missing_rows:
  print '%s: %d' % (error, missing_rows[error])
