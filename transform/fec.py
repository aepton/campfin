import errno
import locale
import os
import uuid

from csv import DictReader, DictWriter
from datetime import datetime
from decimal import *
from ocd import *

locale.setlocale(locale.LC_ALL, '')

jurisdiction = 'FEC'

file_handles = {}
parent_directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

committees = {}

with open('data/%s/header_cm.txt' % jurisdiction) as FH:
  cm_header = FH.read().strip().split('|')

with open('data/%s/cm.txt' % jurisdiction) as FH:
  reader = DictReader(FH, cm_header, delimiter='|')
  for row in reader:
    committees[row['CMTE_ID']] = {
      'name': row['CMTE_NM'],
      'state': row['CMTE_ST']
    }

with open('data/%s/header_itcont.txt' % jurisdiction) as FH:
  itcont_header = FH.read().strip().split('|')

in_kind_codes = ['15Z', '24Z']
FEC_DATETIME_FORMAT = '%m%d%Y'

counter = 0
missing_rows = {}
with open('data/%s/itcont.txt' % jurisdiction) as FH:
  reader = DictReader(FH, itcont_header, delimiter='|')
  for row in reader:
    try:
      receipt_date = datetime.strptime(row['TRANSACTION_DT'], FEC_DATETIME_FORMAT)
    except Exception, e:
      error = 'receipt date error: %s' % e
      if error not in missing_rows:
        missing_rows[error] = 0
      missing_rows[error] += 1
      continue

    try:
      ocd_row = Transaction(
        row_id='ocd-campaignfinance-transaction/%s' % uuid.uuid5(uuid.NAMESPACE_OID, row['SUB_ID']).hex,
        filing_action=None,
        identifier=row['SUB_ID'],
        classification='contribution',
        amount__value=Decimal(row['TRANSACTION_AMT']),
        amount__currency='$',
        amount__is_inkind=True if row['TRANSACTION_TP'] in in_kind_codes else False,
        sender__entity_type='p',
        sender__person__name=row['NAME'],
        sender__person__employer=row['EMPLOYER'],
        sender__person__occupation=row['OCCUPATION'],
        sender__person__location=', '.join(
          [e for e in [
            row['CITY'],
            row['STATE'],
            row['ZIP_CODE']
          ] if e]),
        recipient__entity_type='o',
        recipient__organization__entity_id=row['CMTE_ID'],
        recipient__organization__name=committees[row['CMTE_ID']]['name'],
        recipient__organization__state=committees[row['CMTE_ID']]['state'],
        url='http://docquery.fec.gov/cgi-bin/fecimg/?%s' % row['IMAGE_NUM'],
        regulator='FEC',
        date=receipt_date.strftime(OCD_DATETIME_FORMAT),
        description=row['MEMO_CD'],
        note=row['MEMO_TEXT']
      )
    except Exception, e:
      error = 'ocd loading error: %s' % e
      if error not in missing_rows:
        missing_rows[error] = 0
      missing_rows[error] += 1
      continue

    directory = os.path.join(parent_directory, 'ocd_campaign_finance')
    path = os.path.join(directory, '%s.csv' % committees[row['CMTE_ID']]['state'])

    if path not in file_handles:
      try:
        os.makedirs(directory)
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
