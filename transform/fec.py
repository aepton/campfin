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
ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')
FEC_DIRECTORY = os.path.join(DATA_DIRECTORY, 'FEC')
OCD_DIRECTORY = os.path.join(DATA_DIRECTORY, 'OCD')

committees = {}

COMMITTEE_MASTER_HEADER_DIR = os.path.join(FEC_DIRECTORY, 'committee_master_header')
COMMITTEE_MASTER_HEADER_PATH = max(
  map(
    lambda p: os.path.join(COMMITTEE_MASTER_HEADER_DIR, p),
    os.listdir(COMMITTEE_MASTER_HEADER_DIR)
  ),
  key=os.path.getctime)

COMMITTEE_MASTER_DATA_DIR = os.path.join(FEC_DIRECTORY, 'committee_master')
COMMITTEE_MASTER_DATA_PATH = os.path.join(
  max(
    filter(
      lambda f: os.path.isdir(f),
      map(
        lambda p: os.path.join(COMMITTEE_MASTER_DATA_DIR, p),
        os.listdir(COMMITTEE_MASTER_DATA_DIR)
      )
    ),
    key=os.path.getctime
  ),
  'cm.txt')

CONTRIBUTIONS_HEADER_DIR = os.path.join(FEC_DIRECTORY, 'contributions_header')
CONTRIBUTIONS_HEADER_PATH = max(
  map(
    lambda p: os.path.join(CONTRIBUTIONS_HEADER_DIR, p),
    os.listdir(CONTRIBUTIONS_HEADER_DIR)
  ),
  key=os.path.getctime)

CONTRIBUTIONS_DATA_DIR = os.path.join(FEC_DIRECTORY, 'contributions')
CONTRIBUTIONS_DATA_PATH = os.path.join(
  max(
    filter(
      lambda f: os.path.isdir(f),
      map(
        lambda p: os.path.join(CONTRIBUTIONS_DATA_DIR, p),
        os.listdir(CONTRIBUTIONS_DATA_DIR)
      )
    ),
    key=os.path.getctime
  ),
  'itcont.txt')

with open(COMMITTEE_MASTER_HEADER_PATH) as FH:
  cm_header = FH.read().strip().split(',')

with open(COMMITTEE_MASTER_DATA_PATH) as FH:
  reader = DictReader(FH, cm_header, delimiter='|')
  for row in reader:
    committees[row['CMTE_ID']] = {
      'name': row['CMTE_NM'],
      'state': row['CMTE_ST']
    }

with open(CONTRIBUTIONS_HEADER_PATH) as FH:
  itcont_header = FH.read().strip().split(',')

in_kind_codes = ['15Z', '24Z']
FEC_DATETIME_FORMAT = '%m%d%Y'

counter = 0
missing_rows = {}
with open(CONTRIBUTIONS_DATA_PATH) as FH:
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
        filing__action__id=None,
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
        filing__recipient='FEC',
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

    path = os.path.join(OCD_DIRECTORY, '%s.csv' % committees[row['CMTE_ID']]['state'])

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
