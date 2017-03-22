import errno
import locale
import ocd
import os
import uuid

from csv import DictReader, DictWriter
from datetime import datetime
from decimal import *
from settings import settings

locale.setlocale(locale.LC_ALL, '')

jurisdiction = 'FEC'

def load_committee_metadata():
  committees = {}
  fec_directory = os.path.join(settings.DATA_DIRECTORY, 'FEC')

  committee_master_header_path = os.path.join(
    fec_directory, 'committee_master_header', 'latest_committee_master_header')

  committee_master_data_path = os.path.join(
    fec_directory, 'committee_master', 'latest_committee_master')

  with open(committee_master_header_path) as fh:
    header = fh.read().strip().split(',')

  with open(committee_master_data_path) as fh:
    reader = DictReader(fh, header, delimiter='|')
    for row in reader:
      committees[row['CMTE_ID']] = {
        'name': row['CMTE_NM'],
        'state': row['CMTE_ST']
      }

  return committees

def transform_data(file_path, data_type):
  # General FEC settings
  delimiter = ','
  in_kind_codes = ['15Z', '24Z']

  # Specific settings depending on what type of file we're transforming
  if data_type == 'contributions':
    header_path = os.path.join(
      fec_directory, 'contributions_header', 'latest_contributions_header')
    with open(header_path) as fh:
      header = fh.read().strip().split(',')

    delimiter = '|'

  # Load committee metadata (some of which we'll attach to individual rows)
  committees = load_committee_metadata()

  # Now load and transform the data we were asked to transform
  counter = 0
  missing_rows = {}
  file_handles = {}
  with open(file_path) as fh:
    reader = DictReader(fh, header, delimiter=delimiter)
    for row in reader:
      try:
        receipt_date = datetime.strptime(row['TRANSACTION_DT'], settings.FEC_DATETIME_FMT)
      except Exception, e:
        error = 'receipt date error: %s' % e
        if error not in missing_rows:
          missing_rows[error] = 0
        missing_rows[error] += 1
        continue

      try:
        ocd_row = Transaction(
          row_id='ocd-campaignfinance-transaction/%s' % uuid.uuid5(
            uuid.NAMESPACE_OID, row['SUB_ID']).hex,
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
          sources__url='http://docquery.fec.gov/cgi-bin/fecimg/?%s' % row['IMAGE_NUM'],
          filing__recipient='FEC',
          date=receipt_date.strftime(settings.OCD_DATETIME_FORMAT),
          description=row['MEMO_CD'],
          note=row['MEMO_TEXT']
        )
      except Exception, e:
        error = 'ocd loading error: %s' % e
        if error not in missing_rows:
          missing_rows[error] = 0
        missing_rows[error] += 1
        continue

      path = os.path.join(settings.OCD_DIRECTORY, '%s.csv' % committees[row['CMTE_ID']]['state'])

      if path not in file_handles:
        try:
          os.makedirs(settings.OCD_DIRECTORY)
        except OSError as exception:
          if exception.errno != errno.EEXIST:
            raise

        if not os.path.exists(path):
          with open(path, 'w+') as fh:
            writer = DictWriter(fh, ocd.TRANSACTION_CSV_HEADER)
            writer.writeheader()
            fh.close()

        file_handles[path] = open(path, 'a')

      file_handles[path].write(ocd_row.to_csv_row())

      counter += 1
      if counter % 1000000 == 0:
        print locale.format('%d', counter, grouping=True)
    print locale.format('%d', counter, grouping=True)

  print 'Errors:'
  for error in missing_rows:
    print '%s: %d' % (error, missing_rows[error])