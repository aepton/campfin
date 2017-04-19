import errno
import locale
import logging
import ocd
import os
import uuid

from csv import DictReader, DictWriter
from datetime import datetime
from decimal import *
from deduplication import deduper
from settings import settings

locale.setlocale(locale.LC_ALL, '')

jurisdiction = 'FEC'

def load_committee_metadata(year):
  committees = {}
  fec_directory = os.path.join(settings.DATA_DIRECTORY, 'FEC')
  year_directory = os.path.join(fec_directory, year)

  committee_master_header_path = os.path.join(
    fec_directory, 'committee_master_header', 'committee_master_header.csv')

  committee_master_data_path = os.path.join(
    year_directory, 'committee_master', 'committee_master', 'cm.txt')

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

def transform_data(file_path, data_type, year):
  # General FEC settings
  delimiter = ','
  in_kind_codes = ['15Z', '24Z']
  fec_directory = os.path.join(settings.DATA_DIRECTORY, 'FEC')
  year_directory = os.path.join(fec_directory, year)

  # Specific settings depending on what type of file we're transforming
  if data_type == 'contributions':
    header_path = os.path.join(fec_directory, 'contributions_header', 'contributions_header.csv')
    with open(header_path) as fh:
      header = fh.read().strip().split(',')

    delimiter = '|'

  # Load committee metadata (some of which we'll attach to individual rows)
  committees = load_committee_metadata(year)

  # Now load and transform the data we were asked to transform
  counter = 0
  missing_rows = {}
  file_handles = {}

  # Stash the DynamoDB table we're using to look up clusters
  table = deduper.get_dedupe_table()
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
        ocd_row = ocd.Transaction(
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
          date=receipt_date.strftime(settings.OCD_DATETIME_FMT),
          description=row['MEMO_CD'],
          note=row['MEMO_TEXT']
        )
      except Exception, e:
        error = 'ocd loading error: %s' % e
        if error not in missing_rows:
          missing_rows[error] = 0
        missing_rows[error] += 1
        continue

      ocd_row.set_cluster_id(table)

      # Handle contributions to a particular state, and from within that state
      for state in set([committees[row['CMTE_ID']]['state'], row['STATE']]):
        if state.find('/') != -1:
          logging.warning('Odd, found slash in state for %s' % row)
          state = state.replace('/', '')
        if state not in settings.STATES_IMPLEMENTED:
          continue
        path = os.path.join(settings.OCD_DIRECTORY, '%s.csv' % state)

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
        logging.info('Processed %s' % locale.format('%d', counter, grouping=True))
    logging.info('Finished processing with %s' % locale.format('%d', counter, grouping=True))

  logging.info('Errors:')
  for error in missing_rows:
    logging.info('%s: %d' % (error, missing_rows[error]))
