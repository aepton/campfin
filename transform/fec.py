import errno
import locale
import logging
import os
import uuid

from csv import DictReader, DictWriter
from datetime import datetime
from decimal import *
from deduplication import deduper
from ocd import transaction
from settings import fec_identifiers
from settings import settings
from utilities import utils

locale.setlocale(locale.LC_ALL, '')
logger = logging.getLogger(__name__)

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
      committee_types = []
      committee_types.append(fec_identifiers.COMMITTEE_DESIGNATIONS[row['CMTE_DSGN'].upper()])
      committee_types.append(fec_identifiers.COMMITTEE_TYPES[row['CMTE_TP'].upper()])
      committee_types.append(row['CMTE_PTY_AFFILIATION'].upper())
      committee_types.append(fec_identifiers.COMMITTEE_PARTIES[row['CMTE_PTY_AFFILIATION'].upper()])

      committees[row['CMTE_ID']] = {
        'name': row['CMTE_NM'],
        'state': row['CMTE_ST'],
        'officers': [{
          'person': row['TRES_NM'],
          'title': fec_identifiers.TREASURER
        }],
        'statuses': [{
          'start_date': '01/01/%d' % (int(year) - 1),
          'end_date': '12/31/%s' % year,
          'note': 'Filed for this cycle',
          'classification': fec_identifiers.BASIC_FILING_STATUS
        }],
        'committee_types': committee_types,
        'candidacy_designations': ['%s%s' % (fec_identifiers.FEC_PREFIX, row['CAND_ID'])]
        'notes': 'Connected organization: %s' % row['CONNECTED_ORG_NM']
      }

  return committees

def transform_contribution(row, committees, alert_filters):
  in_kind_codes = ['15Z', '24Z']
  try:
    receipt_date = datetime.strptime(row['TRANSACTION_DT'], settings.FEC_DATETIME_FMT)
  except Exception, e:
    error = 'receipt date error: %s' % e
    return ({}, error, [])

  try:
    ocd_row = transaction.Transaction(
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
      note=row['MEMO_TEXT'],
      alert_filters=alert_filters
    )
  except Exception, e:
    error = 'ocd loading error: %s' % e
    return ({}, error, [])

  # Handle contributions to a particular state, and from within that state
  relevant_states = set([committees[row['CMTE_ID']]['state'], row['STATE']])
  return (ocd_row, None, relevant_states)

def transform_transaction_data(file_path, data_type, year):
  # General FEC settings
  delimiter = ','
  fec_directory = os.path.join(settings.DATA_DIRECTORY, 'FEC')
  year_directory = os.path.join(fec_directory, year)

  # Specific settings depending on what type of file we're transforming
  input_settings = {
    'contributions': {
      'delimiter': '|',
      'path': os.path.join(fec_directory, 'contributions_header', 'contributions_header.csv')
    }
  }

  with open(input_settings[data_type]['path']) as fh:
    header = fh.read().strip().split(',')

  # Load committee metadata (some of which we'll attach to individual rows)
  committees = load_committee_metadata(year)
  logger.info('Loaded %d committees' % len(committees))

  # Load transformation functions
  transform_row = {
    'contributions': transform_contribution
  }

  # Load alert filters for contribution type
  output_header = {
    'contributions': transaction.TRANSACTION_CSV_HEADER
  }
  alert_filters = utils.load_alert_filters(output_header[data_type], data_type)
  logger.info('Loaded %d alert filters' % len(alert_filters))

  # Now load and transform the data we were asked to transform
  counter = 0
  missing_rows = {}
  file_handles = {}

  with open(file_path) as fh:
    logger.info('Opened %s' % file_path)
    reader = DictReader(fh, header, delimiter=input_settings[data_type]['delimiter'])

    for row in reader:
      (ocd_row, error, relevant_states) = transform_row[data_type](row, committees, alert_filters)
      if error:
        if error not in missing_rows:
          missing_rows[error] = 0
        missing_rows[error] += 1
        continue

      for state in relevant_states:
        if state.find('/') != -1:
          logger.warning('Odd, found slash in state for %s' % row)
          state = state.replace('/', '')
        if state not in settings.STATES_IMPLEMENTED:
          continue
        path = os.path.join(settings.OCD_DIRECTORY, data_type, '%s.csv' % state)

        if path not in file_handles:
          try:
            os.makedirs(settings.OCD_DIRECTORY)
          except OSError as exception:
            if exception.errno != errno.EEXIST:
              raise

          try:
            os.makedirs(os.path.join(settings.OCD_DIRECTORY, data_type))
          except OSError as exception:
            if exception.errno != errno.EEXIST:
              raise

          if not os.path.exists(path):
            logger.info('Creating path: %s' % path)
            with open(path, 'w+') as fh:
              writer = DictWriter(fh, output_header[data_type])
              writer.writeheader()
              fh.close()

          file_handles[path] = open(path, 'a')

        file_handles[path].write(ocd_row.to_csv_row())

      counter += 1
      if counter % 1000000 == 0:
        logger.info('Processed %s' % locale.format('%d', counter, grouping=True))
    logger.info('Finished processing with %s' % locale.format('%d', counter, grouping=True))

  logger.info('Errors:')
  for error in missing_rows:
    logger.info('%s: %d' % (error, missing_rows[error]))
