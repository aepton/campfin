import errno
import locale
import os
import uuid

from csv import DictReader, DictWriter
from datetime import datetime
from decimal import *
from ocd import *
from settings import settings

locale.setlocale(locale.LC_ALL, '')

jurisdiction = 'WA'

def transform_data(contribs_file_path):
  counter = 0
  file_handles = {}
  missing_rows = {}

  with open(contribs_file_path) as fh:
    reader = DictReader(fh)
    for row in reader:
      row_id = '%s-%s' % (row['ID'], row['origin'])
      try:
        receipt_date = datetime.strptime(row['receipt_date'], settings.PDC_DATETIME_FORMAT)
      except Exception, e:
        error = 'receipt date error: %s' % e
        if error not in missing_rows:
          missing_rows[error] = 0
        missing_rows[error] += 1
        continue

      try:
        ocd_row = Transaction(
          row_id='ocd-campaignfinance-transaction/%s' % uuid.uuid5(uuid.NAMESPACE_OID, row_id).hex,
          filing__action__id=None,
          identifier=row_id,
          classification='contribution',
          amount__value=Decimal(row['amount'].replace('$', '')),
          amount__currency='$',
          amount__is_inkind=True if row['cash_or_in_kind'] == 'Cash' else False,
          sender__entity_type='p',
          sender__person__name=row['contributor_name'],
          sender__person__employer=row['contributor_employer_name'],
          sender__person__occupation=row['contributor_occupation'],
          sender__person__location=', '.join(
            [e for e in [
              row['contributor_address'],
              row['contributor_city'],
              row['contributor_state'],
              row['contributor_zip']
            ] if e]),
          recipient__entity_type='o',
          recipient__organization__name=row['filer_name'],
          recipient__organization__entity_id=row['filer_id'],
          recipient__organization__state='WA',
          sources__url=row['url'].replace('View report (', '')[:-1],
          filing__recipient='PDC',
          date=receipt_date.strftime(settings.OCD_DATETIME_FMT),
          description=row['description'],
          note=row['memo']
        )
      except Exception, e:
        error = 'ocd loading error: %s' % e
        if error not in missing_rows:
          missing_rows[error] = 0
        missing_rows[error] += 1
        continue

      path = os.path.join(settings.OCD_DIRECTORY, 'WA.csv')

      if path not in file_handles:
        try:
          os.makedirs(settings.OCD_DIRECTORY)
        except OSError as exception:
          if exception.errno != errno.EEXIST:
            raise

        if not os.path.exists(path):
          with open(path, 'w+') as fh:
            writer = DictWriter(fh, TRANSACTION_CSV_HEADER)
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

if __name__ == '__main__':
  transform_data()
