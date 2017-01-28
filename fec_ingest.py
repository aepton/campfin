import errno
import locale
import os
import uuid

from csv import DictReader, DictWriter
from decimal import *
from ocd import *

locale.setlocale(locale.LC_ALL, '')

jurisdiction = 'FEC'
CLOBBER_EXISTING = True

file_handles = {}
current_directory = os.path.dirname(os.path.realpath(__file__))

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

print 'NEED TO RETHINK SHARDING - PROBABLY SHARD BY YEAR OF CONTRIB'

with open('data/%s/header_itcont.txt' % jurisdiction) as FH:
  itcont_header = FH.read().strip().split('|')

in_kind_codes = ['15Z', '24Z']

counter = 0
with open('data/%s/itcont.txt' % jurisdiction) as FH:
  reader = DictReader(FH, itcont_header, delimiter='|')
  for row in reader:
    ocd_row = Transaction(
      row_id='ocd-campaignfinance-transaction/%s' % uuid.uuid5(uuid.NAMESPACE_OID, row['SUB_ID']).hex,
      filing_action=None,
      identifier=row['SUB_ID'],
      classification='contribution',
      amount__value=Decimal(row['TRANSACTION_AMT']),
      amount__currency='$',
      amount__is_inkind=True if row['TRANSACTION_TP'] in in_kind_codes else False,
      sender__name=row['NAME'],
      sender__employer=row['EMPLOYER'],
      sender__occupation=row['OCCUPATION'],
      sender__location=', '.join(
        [e for e in [
          row['CITY'],
          row['STATE'],
          row['ZIP_CODE']
        ] if e]),
      recipient=committees[row['CMTE_ID']]['name'],
      recipient__state=committees[row['CMTE_ID']]['state'],
      url='http://docquery.fec.gov/cgi-bin/fecimg/?%s' % row['IMAGE_NUM'],
      date=row['TRANSACTION_DT'],
      description=row['MEMO_CD'],
      note=row['MEMO_TEXT']
    )

    directory = os.path.join(current_directory, 'data', 'OCD', committees[row['CMTE_ID']]['state'])
    path = os.path.join(directory, '%d.csv' % 2016)

    if path not in file_handles:
      try:
        os.makedirs(directory)
      except OSError as exception:
        if exception.errno != errno.EEXIST:
          raise

      if CLOBBER_EXISTING:
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
