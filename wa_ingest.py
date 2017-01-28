import errno
import os
import uuid

from csv import DictReader, DictWriter
from decimal import *
from ocd import *

jurisdiction = 'WA'
CLOBBER_EXISTING = False

file_handles = {}
current_directory = os.path.dirname(os.path.realpath(__file__))

print 'NEED TO RETHINK SHARDING - PROBABLY SHARD BY YEAR OF CONTRIB'

with open('data/%s/contribs.csv' % jurisdiction) as FH:
  reader = DictReader(FH)
  for row in reader:
    row_id = '%s-%s' % (row['ID'], row['Origin'])
    ocd_row = Transaction(
      row_id='ocd-campaignfinance-transaction/%s' % uuid.uuid5(uuid.NAMESPACE_OID, row_id).hex,
      filing_action=None,
      identifier=row_id,
      classification='contribution',
      amount__value=Decimal(row['Amount'].replace('$', '')),
      amount__currency='$',
      amount__is_inkind=True if row['Cash or in-kind'] == 'Cash' else False,
      sender__name=row['Contributor name'],
      sender__employer=row['Contributor employer name'],
      sender__occupation=row['Contributor occupation'],
      sender__location=', '.join(
        [e for e in [
          row['Contributor address'],
          row['Contributor city'],
          row['Contributor state'],
          row['Contributor zip']
        ] if e]),
      recipient=row['Filer ID'],
      recipient__state='WA',
      url=row['URL'].replace('View report (', '')[:-1],
      date=row['Receipt date'],
      description=row['Description'],
      note=row['Memo']
    )

    directory = os.path.join(current_directory, 'data', 'OCD', jurisdiction)
    path = os.path.join(directory, '%d.csv' % int(row['Election year']))

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
