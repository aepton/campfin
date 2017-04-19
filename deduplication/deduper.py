import boto3
import dedupe
import uuid

from csv import DictReader, DictWriter
from dedupe import Dedupe, StaticDedupe, canonicalize

variables = [
  {'field': 'Name', 'type': 'String'},
  {'field': 'Address', 'type': 'String', 'has missing': True},
  {'field': 'Employer', 'type': 'String', 'has missing': True},
  {'field': 'Occupation', 'type': 'String', 'has missing': True}
]

def generate_donor_hash(record):
  hashable_donor = ';'.join([
    record['Name'] if record['Name'] else '',
    record['Address'] if record['Address'] else '',
    record['Employer'] if record['Employer'] else '',
    record['Occupation'] if record['Occupation'] else ''
  ])

  return uuid.uuid5(uuid.NAMESPACE_OID, hashable_donor).hex

def load_records(limit):
  records = {}

  seen_donor_hashes = set()

  with open('/Users/abraham.epton/Downloads/WA_contributions__to_and_from.csv') as fh:
    reader = DictReader(fh)
    for row in reader:
      record = {
        'Name': row['sender__person__name'] if row['sender__person__name'] else None,
        'Address': row['sender__person__location'] if row['sender__person__location'] else None,
        'Employer': row['sender__person__employer'] if row['sender__person__employer'] else None,
        'Occupation': row['sender__person__occupation'] if row['sender__person__occupation'] else None
      }
      donor_hash = generate_donor_hash(record)
      if donor_hash not in seen_donor_hashes:
        seen_donor_hashes.add(donor_hash)
        records[row['id']] = record
        if len(records) >= limit and limit != -1:
          return records
  print 'Found %d records' % len(records)
  return records

def train_dedupe():
  records = load_records(50000)

  deduper = Dedupe(variables)
  sample = deduper.sample(records)

  dedupe.consoleLabel(deduper)

  deduper.train()

  with open('training.json', 'w+') as fh:
    deduper.writeTraining(fh)

  with open('settings.dedupe', 'wb') as fh:
    deduper.writeSettings(fh)

def cluster_records():
  with open('settings.dedupe') as fh:
    deduper = StaticDedupe(fh)

  records = load_records(-1)

  #threshold = deduper.threshold(records, recall_weight=.5)
  threshold = 0.799195
  print 'Setting threshold', threshold

  """
  for field in deduper.blocker.index_fields:
    field_data = set(records[record][field] for record in records)
    deduper.index(field_data, field)

  blocks = deduper.blocker(records.items())
  for block in blocks:
    print block
  duplicates = deduper.matchBlocks(blocks)
  """
  duplicates = deduper.match(records, threshold)

  with open('clusters.csv', 'w+') as fh:
    writer = DictWriter(fh, ['objectID', 'clusterID', 'donorHash'])
    writer.writeheader()

    for dupe in duplicates:
      for contrib in dupe[0]:
        writer.writerow({
          'objectID': contrib,
          'clusterID': dupe[0][0],
          'donorHash': generate_donor_hash(records[contrib])
        })

def create_dynamodb_table():
  session = boto3.Session(profile_name='abe')
  dynamodb = session.client('dynamodb')

  table = dynamodb.create_table(
    TableName='dedupe',
    KeySchema=[{
      'AttributeName': 'donorHash',
      'KeyType': 'HASH'
    }],
    AttributeDefinitions=[{
      'AttributeName': 'donorHash',
      'AttributeType': 'S'
    }],
    ProvisionedThroughput={
      'ReadCapacityUnits': 5,
      'WriteCapacityUnits': 5
    }
  )

def store_donor_cluster_map_in_dynamodb():
  session = boto3.Session(profile_name='abe')
  dynamodb = session.resource('dynamodb')
  table = dynamodb.Table('dedupe')

  print table.item_count

  with open('clusters.csv') as fh:
    reader = DictReader(fh)

    with table.batch_writer(overwrite_by_pkeys=['donorHash']) as batch:
      for row in reader:
        table.put_item(
          Item={
            'donorHash': row['donorHash'],
            'clusterID': row['clusterID']
          }
        )

  print 'Table has %d items' % table.item_count

if __name__ == '__main__':
  #train_dedupe()
  cluster_records()
  #create_dynamodb_table()
  store_donor_cluster_map_in_dynamodb()
