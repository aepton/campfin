import boto3
import dedupe
import logging
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
  print hashable_donor

  return uuid.uuid5(uuid.NAMESPACE_OID, hashable_donor).hex

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
      'ReadCapacityUnits': 1,
      'WriteCapacityUnits': 1
    }
  )

def get_dynamodb_table(table_name):
  session = boto3.Session(profile_name='abe')
  dynamodb = session.resource('dynamodb')
  return dynamodb.Table(table_name)

def get_dynamodb_client():
  session = boto3.Session(profile_name='abe')
  return session.client('dynamodb')

def set_dynamodb_throughput(table, operation, capacity):
  read_operation = 'ReadCapacityUnits'
  write_operation = 'WriteCapacityUnits'

  if table.provisioned_throughput[operation] != capacity:
    throughput = {operation: capacity}

    # Leave the property we didn't intend to modify unmodified
    if operation == read_operation:
      throughput[write_operation] = table.provisioned_throughput[write_operation]
    else:
      throughput[read_operation] = table.provisioned_throughput[read_operation]

    table = table.update(ProvisionedThroughput=throughput)

  return table

def batch_set_cluster_ids(rows):
  client = get_dynamodb_client()

  keys = [row.props['donor_hash'] for row in rows]

  response = client.batch_get_item(
    RequestItems={
      'dedupe': {
        'Keys': [{'donorHash': {'S': key}} for key in keys]
      }
    }
  )

  logging.info('Found %d and missed %d from %d keys' % (
    len(response['Responses'].get('dedupe', [])),
    len(response['UnprocessedKeys'].get('dedupe', [])),
    len(keys)))

  matches = {}
  for key in response['Responses']['dedupe']:
    matches[key['donorHash']] = key['clusterID']

  clustered_rows = []
  unclustered_rows = []
  for row in rows:
    if row.props['donor_hash'] in matches:
      row.props['cluster_id'] = matches[row.props['donor_hash']]
      clustered_rows.append(row)
    else:
      unclustered_rows.append(row)

  return (clustered_rows, unclustered_rows)

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

def store_donor_cluster_map_in_dynamodb():
  table = get_dynamodb_table('dedupe')
  table = set_dynamodb_throughput(table, 'WriteCapacityUnits', 100)

  with open('clusters.csv') as fh:
    reader = DictReader(fh)

    with table.batch_writer() as batch:
      for row in reader:
        table.put_item(
          Item={
            'donorHash': row['donorHash'],
            'clusterID': row['clusterID']
          }
        )

  table = set_dynamodb_throughput(table, 'WriteCapacityUnits', 1)

if __name__ == '__main__':
  #train_dedupe()
  #cluster_records()
  #create_dynamodb_table()
  #store_donor_cluster_map_in_dynamodb()
  record = {
    'Name': "KLEIN, LYN",
    'Employer': 'SELF',
    'Occupation': 'CLERICAL',
    'Address': "BELLEVUE, WA, 980063156"
  }
  donor_hash = generate_donor_hash(record)
  print donor_hash
  table = get_dynamodb_table('dedupe')
  print table.get_item(Key={'donorHash': donor_hash})
  client = get_dynamodb_client()
  response = client.batch_get_item(
    RequestItems={
      'dedupe': {
        'Keys': [{'donorHash': {'S': donor_hash}}]
      }
    }
  )

  print 'Found %d and missed %d' % (
    len(response['Responses'].get('dedupe', [])),
    len(response['UnprocessedKeys'].get('dedupe', [])))
  print response
  print 'Create little tester here, run it to generate hash locally and on server, confirm they match'
