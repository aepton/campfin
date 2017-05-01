import logging
import os

from settings import settings

# Setting this up before other imports so everything logs correctly
logging.basicConfig(
  filename=os.path.join(settings.LOG_DIR, 'dedupe.log'),
  filemode='a',
  format='%(levelname)s %(asctime)s %(filename)s:%(lineno)d in %(funcName)s: %(message)s',
  level=logging.INFO)

import boto3
import dedupe
import pandas
import uuid

from cStringIO import StringIO
from csv import DictReader, DictWriter
from datetime import datetime
from dedupe import Dedupe, StaticDedupe, canonicalize
from utilities import utils

logger = logging.getLogger(__name__)

variables = [
  {'field': 'Name', 'type': 'String'},
  {'field': 'Address', 'type': 'String', 'has missing': True},
  {'field': 'Employer', 'type': 'String', 'has missing': True},
  {'field': 'Occupation', 'type': 'String', 'has missing': True}
]

def generate_donor_hash(record):
  hashable_donor = ';'.join([
    record['Name'].strip() if record['Name'] else '',
    record['Address'].strip() if record['Address'] else '',
    record['Employer'].strip() if record['Employer'] else '',
    record['Occupation'].strip() if record['Occupation'] else ''
  ]).upper()

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
        'Keys': [{'donorHash': {'S': key}} for key in set(keys)]
      }
    }
  )

  logger.info('Found %d and missed %d from %d keys' % (
    len(response['Responses'].get('dedupe', [])),
    len(response['UnprocessedKeys'].get('dedupe', [])),
    len(keys)))

  matches = {}
  for key in response['Responses']['dedupe']:
    matches[key['donorHash']['S']] = key['clusterID']['S']

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
  fh = StringIO(utils.get_temp_filehandle_for_reading_s3_obj('WA.csv'))
  reader = DictReader(fh)
  for row in reader:
    record = {
      'Name': row['sender__person__name'] if row['sender__person__name'] else None,
      'Address': row['sender__person__location'] if row['sender__person__location'] else None,
      'Employer': row['sender__person__employer'] if row['sender__person__employer'] else None,
      'Occupation': (row['sender__person__occupation'] if row['sender__person__occupation'] else
        None)
    }
    donor_hash = generate_donor_hash(record)
    if donor_hash not in seen_donor_hashes:
      seen_donor_hashes.add(donor_hash)
      records[row['id']] = record
      if len(records) >= limit and limit != -1:
        fh.close()
        return records
  print 'Found %d records' % len(records)
  fh.close()
  return records

def train_dedupe():
  records = load_records(50000)

  deduper = Dedupe(variables)
  sample = deduper.sample(records)

  try:
    fh = StringIO(utils.load_from_s3('training.json'))
    deduper.readTraining(fh)
  except:
    pass

  dedupe.consoleLabel(deduper)

  deduper.train()

  deduper = Dedupe(variables)
  with open('data/dedupe/training.json') as fh:
    deduper.readTraining(fh)
  output = StringIO()
  deduper.writeTraining(output)
  utils.write_to_s3('training.json', contents=output.getvalue())

  output = StringIO()
  deduper.writeSettings(output)
  utils.write_to_s3('settings.dedupe', contents=output.getvalue())

def cluster_records():
  fh = StringIO(utils.load_from_s3('settings.dedupe'))
  deduper = StaticDedupe(fh)

  records = load_records(-1)

  #threshold = deduper.threshold(records, recall_weight=.5)
  threshold = 0.799195
  logger.info('Setting cluster threshold: %s' % threshold)

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

  with open('data/clusters.csv', 'w+') as fh:
    writer = DictWriter(fh, ['clusterID', 'donorHash'])
    writer.writeheader()

    for dupe in duplicates:
      for contrib in dupe[0]:
        writer.writerow({
          'clusterID': dupe[0][0],
          'donorHash': generate_donor_hash(records[contrib])
        })
  utils.write_to_s3('clusters.csv', local_path='data/clusters.csv')

def store_donor_cluster_map_in_dynamodb():
  table = get_dynamodb_table('dedupe')
  table = set_dynamodb_throughput(table, 'WriteCapacityUnits', 100)

  with open('data/clusters.csv') as fh:
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

def store_donor_cluster_map_in_s3():
  df = pandas.read_csv('/Users/abraham.epton/Downloads/WA_contributions__to_and_from.csv.current')
  df.to_hdf('data/WA_contribs.h5', 'table')
  #df = pandas.read_csv('clusters.csv')
  #df.to_hdf('data/WA_clusters.h5', 'table')

def merge_donors_clusters_contribs():
  print 'starting', datetime.now().isoformat()
  donations_df = pandas.read_hdf('data/WA_contribs.h5', 'table')
  print 'done loading donations', datetime.now().isoformat()
  donations_df['amount__value'] = donations_df['amount__value'].map(lambda x: x.replace('$', ''))
  print 'done processing column'
  clusters_df = pandas.read_hdf('data/WA_clusters.h5', 'table')
  print 'done loading clusters', datetime.now().isoformat()
  donations_df.set_index('id').join(clusters_df.set_index('objectID')).to_csv(
    'data/combined_WA.csv')
  print 'done joining', datetime.now().isoformat()


if __name__ == '__main__':
  train_dedupe()
  #cluster_records()
  #create_dynamodb_table()
  #store_donor_cluster_map_in_dynamodb()
  #store_donor_cluster_map_in_s3()
  #merge_donors_clusters_contribs()
