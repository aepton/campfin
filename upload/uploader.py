import delegator
import json
import os
import requests
import urllib

from csv import DictReader, DictWriter
from ocd import *
from sodapy import Socrata

PARENT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(PARENT_DIRECTORY, 'ocd_campaign_finance')



def get_socrata_auth(
    lastpass_domain=os.environ['LASTPASS_DOMAIN'],
    socrata_domain='abrahamepton.demo.socrata.com'):
  username = delegator.run('lpass show --username "%s"' % lastpass_domain).out.strip()
  password = delegator.run('lpass show --password "%s"' % lastpass_domain).out.strip()
  token = delegator.run('lpass show --field=pubtoken "%s"' % lastpass_domain).out.strip()
  return {
    'username': username,
    'password': password,
    'token': token
  }

def update_dataset_registry(dataset_id, state, year):
  with open('dataset_registry.csv', 'a') as FH:
    writer = DictWriter(FH, ['dataset_id', 'state', 'year'])
    writer.writerow({'dataset_id': dataset_id, 'state': state, 'year': year})

def upload_file(state, auth):
  auth = get_socrata_auth()

  path = os.path.join(DATA_DIRECTORY, state)
  files = {
    'file': open(path)
  }

  headers = {
    'X-App-Token': auth['token']
  }

  r = requests.post(
    'https://abrahamepton.demo.socrata.com/imports2?method=scan',
    auth=(auth['username'], auth['password']),
    files=files,
    headers=headers)

  try:
    file_id = r.json()['fileId']
  except:
    print 'Error scanning file for %s-%d: %s' % (state, year, r.text)
    return

  file_name = '%s_%d.csv' % (state, year)
  pretty_file_name = '%s %d' % (state, year)

  print 'Posted file, got fileId %s' % file_id

  r = requests.post(
    'https://abrahamepton.demo.socrata.com/imports2.json',
    auth=(auth['username'], auth['password']),
    headers=headers,
    data={
      'name': file_name,
      'fileId': file_id,
      'blueprint': json.dumps({
        'columns': TRANSACTION_BLUEPRINT_COLS,
        'skip': 1,
        'name': pretty_file_name
      })
    })

  try:
    print 'Got status code %s for dataset %s' % (r.status_code, r.json()['id'])
  except KeyError:
    print 'Error uploading; got %s' % r.text
    return

  client = Socrata(
    'abrahamepton.demo.socrata.com',
    auth['token'],
    username=auth['username'],
    password=auth['password'])

  try:
    client.update_metadata(r.json()['id'], {'tags': [state, str(year)]})
    client.publish(r.json()['id'])
    client.set_permission(r.json()['id'], 'public')
  except Exception, e:
    pass

  update_dataset_registry(r.json()['id'], state, year)

if __name__ == '__main__':
  #auth = get_socrata_auth()
  current_dir = os.path.dirname(os.path.realpath(__file__))
  for state in os.listdir(os.path.join(current_dir, 'data', 'OCD')):
    for file in os.listdir(os.path.join(current_dir, 'data', 'OCD', state)):
      if file.endswith('.csv'):
        try:
          year = int(file.replace('.csv', ''))
        except Exception, e:
          print 'No way to cast year %s as int: %s' % (file, e)
          continue
        print 'Uploading %s %d' % (state)
        #upload_file('WA', auth)