import delegator
import json
import os
import requests
import urllib

from transform.ocd import *

from csv import DictReader, DictWriter
from sodapy import Socrata

ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')
OCD_DIRECTORY = os.path.join(DATA_DIRECTORY, 'OCD')

ENABLED_STATES = ['WA']

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
  path = os.path.join(DATA_DIRECTORY, 'dataset_registry.csv')
  with open(path, 'a') as FH:
    writer = DictWriter(FH, ['dataset_id', 'state'])
    writer.writerow({'dataset_id': dataset_id, 'state': state})

def upload_file(state, auth):
  auth = get_socrata_auth()

  file_name = '%s.csv' % state
  path = os.path.join(OCD_DIRECTORY, '%s.csv' % state)
  files = {
    'file': open(path)
  }

  headers = {
    'X-App-Token': auth['token']
  }

  r = requests.post(
    'https://abrahamepton.demo.socrata.com/imports2?method=scan&nbe=true',
    auth=(auth['username'], auth['password']),
    files=files,
    headers=headers)

  try:
    file_id = r.json()['fileId']
  except:
    print 'Error scanning file for %s: %s' % (state, r.text)
    return

  print 'Posted file, got fileId %s' % file_id

  r = requests.post(
    'https://abrahamepton.demo.socrata.com/imports2.json?ingress_strategy=nbe&nbe=true',
    auth=(auth['username'], auth['password']),
    headers=headers,
    data={
      'name': file_name,
      'fileId': file_id,
      'blueprint': json.dumps({
        'columns': TRANSACTION_BLUEPRINT_COLS,
        'skip': 1,
        'name': file_name
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
    client.update_metadata(r.json()['id'], {'tags': [state]})
    client.publish(r.json()['id'])
    client.set_permission(r.json()['id'], 'public')
  except Exception, e:
    pass

  update_dataset_registry(r.json()['id'], state)

if __name__ == '__main__':
  auth = get_socrata_auth()
  for state in ENABLED_STATES:
    print 'Uploading %s' % (state)
    upload_file(state, auth)