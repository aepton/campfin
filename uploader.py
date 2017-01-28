import delegator
import requests

from ocd import *
from sodapy import Socrata

def get_socrata_auth(
    lastpass_domain='socrata.com US Superuser',
    socrata_domain='abrahamepton.demo.socrata.com'):
  username = delegator.run('lpass show --username "%s"' % lastpass_domain).out.strip()
  password = delegator.run('lpass show --password "%s"' % lastpass_domain).out.strip()
  token = delegator.run('lpass show --field=pubtoken "%s"' % lastpass_domain).out.strip()
  #return Socrata(socrata_domain, token, username=username, password=password)
  return {
    'username': username,
    'password': password,
    'token': token
  }

auth = get_socrata_auth()

"""
auth.create(
  'Uploaded WA 2016 contribs',
  columns=TRANSACTION_CSV_HEADER,
  row_identifier='id',
  new_backend=True)
"""

files = {
  'file': open('data/OCD/WA/2017.csv')
}

headers = {
  'X-App-Token': auth['token']
}

r = requests.post(
  'https://abrahamepton.demo.socrata.com/imports2?method=scan',
  auth=(auth['username'], auth['password']),
  files=files,
  headers=headers)

print r.raw

print r.status_code

print r.text

print r.json()