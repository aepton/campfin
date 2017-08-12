import base64
import hashlib
import logging
import os

from csv import DictReader
from datetime import datetime
from jinja2 import Template
from utilities import email_utils
from settings import settings

logger = logging.getLogger(__name__)

def process_alerts():
  """
    1. Diff alerts with last-alerts-old
    2. Format diff
    3. Send email
    4. Move alerts to last-alerts-old
  """
  alerts_dir = os.path.join(settings.DATA_DIRECTORY, 'alerts')
  for path in os.listdir(alerts_dir):
    if path.endswith('.csv'):
      diff = generate_diff(path)

      formatted_diff = format_diff(diff)

      to_addresses = [base64.urlsafe_b64decode(path[:-4])]
      email_utils.send_email(to_addresses=to_addresses, subject='Alerts as of', body=formatted_diff)

      os.rename(path, '%s.old' % path)


def generate_diff(path):
  """
    Stream both new and old alerts files - for each, extract hash for each alert line, store hash in
    set. Set difference tells you which txactn are new or removed - don't care about the rest, so
    the 2 lists we do care about should be relatively small. Stream through the alert file at path
    again, and fill out dicts of new and removed txactns, grouped by campaign and then by type (
    contrib, expense, etc).
  """
  alerts_dir = os.path.join(settings.DATA_DIRECTORY, 'alerts')
  alerts = {
    'new': set(),
    'old': set()
  }

  for (p, path_type) in [(path, 'new'), ('%s.old' % path, 'old')]:
    with open(os.path.join(alerts_dir, p)) as fh:
      reader = DictReader(fh)

      for row in reader:
        hash_digest = hashlib.md5(str(row)).hexdigest()

        alerts[path_type].add(hash_digest)

  hashes = {
    'new': alerts['new'] - alerts['old'],
    'removed': alerts['old'] - alerts['new']
  }

  logger.info(
    'Found %d new alerts, %d old alerts, %d new hashes and %d removed hashes' % (
      len(alerts['new']), len(alerts['old']), len(hashes['new']), len(hashes['removed'])))

  diff = {
    'new': {},
    'removed': {}
  }

  # Gotta do 2 passes, unfortunately.
  for (p, path_type) in [(path, 'new'), ('%s.old' % path, 'removed')]:
    with open(os.path.join(alerts_dir, path)) as fh:
      reader = DictReader(fh)

      for row in reader:
        hash_digest = hashlib.md5(str(row)).hexdigest()

        if hash_digest in hashes[path_type]:
          filer_id = row['recipient__organization__entity_id']
          if filer_id not in diff[path_type]:
            diff[path_type][filer_id] = {'contribs': []}
          diff[path_type][filer_id]['contribs'].append(row)

  logger.info(
    'Diff contains alerts for %d committees with new alerts and %d committees with removed' % (
      len(diff['new']), len(diff['removed'])))

  return diff


def format_diff(diff):
  with open(os.path.join(settings.ROOT_DIRECTORY, 'utilities', 'alert_email.html')) as fh:
    template = Template(fh.read())
    sorted_diffs = {'new': {}, 'removed': {}}
    for key in ['new', 'removed']:
      for filer in diff[key]:
        sorted_diffs[key][filer] = sorted(
          diff["new"][filer]["contribs"], key=lambda c: float(c['amount__value']), reverse=True)
    return template.render({'diff': sorted_diffs})
