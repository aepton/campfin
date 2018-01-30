import base64
import boto3
import json
import logging
import ocd_base
import os
import uuid

from csv import DictReader, DictWriter
from decimal import *
from deduplication import deduper
from cStringIO import StringIO
from settings import settings
from utilities import utils

logger = logging.getLogger(__name__)

COMMITTEE_TYPE_COLUMNS = [
  {
    'name': 'ID',
    'datatype': 'text',
    'csv_name': 'id'
  },
  {
    'name': 'Ballot Name',
    'datatype': 'text',
    'csv_name': 'ballot_name'
  },
  {
    'name': 'Ballot Year',
    'datatype': 'number',
    'csv_name': 'ballot_year'
  },
  {
    'name': 'Ballot Jurisdiction',
    'datatype': 'text',
    'csv_name': 'ballot_jurisdiction'
  }
]

COMMITTEE_TYPE_CSV_HEADER = [h['csv_name'] for h in COMMITTEE_TYPE_COLUMNS]

COMMITTEE_TYPE_BLUEPRINT_COLS = [
  {'name': h['name'], 'datatype': h['datatype']} for h in COMMITTEE_TYPE_COLUMNS]

class CommitteeType(ocd_base.OCD):
  def __init__(
      self,
      row_id='',
      ballot_name='',
      ballot_year=None,
      ballot_jurisdiction='',
      csv_header=COMMITTEE_TYPE_CSV_HEADER):
    super(OCD, self).__init__()

    self.csv_header = csv_header

    if row_id == '':
      row_id = '-'.join([ballot_name, ballot_year, ballot_jurisdiction])

    self.props.update({
      'id': row_id,
      'ballot_name': ballot_name,
      'ballot_year': ballot_year,
      'ballot_jurisdiction': ballot_jurisdiction
    })

COMMITTEE_COLUMNS = [
  {
    'name': 'Id',
    'datatype': 'text',
    'csv_name': 'id'
  },
  {
    'name': 'Name',
    'datatype': 'text',
    'csv_name': 'name'
  },
  {
    'name': 'Alternate Name',
    'datatype': 'text',
    'csv_name': 'alternate_name'
  },
  {
    'name': 'Identifier',
    'datatype': 'text',
    'csv_name': 'identifier'
  },
  {
    'name': 'Classification',
    'datatype': 'text',
    'csv_name': 'classification'
  },
  {
    'name': 'Geographic Area',
    'datatype': 'text',
    'csv_name': 'geographic_area'
  },
  {
    'name': 'Street 1',
    'datatype': 'text',
    'csv_name': 'contact__street_1'
  },
  {
    'name': 'Street 2',
    'datatype': 'text',
    'csv_name': 'contact__street_2'
  },
  {
    'name': 'City',
    'datatype': 'text',
    'csv_name': 'contact__city'
  },
  {
    'name': 'State',
    'datatype': 'text',
    'csv_name': 'contact__state'
  },
  {
    'name': 'Country',
    'datatype': 'text',
    'csv_name': 'contact__country'
  },
  {
    'name': 'Officers',
    'datatype': 'text',
    'csv_name': 'officers'
  },
  {
    'name': 'Status 1 - Start',
    'datatype': 'calendar_date',
    'csv_name': 'status_1__start'
  },
  {
    'name': 'Status 1 - End',
    'datatype': 'calendar_date',
    'csv_name': 'status_1__end'
  },
  {
    'name': 'Status 1 - Note',
    'datatype': 'text',
    'csv_name': 'status_1__note'
  },
  {
    'name': 'Status 1 - Classification',
    'datatype': 'text',
    'csv_name': 'status_1__classification'
  },
  {
    'name': 'Committee Types',
    'datatype': 'text',
    'csv_name': 'committee_types'
  },
  {
    'name': 'Candidacy Designations',
    'datatype': 'text',
    'csv_name': 'candidacy_designations'
  },
  {
    'name': 'Notes',
    'datatype': 'text',
    'csv_name': 'notes'
  },
  {
    'name': 'Filing Year',
    'datatype': 'text',
    'csv_name': 'filing_year'
  }
]

COMMITTEE_CSV_HEADER = [h['csv_name'] for h in COMMITTEE_COLUMNS]

COMMITTEE_BLUEPRINT_COLS = [
  {'name': h['name'], 'datatype': h['datatype']} for h in COMMITTEE_COLUMNS]


class Committee(ocd_base.Organization):
  def __init__(
      self,
      row_id='',
      name='',
      alternate_name='',
      identifier='',
      classification='',
      geographic_area='',
      contact__street_1='',
      contact__street_2='',
      contact__city='',
      contact__state='',
      contact__country='',
      officers=[],
      statuses=[],
      committee_types=[],
      candidacy_designations=[],
      notes='',
      filing_year='',
      csv_header=COMMITTEE_CSV_HEADER):

    super(ocd_base.Organization, self).__init__()

    self.csv_header = csv_header

    self.props.update({
      'id': row_id,
      'name': name,
      'alternate_name': alternate_name,
      'identifier': identifier,
      'classification': classification,
      'geographic_area': geographic_area,
      'contact__street_1': contact__street_1,
      'contact__street_2': contact__street_2,
      'contact__city': contact__city,
      'contact__state': contact__state,
      'contact__country': contact__country,
      'officers': officers,
      'statuses': statuses,
      'committee_types': committee_types,
      'candidacy_designations': candidacy_designations,
      'notes': notes,
      'filing_year': filing_year
    })

  def to_csv_row(self):
    logger.info('yep')
    row_output = StringIO()
    writer = DictWriter(row_output, self.csv_header)
    props = self.props

    if len(props['statuses']):
      props['status_1__start'] = props['statuses'][0]['start_date']
      props['status_1__end'] = props['statuses'][0]['end_date']
      props['status_1__note'] = props['statuses'][0]['note']
      props['status_1__classification'] = props['statuses'][0]['classification']
      del props['statuses']

    if len(props['officers']):
      props['officers'] = '; '.join(
        ['%s (%s)' % (o['person'], o['title']) for o in props['officers']])

    writer.writerow(props)
    return row_output.getvalue()
