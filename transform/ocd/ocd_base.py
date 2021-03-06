import base64
import json
import logging
import os
import uuid

from csv import DictReader, DictWriter
from decimal import *
from cStringIO import StringIO
from settings import settings
from utilities import utils

logger = logging.getLogger(__name__)

class OCD(object):
  def __init__(self):
    self.csv_header = []
    self.props = {}

  def to_csv_row(self):
    row_output = StringIO()
    writer = DictWriter(row_output, self.csv_header)
    writer.writerow(self.props)
    return row_output.getvalue()


ORGANIZATION_COLUMNS = [
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
    'name': 'Alternate Names',
    'datatype': 'text',
    'csv_name': 'alternate_names'
  },
  {
    'name': 'Former Names',
    'datatype': 'text',
    'csv_name': 'former_names'
  },
  {
    'name': 'Identifiers',
    'datatype': 'text',
    'csv_name': 'identifiers'
  },
  {
    'name': 'Classification',
    'datatype': 'text',
    'csv_name': 'classification'
  },
  {
    'name': 'Parent Organization (name)',
    'datatype': 'text',
    'csv_name': 'parent_organization__name'
  },
  {
    'name': 'Parent Organization (id)',
    'datatype': 'text',
    'csv_name': 'parent_organization__id'
  },
  {
    'name': 'Geographic Area',
    'datatype': 'text',
    'csv_name': 'geographic_area'
  },
  {
    'name': 'Description (one line)',
    'datatype': 'text',
    'csv_name': 'description_short'
  },
  {
    'name': 'Description (full)',
    'datatype': 'text',
    'csv_name': 'description_full'
  },
  {
    'name': 'Date of Founding',
    'datatype': 'calendar_date',
    'csv_name': 'date_founding'
  },
  {
    'name': 'Date of Dissolution',
    'datatype': 'calendar_date',
    'csv_name': 'date_dissolution'
  },
  {
    'name': 'Image (url)',
    'datatype': 'text',
    'csv_name': 'image_url'
  },
  {
    'name': 'Contact Address',
    'datatype': 'text',
    'csv_name': 'address__raw'
  },
  {
    'name': 'Contact Address (location)',
    'datatype': 'text',
    'csv_name': 'address__location'
  },
  {
    'name': 'External link',
    'datatype': 'text',
    'csv_name': 'external_link'
  }
]

ORGANIZATION_CSV_HEADER = [h['csv_name'] for h in ORGANIZATION_COLUMNS]

ORGANIZATION_BLUEPRINT_COLS = [
  {'name': h['name'], 'datatype': h['datatype']} for h in ORGANIZATION_COLUMNS]


class Organization(OCD):
  def __init__(
      self,
      row_id='',
      name='',
      alternate_names='',
      former_names='',
      identifiers='',
      classification='',
      parent_organization__name='',
      parent_organization__id='',
      geographic_area='',
      description_short='',
      description_full='',
      date_founding='',
      date_dissolution='',
      image_url='',
      address__raw='',
      address__location='',
      external_link='',
      csv_header=ORGANIZATION_CSV_HEADER):

    super(OCD, self).__init__()

    self.csv_header = csv_header

    self.props.update({
      'id': row_id,
      'name': name,
      'alternate_names': alternate_names,
      'former_names': former_names,
      'identifiers': identifiers,
      'classification': classification,
      'parent_organization__name': parent_organization__name,
      'parent_organization__id': parent_organization__id,
      'geographic_area': geographic_area,
      'description_short': description_short,
      'description_full': description_full,
      'date_founding': date_founding,
      'date_dissolution': date_dissolution,
      'image_url': image_url,
      'address__raw': address__raw,
      'address__location': address__location,
      'external_link': external_link
    })

CANDIDACY_COLUMNS = [
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

CANDIDACY_CSV_HEADER = [h['csv_name'] for h in CANDIDACY_COLUMNS]

CANDIDACY_BLUEPRINT_COLS = [
  {'name': h['name'], 'datatype': h['datatype']} for h in CANDIDACY_COLUMNS]

class Candidacy(OCD):
  def __init__(
      self,
      row_id='',
      ballot_name='',
      ballot_year=None,
      ballot_jurisdiction='',
      csv_header=CANDIDACY_CSV_HEADER):
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
