import os

from datetime import date

ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')
OCD_DIRECTORY = os.path.join(DATA_DIRECTORY, 'OCD')

FEC_DATETIME_FMT = '%m%d%Y'
OCD_DATETIME_FMT = '%m/%d/%Y'
PDC_DATETIME_FORMAT = '%m/%d/%Y'

LOG_DIR = os.path.join(ROOT_DIRECTORY, 'logs')

DYNAMODB_READ_UNITS_HEAVY = 130
DYNAMODB_READ_UNITS_MINIMAL = 1
DYNAMODB_READ_BATCH_SIZE = 100

AWS_PROFILE_NAME = 'abe'
AWS_REGION = 'us-east-1'
S3_BUCKET = 'campfin'
STREAMING_CHUNK_SIZE = 4096

EMAIL_FROM_ADDRESS = 'abraham.epton@gmail.com'

STATES_IMPLEMENTED = ['FEC', 'WA']
YEAR_IMPLEMENTED_BEGIN = 2004
YEAR_IMPLEMENTED_END = date.today().year if date.today().year % 2 == 0 else date.today().year + 1
YEARS_IMPLEMENTED = [str(year) for year in range(YEAR_IMPLEMENTED_BEGIN, YEAR_IMPLEMENTED_END, 2)]

DATA_URLS = {}
for year in YEARS_IMPLEMENTED:
  cycle = year[2:]
  DATA_URLS[year] = [
    ('candidate_committee_link', 'ftp://ftp.fec.gov/FEC/%s/ccl%s.zip' % (year, cycle)),
    ('candidate_master', 'ftp://ftp.fec.gov/FEC/%s/cn%s.zip' % (year, cycle)),
    ('committee_master', 'ftp://ftp.fec.gov/FEC/%s/cm%s.zip' % (year, cycle)),
    ('committee_to_committee', 'ftp://ftp.fec.gov/FEC/%s/oth%s.zip' % (year, cycle)),
    ('committee_to_candidate', 'ftp://ftp.fec.gov/FEC/%s/pas2%s.zip' % (year, cycle)),
    ('expenditures', 'ftp://ftp.fec.gov/FEC/%s/oppexp%s.zip' % (year, cycle)),
    ('contributions', 'ftp://ftp.fec.gov/FEC/%s/indiv%s.zip' % (year, cycle))
  ]

SUPPORTED_FEC_TYPES = ['contributions']
SUPPORTED_PDC_TYPES = []