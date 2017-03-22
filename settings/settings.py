ROOT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIRECTORY = os.path.join(ROOT_DIRECTORY, 'data')
OCD_DIRECTORY = os.path.join(DATA_DIRECTORY, 'OCD')

FEC_DATETIME_FMT = '%m%d%Y'
OCD_DATETIME_FMT = '%m/%d/%Y'
PDC_DATETIME_FORMAT = '%m/%d/%Y'

STATES_IMPLEMENTED = ['OCD', 'FEC', 'WA']