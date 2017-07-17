import logging
import os

from settings import settings

# Setting this up before other imports so everything logs correctly
logging.basicConfig(
  filename=os.path.join(settings.LOG_DIR, 'rebuild.log'),
  filemode='a',
  level=logging.INFO)

from orchestration import alerts, utils

logger = logging.getLogger(__name__)

def orchestrate():
  logger.info('Starting cleanup')
  utils.cleanup_data_dirs()

  logger.info('Starting FEC')
  utils.download_and_process_fec_data()

  logger.info('Starting WA')
  utils.download_and_process_wa_data()

  logger.info('Starting upload')
  utils.upload_to_s3()
  utils.upload_to_socrata()

  logger.info('Starting alerts processing')
  alerts.process_alerts()

  logger.info('Done with everything')

if __name__ == '__main__':
  orchestrate()
