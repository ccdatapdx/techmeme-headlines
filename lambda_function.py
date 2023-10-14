from techmeme_scraper import TechMemeScraper
import logging

logger = logging.getLogger()


def lambda_handler(event,context):
     logger.setLevel('INFO')
     event = TechMemeScraper(local=False)
     event.parse_river_data()
     event.write_S3()
     logger.info('into S3!')
     
     
