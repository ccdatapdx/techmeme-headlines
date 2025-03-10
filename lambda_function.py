from techmeme_scraper import TechMemeScraper

def lambda_handler(event,context):
     scraper = TechMemeScraper(local=False)
     event = scraper.parse_river_data()
     event = scraper.write_S3()
     return event
     


     
     
     
