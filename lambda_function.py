from techmeme_scraper import TechMemeScraper

def lambda_handler(event,context):
     event = TechMemeScraper(local=False)
     event.parse_river_data()
     event.write_S3()
     return event


     
     
     
