import requests
import pandas as pd
import logging
import os
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from datetime import datetime

class TechMemeScraper:

    def __init__(self,local:bool) -> None:
        self.local = local

    def get_file_path(self):
        current_date = datetime.now()
        current_dir = os.getcwd()
        lambda_dir = '/tmp'
        s3_key = f'{current_date}_TechMeme.json'
        current_date = datetime.strftime(current_date,"%Y.%m.%d")
        if self.local:
            current_dir_path = f'{current_dir}/{current_date}_TechMeme.json'
            return (current_dir_path,s3_key)
        else:
            lambda_dir_path = f'{lambda_dir}/{current_date}_TechMeme.json'
            return (lambda_dir_path,s3_key)
            
    def get_river(self):
        try:
            request = requests.get('https://techmeme.com/river')
            return request
        except requests.exceptions.ConnectionError as err_con:
            logging.error(err_con) 
        except requests.exceptions.RequestException as e_excep:
            logging.error(e_excep)

    def get_soup(self):
        request = self.get_river()
        soup = BeautifulSoup(request.text,'html.parser')
        return soup
    
    def write_S3(self):
        try:
            s3 = boto3.client('s3')
            file_path = self.get_file_path()
            s3.upload_file(f'{file_path[0]}','techmeme-headlines',file_path[1])                       
        except ClientError as e:
            logging.error(e)
        
    def parse_river_data(self):    
        soup = self.get_soup()
        json_title = self.get_file_path()
        news_items=[]
        for row in soup.find_all('table')[1]: #selects the most recent date on the page
            if row != '\n':
                news_items.append((row.text))
        news_items_df = pd.DataFrame(news_items,columns=['raw_techmeme'])
        news_items_df['raw_techmeme'] = news_items_df['raw_techmeme'].str.replace('^.*•','',regex=True) #removing the time stamp
        news_items_df[['pub/author','headline']] = news_items_df['raw_techmeme'].str.split(':',expand=True,n=1) # splitting the raw column
        json_str = news_items_df.to_json(json_title[0])
        return json_str
