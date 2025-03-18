import requests
import pandas as pd
import logging
import os
import boto3
import zoneinfo
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from datetime import datetime

class TechMemeScraper:

    def __init__(self,local:bool) -> None:
        self.local = local
        self.logger = logging.getLogger()
        self.time_zone = zoneinfo.ZoneInfo("America/Los_Angeles")
        self.current_date = datetime.now(
                            tz=self.time_zone
                            ).date().strftime('%Y-%m-%d') 

    def get_file_path(self):
        current_dir = os.getcwd()
        lambda_dir = '/tmp'
        s3_key = f'{self.current_date}_TechMeme.json'
        if self.local:
            current_dir_path = f'{current_dir}/{self.current_date}_TechMeme.json'
            return (current_dir_path,s3_key)
        else:
            lambda_dir_path = f'{lambda_dir}/{self.current_date}_TechMeme.json'
            return (lambda_dir_path,s3_key)
            
    def get_river(self):
        try:
            request = requests.get('https://techmeme.com/river')
            return request
        except requests.exceptions.ConnectionError as err_con:
            self.logger.error(err_con) 
        except requests.exceptions.RequestException as e_excep:
            self.logger.error(e_excep)

    def get_soup(self):
        request = self.get_river()
        soup = BeautifulSoup(request.text,'html.parser')
        return soup
    
    def write_S3(self):
        try:
            s3 = boto3.client('s3')
            file_path = self.get_file_path()
            s3.upload_file(f'{file_path[0]}','techmeme-headlines',file_path[1])
            self.logger.setLevel('INFO')
            self.logger.info('into S3!')                       
        except ClientError as e:
            self.logger.error(e)
    
    def remove_whitespace(self,df:pd.DataFrame):
        for i in df.columns:
            if df[i].dtype == 'object':
                df[i] = df[i].map(str.strip())
            else:
                pass
        return df 
        
    def parse_river_data(self):    
        soup = self.get_soup()
        json_title = self.get_file_path()
        news_items=[]
        news_date = []
        for row in soup.find_all('table')[1]: #selects the most recent date on the page
            if row != '\n':
                news_items.append((row.text))
        for row in soup.find_all('h2')[0]:
            news_date.append(row)
        news_items_df = pd.DataFrame(news_items,columns=['raw_techmeme'])
        news_items_df = news_items_df.merge(pd.Series(news_date,name='news_date'), how='cross')
        news_items_df['raw_techmeme'] = news_items_df['raw_techmeme'].str.replace(
                                                                    '^.*•','',regex=True
                                                                    ) #removing the time stamp
        news_items_df[['pub_author','headline']] = news_items_df['raw_techmeme'].str.split(
                                                                    ':',expand=True,n=1
                                                                    ) # splitting the raw column
        news_items_df['pub_author'] = news_items_df['pub_author'].str.split('/',expand=False)
        news_items_df['author'] = news_items_df['pub_author'].apply(lambda x: x[0] if len(x) > 1 else None)
        news_items_df['pub'] = news_items_df['pub_author'].apply(lambda x: x[0] if len(x) == 1 else x[1])
        news_items_df = news_items_df.drop(columns=["pub_author"])
        news_items_df = self.remove_whitespace(news_items_df)
        json_str = news_items_df.to_json(json_title[0])
        return json_str
