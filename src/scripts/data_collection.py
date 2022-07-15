from bs4 import BeautifulSoup
import pandas as pd
import os, re
from urllib.request import Request, urlopen, urlretrieve
import wget
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pandas as pd
import os
import yaml
import urllib.request
from typing import Collection, List, Tuple
from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent = "geoapiExercises")

# read params
params = yaml.safe_load(open('/home/david/Elvis/Omdena/SustainLab/params.yaml'))['data_collection']

url = params['url']

# create folder to save file
data_path = os.path.join('dataflow_demo', 'metadata')
os.makedirs(data_path, exist_ok=True)

class SASBCompanydetails:
    def __init__(self, url):
      self.url = url
    
    def read_webPage(self):
      req = Request(self.url, headers={'User-Agent': 'Mozilla/5.0'})
      web_byte = urlopen(req).read()
      webpage = web_byte.decode('utf-8')
      soup = BeautifulSoup(webpage,'html.parser')
      table = soup.find("table", {"class": "table"})
      body = table.find_all("tr")
      return body

    def extract_headerColums(self,body):
      head = body[0] # 0th item is the header row
      body_rows = body[1:]
      headings = []
      for item in head.find_all("th"): # loop through all th elements
        # convert the th elements to text and strip "\n"
        item = (item.text).rstrip("\n")
        # append the clean column name to headings
        headings.append(item)
      return headings


    def extract_rows(self,body):
        all_rows = []
        body_rows = body[1:]
        for i in body_rows:
          report_link = i.find('a', href=True)
          if report_link != None:
            report_link = report_link['href']
          else:
            report_links = None
          report_data = []
          for k in i.find_all('td'):
            report_data.append(k.text)
          report_data.append(report_link)
         
          all_rows.append(report_data)
        return all_rows



    def extract_company_metadata(self):
      body = self.read_webPage()
      headings = self.extract_headerColums(body)
      headings.append('Report Link')
      rows = self.extract_rows(body)
      self.df = pd.DataFrame(data=rows,columns=headings)

      return self.df

    def download_reports(self,path=data_path):
        os.makedirs(path,exist_ok=True)
        for report in self.df['Report Link'].tolist():
          if report is not None:
            try:
              wget.download(report,out=data_path)
            except:
              os.system("curl -o f {data_path}"+ os.path.basename(report) + report)
        print(f"Downloaded files to {path}")

sasb = SASBCompanydetails(url)
df = sasb.extract_company_metadata()
df.drop_duplicates(['Company name', 'Type of Document'], keep='last').drop(['Publication Year','Report Link'],axis=1).\
                    to_csv("dataflow_demo/metadata/sasb_company_details.csv",index=False)



