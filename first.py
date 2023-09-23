# -*- coding: utf-8 -*-

#beautifulsoup for web scraping.

import requests  				#from urllib.request import urlopen
import re 						#for regular expression
from bs4 import BeautifulSoup 	#import urllib.parse
import pandas as pd 			#importing pandas to get the data in dataframe


# request allows to get the page and read it in python.

r = requests.get("http://www.football-data.co.uk/spainm.php",headers={'User-Agent': 'Mozilla/5.0'})

#reads the cocntents of the html page
soup = BeautifulSoup(r.content, features="html.parser")

 
allsearch = ''

# reading all the links on the selected page.

for link in soup.find_all('a'):
    mysearch= link.get('href')
    allsearch = allsearch+' '+mysearch

#spliting to get the array of the strings.
y = allsearch.split()
#print(y)

#extracting only division 1 games-which is senior level games.

z = [list(x for x in y if re.search("^mmz.*.SP1.csv$",str(x)))]

#indexing to get back the list from list of list.
z=z[0]

base_url = 'http://www.football-data.co.uk/'

#empty list to append / update with data as we read.
complete_url = []

#converting the abstract links to primary form so that they can be read.
for i in (z):
  u = base_url +  str(i) 
  complete_url.append(u)
  
  
 
#using only 1st 10 years of the data from spanish soccer. The data goes back to 1992 but doen't provide much information in terms 
#of consistency with the format followed. The format changes after 2005, which is why we are selecting only 10 years data.

req_url = complete_url[10:11]
print(req_url)


#using pandas module to finally read the data as dataframe from the links.

readings = pd.DataFrame()
for m in req_url:
    reader = pd.read_csv(m,sep=',', header=0,error_bad_lines=False)
    readings = readings.append(reader)

#renaming some of the column names as required.
readings.rename(columns={'BbMx>2.5':'BbMxTwo', 'BbAv>2.5':'BbAvTwo','BbMx<2.5':'BbMxLess','BbAv<2.5':'BbAvLess'}, inplace=True)


"""
converting the dataframe to dictionary which is the "required" and "eligible" format allowed in mongoDB. Dictionaries are most 
closely how the data are accepted in mongoDB, that is BSON format.
"""

readings_df = readings.to_dict('records')
#print(readings_df)  


