# -*- coding: utf-8 -*-
"""
Volatility notification system - pushover, polygon.io
@author: AdamGetbags
"""

#pip OR conda install
#pip install polygon-api-client

# import modules
import pandas as pd
from polygon import RESTClient
import numpy as np
import http.client, urllib
from pushoverSecrets import token, user

# apis key from config
from polygonAPIkey import polygonAPIkey

# OR just assign your API as a string variable
# polygonAPIkey = 'apiKeyGoesHere'

# how many tickers to include in message
trimLength = 3

# empty dataframe to store log returns
stdDevsData = pd.DataFrame()

# create client and authenticate w/ API key // rate limit 5 requests per min
client = RESTClient(polygonAPIkey) # api_key is used

stockTickers = ['AAPL', 'A','AA', 'BAC', 'BA', 'C'] 

for i in stockTickers:
    
    # request daily bars
    dataRequest = client.get_aggs(ticker = i, 
                                  multiplier = 1,
                                  timespan = 'day',
                                  from_ = '2022-09-01',
                                  to = '2023-03-03')
    
    # list of polygon agg objects to DataFrame
    priceData = pd.DataFrame(dataRequest)
    
    #create Date column
    priceData['Date'] = priceData['timestamp'].apply(
                              lambda x: pd.to_datetime(x*1000000))
    

    priceData = priceData.set_index('Date')
        
    priceData['logReturns'] = np.log(priceData.close) - np.log(
        priceData.close.shift(1))
    
    #rolling stdDev window
    rollingStdDevWindow = 20
    
    #rolling stdDev log returns 
    priceData['stdDevs'] = priceData['logReturns'].rolling(
        center=False, window = rollingStdDevWindow).std()
    
    stdDevsData[i] = priceData.stdDevs

# trim data
stdDevsData = stdDevsData[rollingStdDevWindow:]

# rename index before transpose
stdDevsData.index = stdDevsData.index.rename('Tickers')
# transpose data
sortedData = stdDevsData[-1:].T
# rename column
sortedData = sortedData.rename(
    columns={sortedData.columns[0]: "stdDevs"})
# sort data
sortedData = sortedData.sort_values(by=['stdDevs'], ascending=False)
# trim data
highestVol = sortedData[:trimLength]

# make notification
msg = ''

# create line for all tickers except final ticker
for i in highestVol.index[:-1]:
    msg = msg + str(
        i + ' vol = ' + str(round(highestVol.loc[i][0],4))) + '\n'

# create line for final ticker
msg = msg + str(highestVol.index[-1] + ' vol = ' + str(
    round(highestVol.loc[highestVol.index[-1]][0],4)))

# create connection
conn = http.client.HTTPSConnection("api.pushover.net:443")

# make POST request to send message
conn.request("POST", "/1/messages.json",
  urllib.parse.urlencode({
    "token": token,
    "user": user,
    "title": "Vol Notifs",
    "message": msg,
    "url": "",
    "priority": "0" 
  }), { "Content-type": "application/x-www-form-urlencoded" })

# get response
conn.getresponse()
