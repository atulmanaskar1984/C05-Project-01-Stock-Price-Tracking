import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today() - datetime.timedelta(4)
yesterday = datetime.date.today() - datetime.timedelta(5)

print(today)
print(yesterday)

# Example of pulling the data between 2 dates from yfinance API
#data = yf.download("MSFT", start= yesterday, end= today, interval = '1h' )

## Add code to pull the data for the stocks specified in the doc
companies = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]
for company in companies:
    data = yf.download(company, start= yesterday, end= today, interval = '1h').to_json()
    print(company + " share closing price")
    #print(data)

    print(company + " - 52 Week High and Low")
    tkr_data = yf.Ticker(company)
    #print(f'Fifty two weeks High : {tkr_data.info["fiftyTwoWeekHigh"]}')
    #print(f'Fifty two weeks Low : {tkr_data.info["fiftyTwoWeekLow"]}')

    formatted_data = {"stockid": company}
    formatted_data.update(json.loads(data)["Close"])
    formatted_data.update({"52WeeksHigh": tkr_data.info["fiftyTwoWeekHigh"]})
    formatted_data.update({"52WeeksLow": tkr_data.info["fiftyTwoWeekLow"]})

    print(json.dumps(formatted_data))

    kinesis.put_record(StreamName='kinesis', Data=json.dumps(formatted_data), PartitionKey="partitionKey")
    print(f'Successfully pushed data for {company} in Kinesis')
    #for ind in data.index:
        #print(data.loc[ind].at["Close"])

## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
#for company in companies:
#    tkr_data = yf.Ticker(company)
#    print(company + " - 52 Week High and Low")
#    print(f'Fifty two weeks High : {tkr_data.info["fiftyTwoWeekHigh"]}')
#    print(f'Fifty two weeks Low : {tkr_data.info["fiftyTwoWeekLow"]}')

## Add your code here to push data records to Kinesis stream.


