import json
import requests
import pandas as pd

key = '0dac308619msh12079fba8db7bcfp10d0cajsn653ddfe6fcca'

def get_stocks():
   url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

   headers = {
      "X-RapidAPI-Key": key,
      "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
   }
   response = requests.request("GET", url, headers=headers)
   res = json.loads(response.text)
   stocks = res['stocks']
   stocks = stocks[:100]
   return stocks

def get_historical_data():
   stocks = get_stocks()
   for i in range(len(stocks)):
      url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
      querystring = {"ticker_symbol": str(stocks[i]), "years": "1", "format": "json"}
      headers = {
         "X-RapidAPI-Key": key,
         "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
      }
      response = requests.request("GET", url, headers=headers, params=querystring)
      res = json.loads(response.text)
      df = pd.DataFrame(res['historical prices'])
      df.to_csv('csv_database/'+str(stocks[i]+'.csv'))


