import os
import pathlib
import requests
import shutil
import datetime
import time
import math
import wget
from collections import defaultdict

VendorName = "coingecko";
VendorDataName = "marketcap";
db_file = "https://raw.githubusercontent.com/QuantConnect/Lean/master/Data/symbol-properties/symbol-properties-database.csv"
db_file_name = os.path.basename(db_file)

class CoinGeckoMarketCapDataDownloader:
    def __init__(self, destinationFolder, db_file, apiKey = None):
      self.destinationFolder = destinationFolder
      self.symbol_id = self.preprocess(db_file)
    
      pathlib.Path(self.destinationFolder).mkdir(parents=True, exist_ok=True)

    def Run(self):

      for symbol in self.symbol_id:
        coin_id = self.symbol_id[symbol]
        filename = os.path.join(self.destinationFolder, f'{symbol}.csv')
        print(f'Processing coin: {symbol}')
        trial = 5
        total_time = 1.4
        standard_sleep = 0.15

        while trial != 0:
          try:
            req_start_time = time.time()
            coin_history = self.HttpRequester(f"{coin_id}/market_chart?vs_currency=usd&days=max&interval=daily")['market_caps']
            req_end_time = time.time()
            req_time = req_end_time - req_start_time
            time.sleep(max(total_time - req_time, standard_sleep))

            if len(coin_history) == 0:
              print(f'No data for: {symbol}')
              break

            lines = []

            for data_point in coin_history:
              unix_timestamp = data_point[0]
              date = datetime.datetime.fromtimestamp(unix_timestamp/1000.0).strftime("%Y-%m-%d")
              market_cap = data_point[1]

              lines.append(','.join([date, str(market_cap)]))

            with open(filename, 'w') as coin_file:
              coin_file.write('\n'.join(lines))

            print(f'Finished processing {symbol}')
            break

          except Exception as e:
            print(f'{e} - Failed to parse data for {symbol} - Retrying')
            time.sleep(2)
            trial -= 1


    def preprocess(self, db_file):

      print('Creating a map of list from API')
      coins = self.HttpRequester("list")

      all_symbol_id = defaultdict(list)

      for coin in coins:
        all_symbol_id[coin['symbol']].append(coin['id'])

      print('Map created from API')

      print('Downloading db_file and preprocessing it')
      if(os.path.exists(db_file_name)):
        os.remove(db_file_name)
      wget.download(db_file)
      req_symbols = set()
      with open(f"{db_file_name}", 'r') as f:
        lines = f.readlines()

      for line in lines:
        if line[0] == '#' or line == '\n':
          continue
        line  = line.strip()
        line = line.split(',')
        if(len(line) < 5):
          continue
        if line[2] == 'crypto':
          curr_symbol = line[1][:-len(line[4])].lower()
          if curr_symbol in all_symbol_id.keys():
            req_symbols.add(curr_symbol)
      print('db_file preprocessed')


      print('Creating the final map of symbol->id')

      req_symbol_id = {}

      for symbol in req_symbols:
        print(f'Pre-Processing for {symbol}')
        coin_ids = all_symbol_id[symbol]
        if(len(coin_ids) == 1):
          req_symbol_id[symbol] = coin_ids[0]
          print(f'Finished Pre-Processing for {symbol}')
          continue
        max_marketcap = 0
        req_coin_id = coin_ids[0]
        for coin_id in coin_ids:
          print(f'Pre-Processing for {symbol} coin : {coin_id}')
          trial = 5
          total_time = 1.4
          standard_sleep = 0.15
          while trial != 0:
            try:
              start_time = time.time()
              history = self.HttpRequester(f"{coin_id}/market_chart?vs_currency=usd&days=max&interval=daily")['market_caps']
              req_time = time.time() - start_time
              time.sleep(max(total_time - req_time, standard_sleep))
              latest_marketcap = history[-1][1]
              if(latest_marketcap >= max_marketcap):
                max_marketcap = latest_marketcap
                req_coin_id = coin_id
              print(f'Finished Pre-Processing for {symbol} coin : {coin_id}')
              break

            except Exception as e:
              print(f'{e} - Failed to preprocess data for the {symbol} coin : {coin_id}')
              time.sleep(2)
              trial -= 1
        req_symbol_id[symbol] = req_coin_id
        print(f'Finished Pre-Processing for {symbol}')

      return req_symbol_id



    def HttpRequester(self, url):       
        base_url = 'https://api.coingecko.com/api/v3/coins'
        return requests.get(f'{base_url}/{url}').json()


if __name__ == "__main__":
  start_time = time.time()
  destinationDirectory = f"output/alternative/{VendorName}/{VendorDataName}"
  instance = CoinGeckoMarketCapDataDownloader(destinationDirectory, db_file)
  instance.Run()
  time_taken = time.time() - start_time
  print("Total time taken to run in minutes : ", time_taken//60)
