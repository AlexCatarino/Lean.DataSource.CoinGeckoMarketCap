import os
import pathlib
import requests
import shutil
import datetime
import time
import math


VendorName = "coingecko";
VendorDataName = "marketcap";


class CoinGeckoMarketCapDataDownloader:
    def __init__(self, destinationFolder, apiKey = None):
        self.destinationFolder = os.path.join(destinationFolder, VendorDataName)
        self.universeFolder = os.path.join(self.destinationFolder, "universe")

        if os.path.exists(self.universeFolder):
            shutil.rmtree(self.universeFolder)
    
        pathlib.Path(self.destinationFolder).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.universeFolder).mkdir(parents=True, exist_ok=True)

    def Run(self):

        coins = self.HttpRequester("list")
    
        for coin in coins:
          coin_id = coin['id']
          filename = os.path.join(self.destinationFolder, f'{coin_id.lower()}.csv')
          print(f'Processing coin_id: {coin_id}')
          trial = 5
          total_time = 1.4

          while trial != 0:
            try:
              start_time = time.time()
              coin_history = self.HttpRequester(f"{coin_id}/market_chart?vs_currency=usd&days=max&interval=daily")['market_caps']
              end_time = time.time()
              req_time = end_time - start_time
              time.sleep(total_time - req_time)

              if len(coin_history) == 0:
                print(f'No data for: {coin_id}')
                break

              lines = []

              for data_point in coin_history:
                unix_timestamp = data_point[0]
                date = datetime.datetime.fromtimestamp(unix_timestamp/1000.0).strftime("%Y-%m-%d")
                market_cap = data_point[1]

                lines.append(','.join([date, str(market_cap)]))

                with open( os.path.join(self.universeFolder, f'{date.replace("-", "")}.csv'), 'a') as universe_file:
                  universe_file.write(f'{coin_id},{market_cap}\n')

              with open(filename, 'w') as coin_file:
                coin_file.write('\n'.join(lines))

              print(f'Finished processing {coin_id}')
              break

            except Exception as e:
              print(f'{e} - Failed to parse data for {coin_id} - Retrying')
              time.sleep(2)
              trial -= 1


    def HttpRequester(self, url):       
        base_url = 'https://api.coingecko.com/api/v3/coins'
        return requests.get(f'{base_url}/{url}').json()


if __name__ == "__main__":

    destinationDirectory = f"../output/alternative/{VendorName}"
    instance = CoinGeckoMarketCapDataDownloader(destinationDirectory);
    instance.Run()