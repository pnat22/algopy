import requests
import json
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import logging
import sys
import time as ttime
import urllib.parse
import os
import zipfile

logger = logging.getLogger("nseindia")
s_handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler('algo.log')
s_handler.setLevel(logging.DEBUG)
f_handler.setLevel(logging.DEBUG)

s_format = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')

s_handler.setFormatter(s_format)
f_handler.setFormatter(f_format)

logger.addHandler(s_handler)
logger.addHandler(f_handler)
logger.setLevel(logging.DEBUG)

class NseIndia:
    def __init__(self) -> None:
        self.session = self.get_nse_session()

    def get_headers(self, referer = ""):
        headers = {
            "accept": "*/*",
            "accept-language": "en-GB,en;q=0.9",
            "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "upgrade-insecure-requests": "1",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        }
        if referer != "":
            headers["Referer"] = referer
        return headers

    def get_nse_session(self):
        # logger.info("connecting: https://www.nseindia.com/")
        session = requests.Session()
        headers = self.get_headers()
        response = session.get("https://www.nseindia.com/", headers=headers, timeout=60)
        # logger.info(f"response: {response.status_code}")
        if not response.status_code == 200:
            return None
        else:
            return session

    def get_preopen(self) -> pd.DataFrame:
        """ main function """
        try:
            url = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
            # logger.info(f"connecting: {url}")
            headers = self.get_headers("https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market")
            response = self.session.get(url, headers=headers, timeout=60)
            # logger.info(f"response: {response.status_code}")

            if not response.status_code == 200:
                return None

            jdata = json.loads(response.text)

            data = []
            for d in jdata['data']:
                data.append({
                    'symbol': d['metadata']['symbol'],
                    'lastPrice': d['metadata']['lastPrice'],
                    'change': d['metadata']['change'],
                    'pChange': d['metadata']['pChange'],
                    'previousClose': d['metadata']['previousClose'],
                    'totalTradedVolume': d['detail']['preOpenMarket']['totalTradedVolume'],
                    'finalPrice': d['detail']['preOpenMarket']['finalPrice'],
                    'finalQuantity': d['detail']['preOpenMarket']['finalQuantity'],
                    'lastUpdateTime': d['detail']['preOpenMarket']['lastUpdateTime'],
                    'totalSellQuantity': d['detail']['preOpenMarket']['totalSellQuantity'],
                    'totalBuyQuantity': d['detail']['preOpenMarket']['totalBuyQuantity'],
                })

            df: pd.DataFrame
            df = pd.DataFrame(data, columns=['symbol', 'lastPrice', 'change', 'pChange', 'previousClose', 'totalTradedVolume',
                                    'finalPrice', 'finalQuantity', 'lastUpdateTime', 'totalSellQuantity', 'totalBuyQuantity'])

            return df
        except Exception as ex:
            logger.exception("exception")
            return None

    def get_intraday_chart(self, symbol) -> pd.DataFrame:
        """ main function """
        try:
            enc_symbol = urllib.parse.quote(symbol)
            url = f"https://www.nseindia.com/api/chart-databyindex?index={enc_symbol}EQN"
            # logger.info(f"connecting: {url}")
            headers = self.get_headers("https://www.nseindia.com/api/chart-databyindex?index={enc_symbol}")
            response = self.session.get(url, headers=headers, timeout=60)
            # logger.info(f"response: {response.status_code}")

            if not response.status_code == 200:
                return None

            jdata = json.loads(response.text)
            df = pd.DataFrame([{'time': datetime.utcfromtimestamp(x[0]/1000), 'price': x[1]} for x in jdata['grapthData']])
            df.set_index('time', inplace=True)
            return df
        except Exception as ex:
            logger.error(ex)
            return None


    def get_fno_stocks():
        try:

            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en-GB,en;q=0.9",
                "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }

            session = requests.Session()
            response = session.get("https://www.nseindia.com/", headers=headers, timeout=60)

            if not response.status_code == 200:
                print("failed to fetch nseindia")
                print(response.status_code)
                return False

            headers = {
                "accept": "*/*",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "Referer": "https://www.nseindia.com/market-data/live-equity-market",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }

            response = session.get("https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O", headers=headers, timeout=60)

            if not response.status_code == 200:
                print(f"failed to fetch")
                print(response.status_code)
                return False

            jdata = json.loads(response.text)

            df = pd.DataFrame(columns=['symbol', 'pchange'])

            data = []
            for d in jdata['data']:
                opchange = ((float(d['lastPrice']) - float(d['open'])) / float(d['open'])) * 100
                data.append({'symbol': d['symbol'], 'pchange': d['pChange'], 'opchange': opchange })
            df = pd.DataFrame(data)
            df['abspct'] = df['pchange'].abs()
            df['absopct'] = df['opchange'].abs()
            df = df.sort_values(by=['abspct'], ascending=False)
            lst = (df['symbol'].tolist())

            return lst
            # return data
        except Exception as ex:
            print(ex)
            return None

    def download_and_save_from_nse(self, url, folder) -> bool:
        try:
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en-GB,en;q=0.9",
                "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }

            session = requests.Session()
            response = session.get("https://www.nseindia.com/", headers=headers, timeout=60)

            if not response.status_code == 200:
                print("failed to fetch nseindia")
                print(response.status_code)
                return False

            headers = {
                "accept": "*/*",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "Referer": "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market",
                "Referrer-Policy": "strict-origin-when-cross-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }

            response = session.get(url, headers=headers, timeout=60)

            if not response.status_code == 200:
                print(f"failed to fetch given {url}")
                print(response.status_code)
                return False

            filename = 'fo'
            with open(f'{folder}/{filename}.zip', 'wb') as file:
                file.write(response.content)

            namelist = []
            with zipfile.ZipFile(f'{folder}/{filename}.zip', 'r') as zip_ref:
                namelist = zip_ref.namelist()
                zip_ref.extractall(folder)
            os.remove(f'{folder}/{filename}.zip')

            return f'{folder}/{namelist[0]}'
        except Exception as ex:
            print(ex)
            return False

    def get_fo_bhav_copy_url(self, date: datetime):
        formatted_date = date.strftime("%d%b%Y").upper()
        month = date.strftime("%b").upper()
        url = "https://nsearchives.nseindia.com/content/historical/DERIVATIVES"
        url = url + f"/{date.year}/{month}/fo{formatted_date}bhav.csv.zip"
        return url

    def get_eq_bhav_copy_url(self, date: datetime):
        formatted_date = date.strftime("%d%b%Y").upper()
        month = date.strftime("%b").upper()
        url = "https://nsearchives.nseindia.com/content/historical/EQUITIES"
        url = url + f"/{date.year}/{month}/cm{formatted_date}bhav.csv.zip"
        return url

    def download_eq_bhav_copy(self, date: datetime, folder):
        url = self.get_eq_bhav_copy_url(date)
        filename = self.download_and_save_from_nse(url, folder)
        return filename


    def save_file_to_db(self, connection, filename):
        cursor = connection.cursor()
        try:
            # save_file_to_db(cursor, "data-algotest/NIFTY 2024-01-05 09_16_00.json", datetime(2024,1,5,9,16,0))
            df = pd.read_csv(filename)
            df = df[df['SERIES'] == 'EQ']
            for index, row in df.iterrows():
                self.save_record_to_db(cursor, datetime.strptime(row['TIMESTAMP'], "%d-%b-%Y"), row['SYMBOL'],
                                row['OPEN'], row['HIGH'], row['LOW'], row['CLOSE'],
                                row['PREVCLOSE'], row['LAST'], row['TOTTRDQTY'], row['TOTTRDVAL'], row['TOTALTRADES'])
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"Error: {e}")
        finally:
            cursor.close()

    def get_fnolist_from_file():
        fnolist = []
        with open('fnolist.txt', 'r') as file:
            fnolist = [s.strip() for s in file.readlines() if s.strip()]
        return fnolist

if __name__ == '__main__':
    # if len(sys.argv) > 1 and sys.argv[1] == "eod-update":
    #     print("updating eod data")
    #     nse = NseIndia()
    #     nse.update_eod_data()

    if len(sys.argv) > 1 and sys.argv[1] == "fno-stocks":
        print("fetching fno stocks")
        fnostocks = NseIndia.get_fno_stocks()
        print(fnostocks)

