import requests
import json
import pandas as pd
import datetime as dt
from datetime import datetime
import logging
import sys
import time as ttime

logger = logging.getLogger("pre-open")

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

def download_preopen() -> bool:
    """ main function """
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

        logger.info("connecting: https://www.nseindia.com/")
        session = requests.Session()
        response = session.get("https://www.nseindia.com/", headers=headers, timeout=60)
        logger.info(f"response: {response.status_code}")
        if not response.status_code == 200:
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

        logger.info(f"connecting: https://www.nseindia.com/api/market-data-pre-open?key=FO")
        response = session.get("https://www.nseindia.com/api/market-data-pre-open?key=FO", headers=headers, timeout=60)
        logger.info(f"response: {response.status_code}")
        if not response.status_code == 200:
            return False

        with open(f"pre-open-response/{dt.datetime.now().strftime('%Y-%m-%d')}.txt", "w") as f:
            f.write(response.text)

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

        df.to_csv(f"pre-open-data/{dt.datetime.now().strftime('%Y-%m-%d')}.csv")
        with open('trade.txt', 'w') as f:
            f.write(df.iloc[0]['symbol'] + "\n")
            # f.write(df.iloc[1]['symbol'] + "\n")
        with open('pre-open-date.txt', 'w') as f:
            f.write(f'{datetime.today().date().isoformat()}')

        return True

    except Exception as ex:
        logger.exception("exception")
        return False


def trydownload():
    i = 1
    while i <= 5:
        if download_preopen():
            break
        ttime.sleep(5)
        i = i + 1


if __name__ == '__main__':
    with open('holidays.txt') as f:
        holidays = [l.strip() for l in f.readlines()]

    if datetime.today().date().isoformat() in holidays:
        print("holiday")
    else:
        trydownload()
