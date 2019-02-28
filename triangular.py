import os, sys
try:                                            # if running in CLI
    cur_path = os.path.abspath(__file__)
except NameError:                               # if running in IDE
    cur_path = os.getcwd()

while cur_path.split('/')[-1] != 'binance':
    cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))    
sys.path.insert(1, os.path.join(cur_path, 'lib', 'python3.7', 'site-packages'))


import requests
from lxml import html
from itertools import permutations
from binance.enums import *
from _connections import binance_connection, db_connection
from datetime import datetime, timedelta
import cryptocompare


psql = db_connection('psql')

page = requests.get('https://info.binance.com/en/all')
tree = html.fromstring(page.content)
coins = [{'name':i, 'price':j} for i,j in zip(tree.xpath('//*[@id="__next"]/div/main/div/div/div/div[2]/div[1]/table/tbody/tr/td[2]/div/div/span[1]/text()')[:50], tree.xpath('//*[@id="__next"]/div/main/div/div/div/div[2]/div[1]/table/tbody/tr/td[3]/div/div/text()')[:50])]

client = binance_connection()
prices = client.get_all_tickers()

stable_coin = 'PAX'
stable_len = len(stable_coin)
stable_sells = {i['symbol'][:-stable_len]:float(i['price']) for i in prices if i['symbol'][stable_len:] == stable_coin}
stable_buys = {i['symbol'][stable_len:]:float(i['price']) for i in prices if i['symbol'][:-stable_len] == stable_coin}

def floor_dt(dt, delta):
    return dt + (datetime.min - dt) % delta

last_time = floor_dt(datetime.now(), timedelta(minutes=-30))
for coin_name in [i['name'] for i in coins]:
    

for time_mult in range(0, 1440):
    last_time - timedelta(minutes = -30)*time_mult
    asdfa
    
    
def format_pair(market, price):
#    market, price = i['symbol'], i['price']
    matched_coins = [i for i in coins if i['name'] in market]
    paired_coins = False
    
    if len(matched_coins) < 2 or len([list(i) for i in permutations([i['name'] for i in matched_coins], 2) if ''.join(list(i)) == market]) == 0:
        return False
    if len(matched_coins) > 2:
        rematched_coins = [list(i) for i in permutations([i['name'] for i in matched_coins], 2) if ''.join(list(i)) == market]
        if len(rematched_coins) != 1:
            raise Exception('Too many possible matches')
        else:
            matched_coins = [i for i in matched_coins if i['name'] in rematched_coins[0]]    
    matched_coin_locations = [market.find(i['name']) for i in matched_coins]
    paired_coins = [x for _,x in sorted(zip(matched_coin_locations, matched_coins))]
    
    coin_pair = {'sell_coin': {'name': paired_coins[0]['name'], 'price': float(paired_coins[0]['price'].replace('$', ''))},
    'buy_coin': {'name': paired_coins[1]['name'], 'price': float(paired_coins[1]['price'].replace('$', ''))} }
    coin_pair['price_ratio'] = coin_pair['sell_coin']['price'] / coin_pair['buy_coin']['price']
    coin_pair['exchange_ratio'] = float(price)
    coin_pair['price_exchange_ratio'] = coin_pair['exchange_ratio'] / coin_pair['price_ratio']
    coin_pair['arbitrage_opportunity'] = (coin_pair['price_exchange_ratio'] - 1) * .999
    return(coin_pair)
    
opportunities = {}
for i in prices:
    opp_mrkt = format_pair(i['symbol'], i['price'])
    if opp_mrkt:
        opportunities[i['symbol']] = opp_mrkt
        
sorted_opps = sorted(opportunities, key=lambda x: opportunities[x]['arbitrage_opportunity'], reverse = True)




dict_labels = {0: 'open_time',
                 1: 'open',
                 2: 'high',
                 3: 'low',
                 4: 'close',
                 5: 'volume',
                 6: 'close_time',
                 7: 'quote_asset_volume',
                 8: 'number_of_trades',
                 9: 'taker_buy_base_asset_volume',
                 10: 'taker_buy_quote_asset_volume'}

def list_to_dict(_list):
    _dict = {}
    for list_num, (val) in enumerate(_list):
        if list_num > 10:
            continue
        _dict[dict_labels[list_num]] = val
    return(_dict)
            

def format_kline(data):
    _data = list_to_dict(data)
    for key in _data.keys():
        if key in ['open_time', 'close_time']:
            _data[key] =  datetime.fromtimestamp(_data[key]/1000.0)
        else:
            _data[key] = float(_data[key])
    return(_data)
    

def process_kline(_klines):
    hist_data = []
    for kline in _klines:
        formed_kline = format_kline(kline)
#        start = formed_kline['open_time']
#        formed_kline.pop('open_time')
#        formed_kline.pop('close_time')
        hist_data.append(formed_kline)
    return(hist_data)
    

def mkt_hist_klines(hist_klines, mkt_name):
    return({mkt_name: process_kline(hist_klines)})
    
    
for mkt in sorted_opps:
    klines = client.get_historical_klines(mkt, '30m', "1 month ago UTC")
    if len(klines) > 0:
        dfh
        mongo.client.insert_one(mkt_hist_klines(klines, mkt))



 
last_time = floor_dt(datetime.now(), timedelta(minutes=-30))


for time_mult in range(0, 1440):
    last_time - timedelta(minutes = -30)*time_mult
    asdfa

requests.get('https://min-api.cryptocompare.com/data/pricehistorical?fsym=BTC&tsyms=USD&ts=1452680400').content



client.get_symbol_info('BTC')





cryptocompare.get_historical_price_hour('BTC', curr='USD')

datetime.datetime.fromtimestamp(1551193200)



cryptocompare.get_coin_list(format=False)
   
datetime.datetime.fromtimestamp(1551186000000/1000.0)






opportunities[mkt]

#order = client.create_test_order(
#    symbol=mkt,
#    side=SIDE_BUY,
#    type=ORDER_TYPE_LIMIT,
#    timeInForce=TIME_IN_FORCE_GTC,
#    quantity=100,
#    price='0.00001')

opportunities[sorted_opps[0]]
