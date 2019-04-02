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
#from binance.enums import *
from _connections import binance_connection, db_connection
from datetime import datetime, timedelta
#import cryptocompare
import pandas as pd
from pg_tables import create_tables
import numpy as np
from progress_bar import progress
import time as tm
import operator 
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import Features, SentimentOptions, EmotionOptions
import re
import tweepy
import _config

def pg_create_table(cur, table_name):  
#    cur, table_name = _psql, 
    try:
        # Truncate the table first
        for script in create_tables[table_name]:
            cur.execute(script)
            cur.execute("commit;")
        print("Created {}".format(table_name))
        
    except Exception as e:
        print("Error: {}".format(str(e)))
        
        
def pg_query(cur, query):
    cur.execute(query)
    data = pd.DataFrame(cur.fetchall())
    return(data) 
    
def pg_insert(cur, script):
    try:
        cur.execute(script)
        cur.execute("commit;")
        
    except Exception as e:
        print("Error: {}".format(str(e)))
        raise(Exception)
        

def _det_current_(_psql, field):
#    try: 
    _data = pg_query(_psql.client, 'select %s_id, symbol from binance.%s;' % (field[:-1], field))
#    except:
#        _psql.reset_db_con()
#        pg_create_table(_psql.client, field)
#        _data = pg_query(_psql.client, 'select %s_id, symbol from binance.%s;' % (field[:-1], field))
    if len(_data) > 0:
        current_ = set(_data[1].values)
        next_idx = np.max(_data[0]) + 1
    else:
        next_idx = 0
        current_ = set([])
    return(next_idx, current_, _psql)
    
def validate_pair(market, _cur_coins):
    matched_coins = [i for i in _cur_coins if i in market]
    paired_coins = False
    
    if len(matched_coins) < 2 or len([list(i) for i in permutations([i for i in matched_coins], 2) if ''.join(list(i)) == market]) == 0:
        return False
    if len(matched_coins) > 2:
        rematched_coins = [list(i) for i in permutations([i for i in matched_coins], 2) if ''.join(list(i)) == market]
        if len(rematched_coins) != 1:
            raise Exception('Too many possible matches')
        else:
            matched_coins = [i for i in matched_coins if i in rematched_coins[0]]    
    matched_coin_locations = [market.find(i) for i in matched_coins]
    paired_coins = [x for _,x in sorted(zip(matched_coin_locations, matched_coins))]
    coin_pair = {'sell_coin': paired_coins[0], 'buy_coin': paired_coins[1]}
    return(coin_pair)
    
def pop_coins(psql, coins):
#    psql = PSQL
    print('Updating coins')
    nxt_coin, cur_coins, psql = _det_current_(psql, 'coins')
    total_coins = len(coins)
    for coin_num, (coin) in enumerate([str(i['name']) for i in coins]):
        progress(coin_num, total_coins, status = coin)

        if coin not in cur_coins:
            script = "insert into binance.%s (coin_id, symbol) VALUES (%i, '%s')" % ('coins', nxt_coin, coin)
            pg_insert(psql.client, script)
            cur_coins.add(coin)
            nxt_coin += 1
    print('\n')

def pop_markets(psql, prices):
#    psql = PSQL
    print('Updating markets')
    _, cur_coins, psql = _det_current_(psql, 'coins')
    nxt_market, cur_markets, psql = _det_current_(psql, 'markets')
    total_markets = len(prices)
    for market_num, (market) in enumerate([str(i['symbol']) for i in prices]):
        progress(market_num, total_markets, status = market)

        if market not in cur_markets:
            formed_market = validate_pair(market, cur_coins)
            if formed_market:
                script = "insert into binance.%s (market_id, symbol, sell_coin, buy_coin) VALUES (%i, '%s', '%s', '%s')" % ('markets', nxt_market, market, formed_market['sell_coin'], formed_market['buy_coin'])
                pg_insert(psql.client, script)
                cur_markets.add(market)
                nxt_market += 1
    print('\n')


def _det_current_times_(_psql):
#    _psql = PSQL
#    try: 
    _data = pg_query(_psql.client, 'select time_id, full_time from binance.times as t1 where t1.full_time = (select max(t2.full_time) from binance.times as t2);')
#    except:
#        _psql.reset_db_con()
#        pg_create_table(_psql.client, 'times')
#        _data = pg_query(_psql.client, 'select time_id, full_time from binance.times as t1 where t1.full_time = (select max(t2.full_time) from binance.times as t2);')
    if len(_data) > 0:
        current_ = datetime.strptime(str(_data[1].iloc[0]), '%Y-%m-%d %H:%M:%S') 
        next_idx = _data[0].values[0] + 1
    else:
        next_idx = 0
        current_ = False
    return(next_idx, current_, _psql)


def floor_dt(dt, delta):
    return dt + (datetime.min - dt) % delta


def pop_times(psql, _last_time):
#    psql, _last_time = PSQL, last_time
    print('Updating times')
    nxt_time, cur_max_time, psql = _det_current_times_(psql)
    
#    for time_mult in range(0, 1440):
    insert_times = []
    if isinstance(cur_max_time, bool):
        for time_mult in range(0, 1440):
            insert_times.append(_last_time + timedelta(minutes = -30)*time_mult)
    else:
        for time_mult in range(0, 1440):
            if cur_max_time >= _last_time + timedelta(minutes = -30)*time_mult:
                break
            else:
                insert_times.append(_last_time + timedelta(minutes = -30)*time_mult)
                
    total_times = len(insert_times)
    for time_num, (time) in enumerate(reversed(insert_times)):
        progress(time_num, total_times, status = time)

        script = "insert into binance.times (time_id, q_date, q_time, full_time) VALUES (%i, '%s', '%s', '%s')" % (nxt_time, time.date(), time.time(), time)
        pg_insert(psql.client, script)
        nxt_time += 1

    print('\n')


def _det_current_exchanges(_psql):
#    _psql = PSQL
#    try: 
    _data = pg_query(_psql.client, 'select ex_market_id, max(ex_time_id) from binance.exchanges group by ex_market_id')
#    except:
#        _psql.reset_db_con()
#        pg_create_table(_psql.client, 'exchanges')
#        _data = pg_query(_psql.client, 'select ex_market_id, max(ex_time_id) from binance.exchanges group by ex_market_id')
    if len(_data) > 0:
        current_ = {k:v for k,v in _data.values}
    else:
        current_ = {}
    return(current_, _psql)
    

def _det_current_prices(_psql):
#    _psql = PSQL
#    try: 
    _data = pg_query(_psql.client, 'select ex_market_id, max(ex_time_id) from binance.exchanges group by ex_market_id')
#    except:
#        _psql.reset_db_con()
#        pg_create_table(_psql.client, 'prices')
#        _data = pg_query(_psql.client, 'select ex_market_id, max(ex_time_id) from binance.exchanges group by ex_market_id')
    if len(_data) > 0:
        current_ = {k:v for k,v in _data.values}
    else:
        current_ = {}
    return(current_, _psql)
    
    
    
#pg_create_table(PSQL.client, 'exchanges')

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
#    data = klines[0]
    _data = list_to_dict(data)
    for key in _data.keys():
        if key in ['open_time', 'close_time']:
            _data[key] =  datetime.fromtimestamp(_data[key]/1000.0)
        else:
            _data[key] = float(_data[key])
    return(_data)
    

def process_kline(_klines, _time_conv, _bootstrap_time):
#    _klines, _time_conv = klines, time_conv
    hist_data = []
    for kline in _klines:
        formed_kline = format_kline(kline)
#        start = formed_kline['open_time']
        if formed_kline['open_time'] in _time_conv.keys():
            formed_kline['open_time'] = _time_conv[formed_kline['open_time']]
            if _bootstrap_time:
                if formed_kline['open_time'] <= _bootstrap_time:
                    continue
    #        formed_kline.pop('open_time')
            formed_kline.pop('close_time')
            hist_data.append(formed_kline)
#        else:
#            adsfaf
    return(hist_data)
    

def pop_exchanges(psql, _last_time, _prices, mrkt_conv, current_mkt_data, time_conv, binance):
#    psql, _last_time, _prices = PSQL, last_time, prices
    print('Updating exchanges')
    total_exchanges = len(_prices)
    
    for exchange_num, (mkt) in enumerate([i['symbol'] for i in _prices]):
        progress(exchange_num, total_exchanges, status = mkt)
        bootstrap_time = False
        if mkt not in mrkt_conv.keys():
            continue
        if mrkt_conv[mkt] in current_mkt_data.keys():
            if current_mkt_data[mrkt_conv[mkt]] == time_conv[_last_time]:
                continue
            else:
                bootstrap_time = current_mkt_data[mrkt_conv[mkt]]
        klines = binance.get_historical_klines(mkt, '30m', "2 weeks ago UTC")
        if len(klines) > 0:
            mkt_hist = process_kline(klines, time_conv, bootstrap_time)
            for hist in mkt_hist:
                script = "insert into binance.exchanges (ex_time_id, ex_market_id, open, high, low, \
                close, volume, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, \
                taker_buy_quote_asset_volume) \
                VALUES (%i, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (hist['open_time'], mrkt_conv[mkt], hist['open'], hist['high'], hist['low'], hist['close'], hist['volume'], hist['quote_asset_volume'], hist['number_of_trades'], hist['taker_buy_base_asset_volume'], hist['taker_buy_quote_asset_volume'])
                pg_insert(psql.client, script)   
    print('\n')
                

def conversion_data(psql):
    all_times = pg_query(psql.client, 'select time_id, full_time from binance.times order by full_time desc limit 1440')
    time_conv = {datetime.strptime(str(v), '%Y-%m-%d %H:%M:%S'):k  for k,v in all_times.values}
    all_mkts = pg_query(psql.client, 'select market_id, symbol from binance.markets')
    mrkt_conv = {v:k for k,v in all_mkts.values}
    current_mkt_data, psql = _det_current_exchanges(psql)
    return(current_mkt_data, mrkt_conv, time_conv)
    

def pop_prices(psql, time_conv, coins):
#    psql, time_conv, coins = PSQL, TIME_CONV, COINS
    print('Updating prices')
    all_prices = pg_query(psql.client, 'select pr_symbol, max(pr_time_id) from binance.prices group by pr_symbol')
    price_conv = {k:v for k,v in all_prices.values}
    total_prices = len(coins)
    for price_num, (coin) in enumerate([i['name'] for i in coins]):    
        progress(price_num, total_prices, status = coin)
        for time in list(time_conv.keys()):
            if coin in price_conv.keys():
                if price_conv[coin] >= time_conv[time]:
                    break
            try:
                price_data = requests.get('https://min-api.cryptocompare.com/data/pricehistorical?fsym=%s&tsyms=USD&ts=%s'%(coin, time.strftime("%s"))).json()
            except:
                continue
            script = "insert into binance.prices (pr_symbol, pr_time_id, usd) VALUES \
            ('%s', %i, %s);" % (coin, time_conv[time], price_data[coin]['USD'])
            pg_insert(psql.client, script)   
    print('\n')
       

def cap_market(coin_name_1, coin_name_2, binance):
#    coin_name_1, coin_name_2, binance = coin1, coin2, BINANCE_2
#    coin_name_2 = 'BNB'
#    coin_name_3 = 'BTC'
    
    coin1eth_depth = binance.get_order_book(symbol='%sETH' % (coin_name_1))
    coin1coin2_depth = binance.get_order_book(symbol='%s%s' % (coin_name_1, coin_name_2))
    if coin_name_2 in ['BNB', 'TUSD']:
        ethcoin2_depth = binance.get_order_book(symbol='%sETH' % (coin_name_2))
    else:
        ethcoin2_depth = binance.get_order_book(symbol='ETH%s' % (coin_name_2))
#    if coin_name_3:
#        coin2coin3_depth = binance.get_order_book(symbol='%s%s' % (coin_name_2, coin_name_3))
#        if coin_name_3 in ['BNB', 'TUSD']:
#            ethcoin3_depth = binance.get_order_book(symbol='%sETH' % (coin_name_3))
#        else:
#            ethcoin3_depth = binance.get_order_book(symbol='ETH%s' % (coin_name_3))        
#        
    
    eth_available = binance.get_asset_balance(asset='ETH')
    
    buy_prices = {}
    buy_prices[coin_name_1] = {'ETH': {'price': [float(coin1eth_depth['asks'][0][0]), float(coin1eth_depth['asks'][1][0])], 'volume': [float(coin1eth_depth['asks'][0][1]), float(coin1eth_depth['asks'][1][1])]}}
    buy_prices[coin_name_1][coin_name_2] = {'price': [float(coin1coin2_depth['asks'][0][0]), float(coin1coin2_depth['asks'][1][0])], 'volume': [float(coin1coin2_depth['asks'][0][1]), float(coin1coin2_depth['asks'][1][1])]}
    if coin_name_2 in ['BNB', 'TUSD']:
        buy_prices['ETH'] = {coin_name_2: {'price': [1/float(ethcoin2_depth['asks'][0][0]), 1/float(ethcoin2_depth['asks'][1][0])], 'volume': [float(ethcoin2_depth['asks'][0][1]), float(ethcoin2_depth['asks'][1][1])]}}        
    else:
        buy_prices['ETH'] = {coin_name_2: {'price': [float(ethcoin2_depth['asks'][0][0]), float(ethcoin2_depth['asks'][1][0])], 'volume': [float(ethcoin2_depth['asks'][0][1]), float(ethcoin2_depth['asks'][1][1])]}}

    buy_prices['ETH'][coin_name_1] = {'price': [1/float(coin1eth_depth['bids'][0][0]), 1/float(coin1eth_depth['bids'][1][0])], 'volume': [float(coin1eth_depth['bids'][0][1]), float(coin1eth_depth['bids'][1][1])]}
    if coin_name_2 in ['BNB', 'TUSD']:
        buy_prices[coin_name_2] = {'ETH': {'price': [float(ethcoin2_depth['bids'][0][0]), float(ethcoin2_depth['bids'][1][0])], 'volume': [float(ethcoin2_depth['bids'][0][1]), float(ethcoin2_depth['bids'][1][1])]}}        
    else:
        buy_prices[coin_name_2] = {'ETH': {'price': [1/float(ethcoin2_depth['bids'][0][0]), 1/float(ethcoin2_depth['bids'][1][0])], 'volume': [float(ethcoin2_depth['bids'][0][1]), float(ethcoin2_depth['bids'][1][1])]}}
    buy_prices[coin_name_2][coin_name_1] = {'price': [1/float(coin1coin2_depth['bids'][0][0]), 1/float(coin1coin2_depth['bids'][1][0])], 'volume': [float(coin1coin2_depth['bids'][0][1]), float(coin1coin2_depth['bids'][1][1])]}

#    if coin_name_3:
#        if coin_name_3 in ['BNB', 'TUSD']:
#            buy_prices[coin_name_3] = {'ETH': {'price': [float(ethcoin3_depth['bids'][0][0]), float(ethcoin3_depth['bids'][1][0])], 'volume': [float(ethcoin3_depth['bids'][0][1]), float(ethcoin3_depth['bids'][1][1])]}}        
#            buy_prices['ETH'][coin_name_3] = {'price': [1/float(ethcoin3_depth['asks'][0][0]), 1/float(ethcoin3_depth['asks'][1][0])], 'volume': [float(ethcoin3_depth['asks'][0][1]), float(ethcoin3_depth['asks'][1][1])]}        
#        else:
#            buy_prices[coin_name_3] = {'ETH': {'price': [1/float(ethcoin3_depth['bids'][0][0]), 1/float(ethcoin3_depth['bids'][1][0])], 'volume': [float(ethcoin3_depth['bids'][0][1]), float(ethcoin3_depth['bids'][1][1])]}}
#            buy_prices['ETH'][coin_name_3] = {'price': [float(ethcoin3_depth['asks'][0][0]), float(ethcoin3_depth['asks'][1][0])], 'volume': [float(ethcoin3_depth['asks'][0][1]), float(ethcoin3_depth['asks'][1][1])]}        
#
#        buy_prices[coin_name_3][coin_name_2] = {'price': [1/float(coin2coin3_depth['bids'][0][0]), 1/float(coin2coin3_depth['bids'][1][0])], 'volume': [float(coin2coin3_depth['bids'][0][1]), float(coin2coin3_depth['bids'][1][1])]}         
    return(buy_prices, eth_available)
        

def sim_tri(exchange_rates, swap_order, available):
    """
    ETH -> coin -> BTC -> ETH
    """
    
#    coin_1, coin_2 = [coin2, coin1]
#    exchange_rates = prices
#    available = float(available_eth['free'])
#    available = 0.18805411
    
    coin_1, coin_2 = swap_order

    try:
        exchange_hist = [('ETH', available)]
        # 1 ETH -> x coin
        coin_1_from_eth = available/exchange_rates[coin_1]['ETH']['price'][0]
        if coin_1_from_eth <= exchange_rates[coin_1]['ETH']['volume'][0]:
            coin_1_from_eth *= .999
            exchange_hist.append((coin_1, coin_1_from_eth))
        else:
            raise Exception('Illiquid')
            
        # x coin -> y BTC
        coin_2_from_coin_1 = coin_1_from_eth / exchange_rates[coin_2][coin_1]['price'][0]
        if coin_1_from_eth <= exchange_rates[coin_2][coin_1]['volume'][0]:
            coin_2_from_coin_1 *= .999
            exchange_hist.append((coin_2, coin_2_from_coin_1))
        else:
            raise Exception('Illiquid')   
     
        # y BTC -> z ETH
        eth_from_coin_2 = coin_2_from_coin_1 / exchange_rates['ETH'][coin_2]['price'][0]
        if coin_2_from_coin_1 <= exchange_rates['ETH'][coin_2]['volume'][0]:
            eth_from_coin_2 *= .999
            exchange_hist.append(('ETH', eth_from_coin_2))
        else:
            raise Exception('Illiquid')   
        
        return(exchange_hist)
    except:
        return(False)
        
        
def filter_potentials(psql): 
#    psql = PSQL_2
    
    eth_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
    	from binance.exchanges ex \
    	join binance.markets mk on mk.market_id = ex_market_id \
    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
    		and buy_coin = 'ETH'"
    eth_prices = pg_query(psql.client, eth_script)
    eth_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
    eth_conv = {k:float(v) for k,v in eth_prices[['sell_coin', 'open']].values}
    
    btc_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
    	from binance.exchanges ex \
    	join binance.markets mk on mk.market_id = ex_market_id \
    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
    		and buy_coin = 'BTC'"
    btc_prices = pg_query(psql.client, btc_script)
    btc_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
    btc_conv = {k:float(v) for k,v in btc_prices[['sell_coin', 'open']].values}
    
    eth_btc = btc_conv['ETH']
    btc_conv.pop('ETH')
    
    btc_traingle = {}
    for coin_2_name, coin_2_eth_price in eth_conv.items():
        if coin_2_name in btc_conv.keys():
            coin_2_btc_price = btc_conv[coin_2_name]
            btc_arb = ((1/coin_2_eth_price)*coin_2_btc_price)/eth_btc
            btc_traingle[coin_2_name] = abs(1 - btc_arb)
            
    btc_traingle = sorted(btc_traingle.items(), key=operator.itemgetter(1), reverse = True)
    btc_potentials = [('BTC', i[0], i[1]) for i in btc_traingle if i[1] >= .005]
    
    bnb_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
    	from binance.exchanges ex \
    	join binance.markets mk on mk.market_id = ex_market_id \
    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
    		and buy_coin = 'BNB'"
    bnb_prices = pg_query(psql.client, bnb_script)
    bnb_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
    bnb_conv = {k:float(v) for k,v in bnb_prices[['sell_coin', 'open']].values}
    
    eth_bnb = 1/eth_conv['BNB']

    bnb_traingle = {}
    for coin_2_name, coin_2_eth_price in eth_conv.items():
        if coin_2_name in bnb_conv.keys():
            coin_2_bnb_price = bnb_conv[coin_2_name]
            bnb_arb = ((1/coin_2_eth_price)*coin_2_bnb_price)/eth_bnb
            bnb_traingle[coin_2_name] = abs(1 - bnb_arb)
            
    bnb_traingle = sorted(bnb_traingle.items(), key=operator.itemgetter(1), reverse = True)
    bnb_potentials = [('BNB', i[0], i[1]) for i in bnb_traingle if i[1] >= .005]
    
#    pax_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
#    	from binance.exchanges ex \
#    	join binance.markets mk on mk.market_id = ex_market_id \
#    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
#    		and buy_coin = 'PAX'"
#    pax_prices = pg_query(psql.client, pax_script)
#    pax_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
#    pax_conv = {k:float(v) for k,v in pax_prices[['sell_coin', 'open']].values}
#    
#    eth_pax = pax_conv['ETH']
#    pax_conv.pop('ETH')
#
#    pax_traingle = {}
#    for coin_2_name, coin_2_eth_price in eth_conv.items():
#        if coin_2_name in pax_conv.keys():
#            coin_2_pax_price = pax_conv[coin_2_name]
#            pax_arb = ((1/coin_2_eth_price)*coin_2_pax_price)/eth_pax
#            pax_traingle[coin_2_name] = abs(1 - pax_arb)
#            
#    pax_traingle = sorted(pax_traingle.items(), key=operator.itemgetter(1), reverse = True)
#    pax_potentials = [('PAX', i[0], i[1]) for i in pax_traingle if i[1] >= .005]
#    
#    tusd_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
#    	from binance.exchanges ex \
#    	join binance.markets mk on mk.market_id = ex_market_id \
#    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
#    		and buy_coin = 'TUSD'"
#    tusd_prices = pg_query(psql.client, tusd_script)
#    tusd_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
#    tusd_conv = {k:float(v) for k,v in tusd_prices[['sell_coin', 'open']].values}
#    
#    eth_tusd = 1/eth_conv['TUSD']
#
#    tusd_traingle = {}
#    for coin_2_name, coin_2_eth_price in eth_conv.items():
#        if coin_2_name in tusd_conv.keys():
#            coin_2_tusd_price = tusd_conv[coin_2_name]
#            tusd_arb = ((1/coin_2_eth_price)*coin_2_tusd_price)/eth_tusd
#            tusd_traingle[coin_2_name] = abs(1 - tusd_arb)
#            
#    tusd_traingle = sorted(tusd_traingle.items(), key=operator.itemgetter(1), reverse = True)
#    tusd_potentials = [('TUSD', i[0], i[1]) for i in tusd_traingle if i[1] >= .005]    
#    
#    bnb_btc = btc_conv['BNB']
#    bnb_btc_square = {}
#    for coin_2_name, coin_2_eth_price in eth_conv.items():
#        if coin_2_name in bnb_conv.keys() and coin_2_name in btc_conv.keys():
#            coin_2_bnb_price = bnb_conv[coin_2_name]
#            coin_2_btc_price = btc_conv[coin_2_name]
#            bnb_btc_arb = eth_btc / coin_2_btc_price * coin_2_bnb_price / eth_bnb
#            bnb_btc_square[coin_2_name] = abs(1 - bnb_btc_arb)
#    bnb_btc_square = sorted(bnb_btc_square.items(), key=operator.itemgetter(1), reverse = True)
#    bnb_btc_potentials = [(['BNB', 'BTC'], i[0], i[1]) for i in bnb_btc_square if i[1] >= .005]

    _potentials = btc_potentials + bnb_potentials # + pax_potentials + tusd_potentials# + bnb_btc_potentials

    return(_potentials)



def clean_tweet(tweet): 
    ''' 
    Utility function to clean tweet text by removing links, special characters 
    using simple regex statements. 
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()).lower() 
  

def pull_tweets(psql, twitter):
    page = requests.get('https://coinmarketcap.com/all/views/all/')
    tree = html.fromstring(page.content)
    coin_name_conv = {v:k for k,v in zip(tree.xpath('//*[@id="currencies-all"]/tbody/tr/td[2]/a/text()'), tree.xpath('//*[@id="currencies-all"]/tbody/tr/td[3]/text()'))}

    coins = pg_query(psql.client, 'SELECT * FROM binance.coins')
    use_coins = {i: coin_name_conv[i] for i in coins[1].values if i in coin_name_conv.keys()}
    coin_id_conv = {v:k for k,v in coins.values}
        
    coin_num = 0
    total_coins = len(use_coins.keys())
    tweets = []
    for symbol, name in use_coins.items():
        progress(coin_num, total_coins, status = symbol)
        fetched_tweets = twitter.search(q = '%s|%s' % (symbol.lower(), name.lower()), count = 10, tweet_mode='extended')
        for tweet in fetched_tweets: 
            # empty dictionary to store required params of a tweet   
            parsed_tweet = {} 
            
            tweet_data = tweet._json
            
            if 'retweeted_status' in tweet_data.keys():
                tweet_data = tweet_data['retweeted_status']
            
            parsed_tweet['text'] = clean_tweet(tweet_data['full_text'])
            parsed_tweet['id'] = tweet_data['id']
            parsed_tweet['rt'] = tweet_data['retweet_count']
            parsed_tweet['fav'] = tweet_data['favorite_count']
            parsed_tweet['user'] = tweet_data['user']['id']
            parsed_tweet['verified'] = tweet_data['user']['verified']
            parsed_tweet['followers'] = tweet_data['user']['followers_count']
            parsed_tweet['datetime'] = datetime.strptime(''.join([' '.join(i.split(' ')[1:]) for i in tweet_data['created_at'].split('+')]), '%b %d %H:%M:%S %Y')        
            tweets.append(parsed_tweet)
        coin_num += 1
    return(tweets, coin_id_conv, use_coins)


def _det_current_twit(_psql, field):
#    _psql, field = PSQL, 'user'
    _data = pg_query(_psql.client, 'select %s_id from binance.twitter_%ss;' % (field, field))
    if len(_data) > 0:
        current_ = set(_data[0].values)
    else:
        current_ = set([])
    return(current_)


def _det_current_nlu(_psql):
    _data = pg_query(_psql.client, 'select max(nlu_id) from binance.twitter_nlu;').values[0][0]
    if _data is None:
        current = 0
    else:
        current = _data + 1
    return(current)


def nlu_insert(nlu_tweet_data, nlu, _psql_, _coin_id_conv, _use_coins):
#    tweet_data = pg_query(PSQL.client, 'select tweet_id, content from binance.twitter_tweets;')
#    nlus = pg_query(PSQL.client, 'select tweet_id from binance.twitter_nlu;')
#    nlus = list(set(nlus[0].values))
#    tweet_idx = [i for i in tweet_data[0] if i not in nlus]
#    tweet_data = tweet_data.set_index(0).loc[tweet_idx].reset_index()
#    nlu_tweet_data, nlu, _psql_, _coin_id_conv, _use_coins = tweet_data[[0,5]], NLU, PSQL, coin_id_conv, use_coins
#    
    matches = {}
    for tweet_id, content in nlu_tweet_data[[0,5]].values:
        matches[tweet_id] = {}
        for symbol, name in _use_coins.items():
            sym_name_match = []
            if name.lower() in content:
                sym_name_match.append(name.lower())
            if symbol.lower() in content:
                sym_name_match.append(symbol.lower())
            if len(sym_name_match) > 0:
                matches[tweet_id][symbol] = sym_name_match
                 
    nlu_idx = _det_current_nlu(_psql_)            
    indexed_tweet_data = nlu_tweet_data.set_index(0)      
    tweet_num = 0
    total_tweets = len(matches.keys())
    for k,v in matches.items():
        progress(tweet_num, total_tweets)
        tweet_num += 1
        targets = []
        for vv in v.values():
            targets += vv
        try:
            nlu_output = nlu.analyze(text=indexed_tweet_data[5].loc[k], features = Features(emotion=EmotionOptions(targets=targets), sentiment=SentimentOptions(targets=targets))).result
        except:
            continue
    
        coin_output = {}
        all_sentiment = nlu_output['sentiment']['targets']
        try:
            all_emotion = nlu_output['emotion']['targets']
        except:
            continue
    
        for coin, names in v.items():
            coin_nlu = {}
            if len([i for i in all_sentiment if i['text'] in names]) == 0:
                continue
            if len([i for i in all_emotion if i['text'] in names]) == 0:
                continue
            coin_nlu['sentiment'] = np.mean([i['score'] for i in all_sentiment if i['text'] in names])
            coin_nlu['sadness'] = np.mean([i['emotion']['sadness'] for i in all_emotion if i['text'] in names])
            coin_nlu['joy'] = np.mean([i['emotion']['joy'] for i in all_emotion if i['text'] in names])
            coin_nlu['fear'] = np.mean([i['emotion']['fear'] for i in all_emotion if i['text'] in names])
            coin_nlu['disgust'] = np.mean([i['emotion']['disgust'] for i in all_emotion if i['text'] in names])
            coin_nlu['anger'] = np.mean([i['emotion']['anger'] for i in all_emotion if i['text'] in names])
            coin_output[coin] = coin_nlu  
            
        for nlu_coin, nlu_values in coin_output.items():
            script = "insert into binance.twitter_nlu (nlu_id, tweet_id, coin_id, sentiment, sadness, joy, fear, disgust, anger) VALUES (%i, %i, %i, %f, %f, %f, %f, %f, %f)" % (nlu_idx, k, _coin_id_conv[nlu_coin], nlu_values['sentiment'], nlu_values['sadness'], nlu_values['joy'], nlu_values['fear'], nlu_values['disgust'], nlu_values['anger'])
            pg_insert(_psql_.client, script)
            nlu_idx += 1
    
    
def insert_twitter(psql, twit, _nlu):   
#    psql, twit, _nlu = PSQL, TWITTER, NLU
    tweets, coin_id_conv, use_coins = pull_tweets(psql, twit)
    user_data = pd.DataFrame([[i['user'], i['followers'], i['verified']] for i in tweets]).drop_duplicates()
    current_users = _det_current_twit(psql, 'user')
    keep_users = [i for i in user_data[0].values if i not in current_users]
    user_data = user_data.set_index(0).loc[keep_users].reset_index()
    print('%i new users' % (len(user_data)))
    for user_id, followers, verified in user_data.values:
        script = "insert into binance.twitter_users (user_id, followers, verified) VALUES (%i, %i, %s)" % (user_id, followers, verified)
        pg_insert(psql.client, script)

    tweet_data = pd.DataFrame([[i['id'], i['user'], i['rt'], i['fav'], i['datetime'], i['text']] for i in tweets]).drop_duplicates()
    current_tweets = _det_current_twit(psql, 'tweet')
    keep_tweets = [i for i in tweet_data[0].values if i not in current_tweets]
    keep_tweets = list(set(keep_tweets))
    tweet_data = tweet_data.set_index(0).loc[keep_tweets].reset_index()
    print('%i new tweets' % (len(tweet_data)))
    
    for tweet_id, user_id, rt, fav, ts, text in tweet_data.values:
        script = "insert into binance.twitter_tweets (tweet_id, user_id, rt, fav, timestamp, content) VALUES (%i, %i, %i, %i, '%s', '%s')" % (tweet_id, user_id, rt, fav, ts, text)
        pg_insert(psql.client, script)
    
    return(tweet_data, coin_id_conv, use_coins)


def find_arb():
    next_update = floor_dt(datetime.now(), timedelta(minutes=30))
    dt_until_update = next_update - datetime.now()
    seconds_before_update = dt_until_update.total_seconds()
    PSQL_2 = db_connection('psql')
    BINANCE_2 = binance_connection()
    potentials = filter_potentials(PSQL_2)
    while seconds_before_update > 0:
        for coin2, coin1, _ in potentials: 
            
#            coin2,coin1,_ = potentials[-1]
            prices, available_eth = cap_market(coin1, coin2, BINANCE_2)
            forward_tri = sim_tri(prices, [coin2, coin1], float(available_eth['free']))
            if forward_tri:
                print('ETH -> %s -> %s -> ETH arbitrage: %.3f%%' % (coin2, coin1, 100 * (forward_tri[-1][-1] - forward_tri[0][-1])))
                if forward_tri[-1][-1] > forward_tri[0][-1]:
                    print(forward_tri)
                    raise Exception('Found Profit!!!')
            backward_tri = sim_tri(prices, [coin1, coin2], float(available_eth['free']))
            if backward_tri:
                print('ETH -> %s -> %s -> ETH arbitrage: %.3f%%' % (coin1, coin2, 100 * (backward_tri[-1][-1] - backward_tri[0][-1])))
                if backward_tri[-1][-1] > backward_tri[0][-1]:
                    print(backward_tri)
                    raise Exception('Found Profit!!!')
#        tm.sleep(15)
        dt_until_update = next_update - datetime.now()
        seconds_before_update = dt_until_update.total_seconds()     
        
    BINANCE_2 = None
    PSQL_2.disconnect()
    PSQL_2 = None
    prices = None
    coin1 = None
    coin2 = None
    forward_tri = None
    backward_tri = None
    next_update = None
    dt_until_update = None
    seconds_before_update = None
            

def update():    
    PSQL = db_connection('psql')
    next_update = floor_dt(datetime.now(), timedelta(minutes=30))
    dt_until_update = next_update - datetime.now()
    seconds_before_update = dt_until_update.total_seconds()
    tm.sleep(seconds_before_update)
    BINANCE = binance_connection()
    NLU = NaturalLanguageUnderstandingV1(version=_config.nlu_credentials["version"], username=_config.nlu_credentials["username"],
                                            password=_config.nlu_credentials["password"])
    NLU.set_default_headers({'x-watson-learning-opt-out' : "true"})
    auth = tweepy.OAuthHandler(_config.twitter_key, _config.twitter_secret)
    TWITTER = tweepy.API(auth)
    
    page = requests.get('https://info.binance.com/en/all')
    tree = html.fromstring(page.content)
    COINS = [{'name':i, 'price':j} for i,j in zip(tree.xpath('//*[@id="__next"]/div/main/div/div/div/div[2]/div[1]/table/tbody/tr/td[2]/div/div/span[1]/text()'), tree.xpath('//*[@id="__next"]/div/main/div/div/div/div[2]/div[1]/table/tbody/tr/td[3]/div/div/text()'))]
          
         
    LAST_TIME = floor_dt(datetime.now(), timedelta(minutes=-30))
    PRICES = BINANCE.get_all_tickers()
    
    pop_coins(PSQL, COINS)
    pop_markets(PSQL, PRICES)
    pop_times(PSQL, LAST_TIME)
    
    CURRENT_MKT_DATA, MRKT_CONV, TIME_CONV = conversion_data(PSQL)
    
    pop_exchanges(PSQL, LAST_TIME, PRICES, MRKT_CONV, CURRENT_MKT_DATA, TIME_CONV, BINANCE)

    tweet_data, coin_id_conv, use_coins = insert_twitter(PSQL, TWITTER, NLU)
#    nlu_insert(tweet_data[[0,1]], NLU, PSQL, coin_id_conv, use_coins)
    
#    pop_prices(PSQL, TIME_CONV, COINS)
    
    PSQL.disconnect()
    PSQL = None
    BINANCE = None
    COINS = None
    LAST_TIME = None
    PRICES = None
    CURRENT_MKT_DATA = None
    MRKT_CONV = None
    TIME_CONV = None
    TWITTER = None
    NLU = None
    tweet_data = None
    coin_id_conv = None
    use_coins = None
        

if __name__ == '__main__':
    cli = False
    try:                                            # if running in CLI
        os.path.abspath(__file__)
        cli = True
    except:
        pass
    
    if cli:
        update()
        while True:
#            find_arb()
                
#            tm.sleep(seconds_before_update)
            update()
            
            
            

       
    
    