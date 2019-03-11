import os, sys
try:                                            # if running in CLI
    cur_path = os.path.abspath(__file__)
except NameError:                               # if running in IDE
    cur_path = os.getcwd()

while cur_path.split('/')[-1] != 'binance':
    cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))    
sys.path.insert(1, os.path.join(cur_path, 'lib', 'python3.7', 'site-packages'))


from update_psql import pg_query
from _connections import db_connection, binance_connection
import operator
import time

PSQL = db_connection('psql')
BINANCE = binance_connection()


def cap_market(coin_name):
    coineth_depth = BINANCE.get_order_book(symbol='%sETH' % (coin_name))
    coinbtc_depth = BINANCE.get_order_book(symbol='%sBTC' % (coin_name))
    ethbtc_depth = BINANCE.get_order_book(symbol='ETHBTC')
    
    buy_prices = {}
    buy_prices[coin_name] = {'ETH': {'price': [float(coineth_depth['asks'][0][0]), float(coineth_depth['asks'][1][0])], 'volume': [float(coineth_depth['asks'][0][1]), float(coineth_depth['asks'][1][1])]}}
    buy_prices[coin_name]['BTC'] = {'price': [float(coinbtc_depth['asks'][0][0]), float(coinbtc_depth['asks'][1][0])], 'volume': [float(coinbtc_depth['asks'][0][1]), float(coinbtc_depth['asks'][1][1])]}
    buy_prices['ETH'] = {'BTC': {'price': [float(ethbtc_depth['asks'][0][0]), float(ethbtc_depth['asks'][1][0])], 'volume': [float(ethbtc_depth['asks'][0][1]), float(ethbtc_depth['asks'][1][1])]}}

    buy_prices['ETH'][coin_name] = {'price': [1/float(coineth_depth['bids'][0][0]), 1/float(coineth_depth['bids'][1][0])], 'volume': [float(coineth_depth['bids'][0][1]), float(coineth_depth['bids'][1][1])]}
    buy_prices['BTC'] = {'ETH': {'price': [1/float(ethbtc_depth['bids'][0][0]), 1/float(ethbtc_depth['bids'][1][0])], 'volume': [float(ethbtc_depth['bids'][0][1]), float(ethbtc_depth['bids'][1][1])]}}
    buy_prices['BTC'][coin_name] = {'price': [1/float(coinbtc_depth['bids'][0][0]), 1/float(coinbtc_depth['bids'][1][0])], 'volume': [float(coinbtc_depth['bids'][0][1]), float(coinbtc_depth['bids'][1][1])]}

    return(buy_prices)
        



def sim_tri(exchange_rates, swap_order):
    """
    ETH -> coin -> BTC -> ETH
    """
    
#    coin_1, coin_2 = 'HOT', 'BTC'
#    exchange_rates = prices

    coin_1, coin_2 = swap_order

    try:
        exchange_hist = [('ETH', 1)]
        # 1 ETH -> x coin
        coin_1_from_eth = 1/exchange_rates[coin_1]['ETH']['price'][0]
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
        
        
def filter_potentials(): 
    eth_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
    	from binance.exchanges ex \
    	join binance.markets mk on mk.market_id = ex_market_id \
    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
    		and buy_coin = 'ETH'"
    eth_prices = pg_query(PSQL.client, eth_script)
    eth_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
    eth_conv = {k:float(v) for k,v in eth_prices[['sell_coin', 'open']].values}
    
    btc_script = "select ex_market_id, ex_time_id, open, sell_coin, buy_coin \
    	from binance.exchanges ex \
    	join binance.markets mk on mk.market_id = ex_market_id \
    	where ex_time_id = (select max(t2.time_id) from binance.times t2) \
    		and buy_coin = 'BTC'"
    btc_prices = pg_query(PSQL.client, btc_script)
    btc_prices.columns = ['market_id', 'time_id', 'open', 'sell_coin', 'buy_coin']
    btc_conv = {k:float(v) for k,v in btc_prices[['sell_coin', 'open']].values}
    
    eth_btc = btc_conv['ETH']
    btc_conv.pop('ETH')
    
    traingle = {}
    for coin_2_name, coin_2_eth_price in eth_conv.items():
        if coin_2_name in btc_conv.keys():
            coin_2_btc_price = btc_conv[coin_2_name]
            arb = ((1/coin_2_eth_price)*coin_2_btc_price)/eth_btc
            traingle[coin_2_name] = abs(1 - arb)
            
    traingle = sorted(traingle.items(), key=operator.itemgetter(1), reverse = True)
    _potentials = [i for i in traingle if i[1] >= .01]
    return(_potentials)
    
potentials = filter_potentials()

while True:
    for coin, _ in potentials:
        print('Analyzing %s' % (coin))
        
        prices = cap_market(coin)
            
        forward_tri = sim_tri(prices, ['BTC', coin])
        if forward_tri and forward_tri[-1][-1] > 1:
            asdfasdf
        backward_tri = sim_tri(prices, [coin, 'BTC'])
        if backward_tri and backward_tri[-1][-1] > 1:
            asdfasdf        
    
    time.sleep(15)

    
    
    
    








