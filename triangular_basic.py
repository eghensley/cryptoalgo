import os, sys
try:                                            # if running in CLI
    cur_path = os.path.abspath(__file__)
except NameError:                               # if running in IDE
    cur_path = os.getcwd()

while cur_path.split('/')[-1] != 'binance':
    cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))    
sys.path.insert(1, os.path.join(cur_path, 'lib', 'python3.7', 'site-packages'))


from update_psql import pg_query
from _connections import db_connection
import operator

PSQL = db_connection('psql')

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
        traingle[coin_2_name] = arb
        
sorted(traingle.items(), key=operator.itemgetter(1), reverse = True)