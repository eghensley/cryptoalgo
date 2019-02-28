coins = ['DROP TABLE IF EXISTS binance.coins;',
           
           'CREATE TABLE binance.coins \
            (coin_id integer NOT NULL, \
            symbol text COLLATE pg_catalog."default" NOT NULL, \
            CONSTRAINT coin_pkey PRIMARY KEY (coin_id), \
            CONSTRAINT coin_uq UNIQUE (symbol)) \
            WITH (OIDS = FALSE) \
            TABLESPACE pg_default;'
            
            'DROP INDEX IF EXISTS binance.coin_name_idx;',
            'CREATE INDEX coin_name_idx \
            ON binance.coins USING btree \
            (symbol COLLATE pg_catalog."default" text_pattern_ops) \
            TABLESPACE pg_default;',
            
            'DROP INDEX IF EXISTS binance.coin_id_idx;',
            'CREATE INDEX coin_id_idx \
            ON binance.coins USING btree \
            (coin_id) \
            TABLESPACE pg_default;'
            ]

markets = ['DROP TABLE IF EXISTS binance.markets;',
           
           'CREATE TABLE binance.markets \
            (market_id integer NOT NULL, \
            symbol text COLLATE pg_catalog."default" NOT NULL, \
            sell_coin text COLLATE pg_catalog."default" NOT NULL, \
            buy_coin text COLLATE pg_catalog."default" NOT NULL, \
            CONSTRAINT market_pkey PRIMARY KEY (market_id), \
            CONSTRAINT market_buy_coin_fkey FOREIGN KEY (buy_coin) \
            REFERENCES binance.coins (symbol), \
            CONSTRAINT market_sell_coin_fkey FOREIGN KEY (sell_coin) \
            REFERENCES binance.coins (symbol)) \
            WITH (OIDS = FALSE) \
            TABLESPACE pg_default;'
            
            'DROP INDEX IF EXISTS binance.sell_coin_idx;',
            'CREATE INDEX sell_coin_idx \
            ON binance.markets USING btree \
            (sell_coin  COLLATE pg_catalog."default" text_pattern_ops) \
            TABLESPACE pg_default;',

            'DROP INDEX IF EXISTS binance.buy_coin_idx;',
            'CREATE INDEX buy_coin_idx \
            ON binance.markets USING btree \
            (buy_coin COLLATE pg_catalog."default" text_pattern_ops) \
            TABLESPACE pg_default;',
            
            'DROP INDEX IF EXISTS binance.market_id_idx;',
            'CREATE INDEX market_id_idx \
            ON binance.markets USING btree \
            (market_id) \
            TABLESPACE pg_default;'
            ]


times = ['DROP TABLE IF EXISTS binance.times;',
           
           'CREATE TABLE binance.times \
            (time_id integer NOT NULL, \
            q_date DATE, \
            q_time TIME, \
            full_time TIMESTAMP, \
            CONSTRAINT time_pkey PRIMARY KEY (time_id), \
            CONSTRAINT time_uq UNIQUE (full_time)) \
            WITH (OIDS = FALSE) \
            TABLESPACE pg_default;'
            
            'DROP INDEX IF EXISTS binance.time_detail_idx;',
            'CREATE INDEX time_detail_idx \
            ON binance.times USING btree \
            (full_time) \
            TABLESPACE pg_default;',

            'DROP INDEX IF EXISTS binance.time_quick_idx;',
            'CREATE INDEX time_quick_idx \
            ON binance.times USING btree \
            (q_time) \
            TABLESPACE pg_default;',

            'DROP INDEX IF EXISTS binance.date_quick_idx;',
            'CREATE INDEX date_quick_idx \
            ON binance.times USING btree \
            (q_date) \
            TABLESPACE pg_default;',
            
            'DROP INDEX IF EXISTS binance.time_id_idx;',
            'CREATE INDEX time_id_idx \
            ON binance.times USING btree \
            (time_id) \
            TABLESPACE pg_default;'
            ]


exchanges = ['DROP TABLE IF EXISTS binance.exchanges;',
           
           'CREATE TABLE binance.exchanges \
            (ex_time_id integer NOT NULL, \
            ex_market_id integer NOT NULL, \
            open numeric NOT NULL, \
            high numeric NOT NULL, \
            low numeric NOT NULL, \
            close numeric NOT NULL, \
            volume numeric NOT NULL, \
            quote_asset_volume numeric NOT NULL, \
            number_of_trades numeric NOT NULL, \
            taker_buy_base_asset_volume numeric NOT NULL, \
            taker_buy_quote_asset_volume numeric NOT NULL, \
            CONSTRAINT exchange_time_fkey FOREIGN KEY (ex_time_id) \
            REFERENCES binance.times (time_id), \
            CONSTRAINT exchange_market_fkey FOREIGN KEY (ex_market_id) \
            REFERENCES binance.markets (market_id)) \
            WITH (OIDS = FALSE) \
            TABLESPACE pg_default;'
            
            'DROP INDEX IF EXISTS binance.exchange_time_idx;',
            'CREATE INDEX exchange_time_idx \
            ON binance.exchanges USING btree \
            (ex_time_id) \
            TABLESPACE pg_default;',

            'DROP INDEX IF EXISTS binance.exchange_market_idx;',
            'CREATE INDEX exchange_market_idx \
            ON binance.exchanges USING btree \
            (ex_market_id) \
            TABLESPACE pg_default;'
            ]


prices = ['DROP TABLE IF EXISTS binance.prices;',
           
           'CREATE TABLE binance.prices \
            (pr_symbol text COLLATE pg_catalog."default" NOT NULL, \
            pr_time_id integer NOT NULL, \
            usd numeric NOT NULL, \
            CONSTRAINT price_time_fkey FOREIGN KEY (pr_time_id) \
            REFERENCES binance.times (time_id), \
            CONSTRAINT price_coin_fkey FOREIGN KEY (pr_symbol) \
            REFERENCES binance.coins (symbol)) \
            WITH (OIDS = FALSE) \
            TABLESPACE pg_default;'
            
            'DROP INDEX IF EXISTS binance.prices_time_idx;',
            'CREATE INDEX prices_time_idx \
            ON binance.prices USING btree \
            (pr_time_id) \
            TABLESPACE pg_default;',

            'DROP INDEX IF EXISTS binance.prices_price_idx;',
            'CREATE INDEX prices_price_idx \
            ON binance.prices USING btree \
            (usd) \
            TABLESPACE pg_default;',
            
            'DROP INDEX IF EXISTS binance.prices_name_idx;',
            'CREATE INDEX prices_name_idx \
            ON binance.prices USING btree \
            (pr_symbol COLLATE pg_catalog."default" text_pattern_ops) \
            TABLESPACE pg_default;',
            ]


create_tables = {}
create_tables['coins'] = coins
create_tables['markets'] = markets
create_tables['times'] = times
create_tables['exchanges'] = exchanges
create_tables['prices'] = prices