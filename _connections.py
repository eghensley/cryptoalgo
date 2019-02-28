#mongodb_creds = "mongodb://eric:Hensley86!@207.38.142.196:4646/ncaa_bb?authSource=marine_science"
import os, sys
try:                                            # if running in CLI
    cur_path = os.path.abspath(__file__)
except NameError:                               # if running in IDE
    cur_path = os.getcwd()

while cur_path.split('/')[-1] != 'binance':
    cur_path = os.path.abspath(os.path.join(cur_path, os.pardir))    
sys.path.insert(1, os.path.join(cur_path, 'lib', 'python3.7', 'site-packages'))


from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
import _config
import psycopg2
from binance.client import Client

def tunnel_config(_port):
    ssh_tunnel_server = SSHTunnelForwarder(
        (_config.SSH_HOST, _config.SSH_PORT),
        ssh_username=_config.SSH_USER,
        ssh_password=_config.SSH_PASS,
        remote_bind_address=('127.0.0.1', _port)
    )
    return(ssh_tunnel_server)
    
class server_null_tunnel:
    
    def __init__(self, _port):
        self.local_bind_port = _port
        
    def start(self):
        ''
    def stop(self):
        ''
    

name_to_port = {'mongo': 27017,
                'psql': 5432}

class db_connection:
    
    def __init__(self, service, collection = 'binance'):
        self.service = service
        self.collection = collection
        self.port = name_to_port[service.lower()] 
        if _config.SERVER:
            self.tunnel = server_null_tunnel(self.port)
        else:        
            self.tunnel = tunnel_config(self.port)         
            self.tunnel.start()
        if self.service == 'mongo':
            self.engine = MongoClient("mongodb://%s:%s@127.0.0.1:%i/%s?authSource=%s" % (_config.DB_USER, _config.DB_PW, self.tunnel.local_bind_port, _config.DB_USER, _config.DB_USER))
            self.client = self.engine['ehens86'][self.collection]
        elif self.service == 'psql':
            params = {
             'database': _config.DB_USER,
             'user': _config.DB_USER,
             'password': _config.DB_PW,
             'host': 'localhost',
             'port': self.tunnel.local_bind_port
             }
            self.engine = psycopg2.connect(**params)
            self.client = self.engine.cursor()   
    def disconnect(self):
        self.engine.close()
        self.tunnel.stop()
        
    def reset_db_con(self):
        if self.service == 'mongo':
            self.engine = MongoClient("mongodb://%s:%s@127.0.0.1:%i/%s?authSource=%s" % (_config.DB_USER, _config.DB_PW, self.tunnel.local_bind_port, _config.DB_USER, _config.DB_USER))
            self.client = self.engine['ehens86'][self.collection]
        elif self.service == 'psql':
            params = {
             'database': _config.DB_USER,
             'user': _config.DB_USER,
             'password': _config.DB_PW,
             'host': 'localhost',
             'port': self.tunnel.local_bind_port
             }
            self.engine = psycopg2.connect(**params)
            self.client = self.engine.cursor()             

def binance_connection():
    client = Client(_config.binance_user, _config.binance_pw)
    return(client)