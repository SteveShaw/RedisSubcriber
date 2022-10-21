from random import sample
import symbol
from turtle import back
import pyarrow.feather as pf
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import sqlalchemy.types
import redis
import time
import json
import argparse
from datetime import datetime

def get_schema():
    schema = {}
    schema['sample_timestamp'] = sqlalchemy.types.DateTime()
    schema['root'] = sqlalchemy.types.VARCHAR(length=45)
    schema['expiration'] = sqlalchemy.types.Date()
    schema['strike'] = sqlalchemy.types.Float()
    schema['callput'] = sqlalchemy.types.VARCHAR(length=45)
    schema['option_id'] = sqlalchemy.types.VARCHAR(length=45)
    schema['bidvol'] = sqlalchemy.types.Float()
    schema['askvol'] = sqlalchemy.types.Float()
    schema['bidvol'] = sqlalchemy.types.Float()
    schema['bidprice'] = sqlalchemy.types.Float()
    schema['askprice'] = sqlalchemy.types.Float()
    schema['bidsize'] = sqlalchemy.types.Float()
    schema['asksize'] = sqlalchemy.types.Float()
    schema['uprice'] = sqlalchemy.types.Float()
    schema['delta'] = sqlalchemy.types.Float()
    schema['vega'] = sqlalchemy.types.Float()
    schema['emavol'] = sqlalchemy.types.Float()

def get_db_config():
    config = {}
    config['host'] = "qbl_mysql.servers.bluefintrading.com"
    config['pwd'] = "You Suck!"
    config['db'] = "IV_recorder"
    config['user'] ="Local_Python"
    return config

def get_symbol_table_map():
    symbol_tables = {}
    symbol_tables['AAPL'] = 'intraday_iv_recorder_aapl'
    symbol_tables['AMZN'] = 'intraday_iv_recorder_amzn'
    symbol_tables['MSFT'] = 'intraday_iv_recorder_msft'
    symbol_tables['META'] = 'intraday_iv_recorder_meta'
    symbol_tables['SPY'] = 'intraday_iv_recorder_spy'
    symbol_tables['QQQ'] = 'intraday_iv_recorder_qqq'
    symbol_tables['TSLA'] = 'intraday_iv_recorder_tsla'
    return symbol_tables

def WriteToHistTable(table_name, data, mysql_conn, schema):
    data.to_sql(table_name, mysql_conn, index=False, if_exists='append', dtype=schema)

def RunSaveToHist(redis_file_key):
    db_config = get_db_config()
    schema = get_schema()
    cache = redis.StrictRedis(host="localhost", port=6379)
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=db_config['host'], db=db_config['db'], user=db_config['user'], pw=db_config['pwd']))
    last_file = ''
    symbol_tables = get_symbol_table_map()

    while True:
        sample_time = datetime.now()
        if sample_time.second < 15:
            time.sleep(15 - sample_time.second)
        elif sample_time.second > 55:
            time.sleep(60- sample_time.second + 14)
        t1 = time.time()
        try:
            byte_msg = cache.get(redis_file_key)
            if byte_msg == None:
                continue
            msg = json.loads(byte_msg.decode('ascii'))
            filename = msg['File']
            if filename == last_file:
                continue
            last_file = filename
        except Exception as ex:
            print('EQUITY RunSaveToHist: Read redis key {} Error {}'.format(redis_file_key, ex))
            continue
        try:
            df = pf.read_feather(filename, memory_map=True)
            df.rename(columns={ 'fittedvol': 'emavol'},inplace=True)
            with engine.begin() as mysql_conn:
                for symbol in ['AAPL', 'MSFT', 'META', 'AMZN', 'QQQ', 'SPY', 'TSLA']:
                    WriteToHistTable(symbol_tables[symbol], df.loc[df['root']==symbol], mysql_conn, schema)
                # WriteToHistTable('temp_iv_recorder_msft', df.loc[df['root']=='MSFT'], mysql_conn, schema)
                # WriteToHistTable('temp_iv_recorder_spy', df.loc[df['root']=='SPY'], mysql_conn, schema)
                # WriteToHistTable('temp_iv_recorder_qqq', df.loc[df['root']=='QQQ'], mysql_conn, schema)
                # WriteToHistTable('temp_iv_recorder_meta', df.loc[df['root']=='META'], mysql_conn, schema)
                # WriteToHistTable('temp_iv_recorder_amzn', df.loc[df['root']=='AMZN'], mysql_conn, schema)
                # WriteToHistTable('temp_iv_recorder_tsla', df.loc[df['root']=='TSLA'], mysql_conn, schema)
        except Exception as ex:
            print('EQUITY RunSaveToHist: Read file {}  and write to table in database {} Error {}'.format(filename, db_config['db'], ex))
            continue

        t2 = time.time()
        print('load file:{}'.format(filename))
        print(df.head())
        print('RunSaveToHist: backup spent {} msecs'.format(int(1000*(t2-t1))))
        time_diff = datetime.now() - sample_time
        if 60 > time_diff.seconds:
            time.sleep(60 - time_diff.seconds)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', "--Key", help = "Latest File Redis Key")
    parser.add_argument('-b', "--Backup", help = "Backup to intraday", action='store_true')
    args = parser.parse_args()
    rd_filekey = None
    if args.Key:
        rd_filekey = args.Key
        print('Read from: {}'.format(args.Key))
    else:
        print('Specify file key')
        exit()
    if args.Backup:
        print('Equity Options: backup to intraday data tables')
        RunSaveToHist(redis_file_key=rd_filekey)
    else:
        print('Please specify -l (save to live table) or -b (backup data to historical data table)')

