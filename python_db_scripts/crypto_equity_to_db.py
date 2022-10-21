from random import sample
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
    schema['fittedvol'] = sqlalchemy.types.Float()

def get_db_config():
    config = {}
    config['host'] = "qbl_mysql.servers.bluefintrading.com"
    config['pwd'] = "You Suck!"
    config['db'] = "crypto"
    config['user'] ="Local_Python"
    return config

def WriteToLiveTable(table_name, data, mysql_conn, schema):
    data.to_sql(table_name, mysql_conn, index=False, if_exists='append', dtype=schema)
    #get minimum sample_timestamp
    min_tm = data['sample_timestamp'].min()
    sql_statement = text("""DELETE FROM {} where sample_timestamp < '{}'""".format(table_name, min_tm))
    mysql_conn.execute(sql_statement)
    print(sql_statement)
    
def WriteToHistTable(table_name, data, mysql_conn, schema):
    data.to_sql(table_name, mysql_conn, index=False, if_exists='append', dtype=schema)

def RunLive(redis_files_key, live_table):
    db_config = get_db_config()

    files_key = redis_files_key
    to_table = live_table

    cache = redis.StrictRedis(host="localhost", port=6379)
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=db_config['host'], db=db_config['db'], user=db_config['user'], pw=db_config['pwd']))
    schema = get_schema()

    last_file = ''
    while True:
        try:
            byte_msg = cache.get(files_key)
            if byte_msg == None:
                time.sleep(1)
                continue
            msg = json.loads(byte_msg.decode('ascii'))
            filename = msg['File']
            if filename == last_file:
                time.sleep(2)
                continue
            last_file = filename
        except Exception as ex:
            print('CRYPTO RunLive: Read redis key {} Error {}'.format(files_key, ex))

        t1 = time.time()
        try:
            df = pf.read_feather(filename, memory_map=True)
            # df.rename(columns={'id':'option_id'},inplace=True)
            with engine.begin() as mysql_conn:
                WriteToLiveTable(to_table, df, mysql_conn, schema)
        except Exception as ex:
            print('CRYPTO RunLive: Read file {}  and write to table {}.{} Error {}'.format(filename, db_config['db'], to_table, ex))
            continue

        t2 = time.time()
        print('load file:{}'.format(filename))
        print(df.head())
        print('Time={}'.format(int(1000*(t2-t1))))
        time.sleep(1)

def RunSaveToHist(redis_file_key, hist_table):
    db_config = get_db_config()
    schema = get_schema()
    cache = redis.StrictRedis(host="localhost", port=6379)
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=db_config['host'], db=db_config['db'], user=db_config['user'], pw=db_config['pwd']))
    last_file = ''
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
            print('CRYPTO RunSaveToHist: Read redis key {} Error {}'.format(redis_file_key, ex))

        try:
            df = pf.read_feather(filename, memory_map=True)
            # df.rename(columns={'id':'option_id'},inplace=True)
            with engine.begin() as mysql_conn:
                WriteToHistTable(hist_table, df, mysql_conn, schema)
        except Exception as ex:
            print('CRYPTO RunSaveToHist: Read file {}  and write to table {}.{} Error {}'.format(filename, db_config['db'], hist_table, ex))
            continue

        t2 = time.time()
        print('backup spent {} msecs'.format(int(1000*(t2-t1))))
        time_diff = datetime.now() - sample_time
        if 60 > time_diff.seconds:
            time.sleep(60 - time_diff.seconds)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', "--Key", help = "Latest File Redis Key")
    parser.add_argument('-l', "--Live", help = "Latest File Redis Key", action='store_true')
    parser.add_argument('-b', "--Backup", help = "Latest File Redis Key", action='store_true')
    args = parser.parse_args()
    rd_filekey = None
    if args.Key:
        rd_filekey = args.Key
        print('Read from: {}'.format(args.Key))
    else:
        print('Specify file key')
        exit()
    if args.Live:
        live_table = 'iv_live_crypto'
        print('Save to live table {}'.format(live_table))
        RunLive(rd_filekey, live_table)
    elif args.Backup:
        backup_table = 'iv_recorder_crypto'
        print('Backup to historical data table {}'.format(backup_table))
        RunSaveToHist(redis_file_key=rd_filekey, hist_table=backup_table)
    else:
        print('Please specify -l (save to live table) or -b (backup data to historical data table)')

