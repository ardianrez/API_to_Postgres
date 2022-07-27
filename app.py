#### source: https://www.alphavantage.co/documentation/#intraday-extended
##### API key: LNDQWMAKTBSNQ7J8

import requests
import psycopg2
import numpy as np
import psycopg2.extras as extras
import pandas as pd
from datetime import datetime
import pytz

portfolio_stocks = ['TSLA', 'AAPL', 'XOM', 'GOOGL']
dft = pd.DataFrame()

for stock in portfolio_stocks:
    # extract intraday data from free AlphaAdvantage API
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=1min&outputsize=full&apikey=LNDQWMAKTBSNQ7J8' %stock
    r = requests.get(url)
    data = r.json()

    #extract ticker from metadata
    symbol = pd.DataFrame(data['Meta Data'], index=[0])['2. Symbol'][0]

    # turn time series into DF and transform
    df = pd.DataFrame(data['Time Series (1min)']).T.reset_index()

    # cleaning to match postgres table (as below):
        # CREATE TABLE IF NOT EXISTS stocks(
        # 	ticker VARCHAR(10) NOT NULL,
        # 	time TIMESTAMP NOT NULL,
        # 	open FLOAT(2),
        # 	high FLOAT(2),
        # 	low FLOAT(2),
        # 	close FLOAT(2),
        # 	volume INTEGER
        # );

    # change data types
    df = df.astype({'1. open':'float', '2. high':'float', '3. low':'float', '4. close':'float', '5. volume':'float'})
    # change column names
    df = df.rename({'index':'time', '1. open':'open', '2. high':'high', '3. low':'low', '4. close':'close', '5. volume':'volume'}, axis=1)
    # set ticker name from json metadata
    df['ticker'] = symbol
    # concatenate the dataframe
    dft = pd.concat([dft, df], axis = 0)
    
def execute_values(conn, df, table):
    
    # get new york time zone for log file
    logtime = datetime.now(pytz.timezone('Asia/Makassar'))
    # transform timestamp into unique log file name
    logname = str(logtime)[:19].translate(str.maketrans({'-':'', ' ':'', ':':''}))

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    # UPSERT query with conflict handling to avoid duplication
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        message = 'PostgreSQL 14 - Stocks DB - Stocks Table has been successfule updated at %s' %logtime
    except (Exception, psycopg2.DatabaseError) as error:
        message = 'Error: %s' % error
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    # write log
    log = open('logs/%s.txt' %logname, 'w')
    log.write('%s\n%s' %(logtime, message))
    log.close()

conn = psycopg2.connect(user="postgres",
                            password="root",
                            host='localhost',
                            port="5432",
                            database="stocks")

# execute the function for the specific ticker                             
execute_values(conn, dft, 'stocks')