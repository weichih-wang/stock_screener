'''
Basic Stock Screener Program which calculates expected stock price based on EPS growth - P/E ratio model
Important functions:
    createStockTable() - creates the sql table in current directory
    updateValues() - updates all values (EPS, future expected EPS, price, P/E ratio, expected stock price, percent price difference)
    findBestStocks(s_or_i,info,num) - finds the best num percentage stock difference within its sector or industry
    expPrice(ticker) - returns expected stock price
'''
    

import pandas as pd
from pandas import DataFrame
import requests
import sqlite3
import datetime
import urllib2
from bs4 import BeautifulSoup
import re
import math

#Stock Database
data = sqlite3.connect('test.db')
c = data.cursor()
reqRet = .1                                                             #require returns of at least 10%

'''
Creates a list of stocks from NYSE, AMEX, and NASDAQ
Table: stockList
Columns:
-id (int): counter for each stock
-Symbol (text): stock symbol
-Name (text): company name
-Price (real): company's previous price
-MarketCap (real): company's market cap
-ADR TSO (text): stock traded in US but represents shares in foreign company
-IPO (int): year of IPO
-Sector (text): sector of company
-Industry (text): industry of company
-Summary Quote (text): company information
'''
def createStockTable():
    nas_lst = requests.get('http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download')
    amex_lst = requests.get('http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download')
    nyse_lst = requests.get('http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download') 
    lst = [nas_lst, amex_lst, nyse_lst] #grabs stock list from nasdaq, amex, and nyse and store text csv in list
    c.executescript('DROP TABLE IF EXISTS stockList')
    c.execute("CREATE TABLE stockList (ID INT, Symbol TEXT, Name TEXT, Price REAL, MarketCap REAL, ADR TSO TEXT, IPO TEXT, Sector TEXT, Industry TEXT, Sum TEXT)")
    for l in lst:
        #transforms unicode text from Nasdaq website into list of words to store into table
        text_lst = []       #list of words of stocks from Nasdaq website
        temp = ""
        start = False
        for i in range(len(l.text)):
            if l.text[i] == '"' and start == False:
                start = True
            elif start == True and l.text[i] == '"':
                start = False
                text_lst.append(temp)
                temp = ""
            elif start == True:
                temp = temp+l.text[i]  
        text_lst = text_lst[9:]  
        
        print type(text_lst)
        #enters in data from text into database
        for i in range(len(text_lst)/9):
            print i
            c.execute("INSERT INTO stockList (ID, Symbol, Name, Price, MarketCap, ADR, IPO, Sector, Industry, Sum) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (i+1,
                text_lst[(9*i)],
                text_lst[(9*i)+1],
                float((text_lst[(9*i)+2] if text_lst[(9*i)+2] != 'n/a' else 0)),
                float(text_lst[(9*i)+3]),
                text_lst[(9*i)+4],
                text_lst[(9*i)+5],
                text_lst[(9*i)+6],
                text_lst[(9*i)+7],
                text_lst[(9*i)+8]))
            data.commit()

'''
Updates stock price, eps, estimated future eps, and p/e within the stock list.  Also removes stock tickers that cannot
be viewed by yahoo finance api.
Data from yahoo: finance.yahoo.com/d/quotes.csv?s=?&f=spe7e8
more info: https://greenido.wordpress.com/2009/12/22/yahoo-finance-hidden-api/
'''
def updateStockInfo():
    c.execute('PRAGMA table_info(stockList)')
    col = [x[1] for x in c.fetchall()]
    if ('EPS' not in col):                                                 #add EPS column if it doesn't exist
        c.execute('ALTER TABLE stockList ADD COLUMN EPS REAL')
    if ('fEPS' not in col):                                                #add future EPS column if it doesn't exist
        c.execute('ALTER TABLE stockList ADD COLUMN fEPS REAL')
    if ('PE' not in col):                                                  #add future P/E ratio column if it doesn't exist
        c.execute('ALTER TABLE stockList ADD COLUMN PE REAL')        
    data.commit()
    c.execute('SELECT Symbol FROM stockList')                              #grabs all tickers from sql stock list
    error = []                                                              #stock list not accepted by yahoo finance
    stock_list_chunks = []                                                   #stock list stored in chunks so there is less access on google finance
    temp = []                        
    for ticker in c.fetchall():                                             #loop used to split stock list into chunks manageable by google finance
        if len(temp) == 1000:
            stock_list_chunks.append(temp)
            temp = []
        temp.append([str(x) for x in ticker][0])
    stock_list_chunks.append(temp)
    for tickers in stock_list_chunks:                                       #loop used to pull stock price, current eps, and future eps from google finance
        #print 'ZTR' in tickers
        d = requests.get('http://finance.yahoo.com/d/quotes.csv?s='+'+'.join(tickers)+'&f=spe7e8r')
        info = d.text
        stock_lst = info.split('\n')                                        #splits stock list into individual stocks for easier parsing
        stock_lst=stock_lst[:-1]
        for stock in stock_lst:
            temp = stock.split(',')                                         #splits individual stocks into ticker, price, eps
            if temp[0] == '"TSFC"':
                print temp
            if 'N/A' in temp:                                               #creates list of stocks not valuable with EPS valuation method
                error.append(str(temp[0])[1:-1])
            else:
                c.execute('UPDATE stockList SET Price=?,EPS=?,fEPS=?,PE=? WHERE Symbol=?',(float(temp[1]),float(temp[2]),float(temp[3]),float(temp[4]),str(temp[0])[1:-1]))
                data.commit()   
    for mia in error:                                                       #removes stock tickers that cannot be extracted from google finance
        c.execute('DELETE FROM stockList Where Symbol = ?',(mia,))
    data.commit()
 
       
'''
Calculates expected stock price using EPS growth - P/E ratio model
'''                      
def calcExpPrice():
    c.execute('PRAGMA table_info(stockList)')
    col = [x[1] for x in c.fetchall()]
    if ('ePrice' not in col):                                                 #add expeced price column if it doesn't exist
        c.execute('ALTER TABLE stockList ADD COLUMN ePrice REAL')      
    c.execute('Select Symbol,Price, fEPS,PE from stockList')                #query stock price, future eps, and current p/e ratio
    for ticker in c.fetchall():
        fPrice = ticker[2]*ticker[3]
        expPrice = fPrice/(1+reqRet)
        c.execute('UPDATE stockList set ePrice=? WHERE Symbol=?',(expPrice,ticker[0]))
    data.commit()

'''
Calculates percentage difference between current stock price and expected stock price
'''   
def calcPercentageDiff():
    c.execute('PRAGMA table_info(stockList)')
    col = [x[1] for x in c.fetchall()]
    if ('perc' not in col):                                                 #add percentage difference if it doesn't exist
        c.execute('ALTER TABLE stockList ADD COLUMN perc REAL')      
    c.execute('Select Symbol,Price, ePrice from stockList')                #query stock price and expected stock price
    for ticker in c.fetchall():
        diff = ticker[2]/ticker[1]
        if diff < 0:
            diff = 0
        c.execute('UPDATE stockList set perc=? WHERE Symbol=?',(diff,ticker[0]))
    data.commit()                                               
  
'''
Updates all stock values
'''                                  
def updateValues():
    updateStockInfo()
    calcExpPrice()
    calcPercentageDiff()
                                                                                                                                                                                                                                                                                                                     
'''
Queries database table to find the best percentage difference in different sectors
s_or_i: query based on sector or industry
info: which part of the sector or industry do you wnat to query
num: returns top group of stocks that you want
'''
def findBestStocks(s_or_i = 'Sector', info = 'None', num = 5):
    c.execute('DROP VIEW IF EXISTS views')
    if info != 'None':
        c.execute('CREATE VIEW views AS SELECT Symbol, perc FROM stockList WHERE ' + s_or_i + '="' + info + '" ORDER BY perc DESC')
    else:
        c.execute('CREATE VIEW views AS SELECT Symbol, perc FROM stockList ORDER BY perc DESC')
    c.execute('SELECT Symbol from views LIMIT 5')
    print c.fetchall()

'''
Returns expected stock price
'''
def expPrice(ticker):
    c.execute('SELECT ePrice FROM stockList WHERE Symbol = ?',(ticker,))
    print c.fetchall()[0][0]

'''
Updates stock price within stock list.  Also removes stock tickers that cannot be viewed by google finance.
Data from Google: http://finance.google.com/finance/info?client=ig&q=?
'''
def updatePrice():
    c.execute("SELECT DISTINCT Symbol FROM stockList")
    stock_list_chunks = []                                                   #stock list stored in chunks so there is less access on google finance
    temp = []        
    stock_price_pair = []      
    valid_stocks = []                                                        #stock ticker list accepted by google                   
    for ticker in c.fetchall():                                             #loop used to split stock list into chunks manageable by google finance
        if len(temp) == 100:
            stock_list_chunks.append(temp)
            temp = []
        temp.append([str(x) for x in ticker][0])
    for tickers in stock_list_chunks:                                       #loop used to pull stock price and ticker pairings from google finance
        d = requests.get("http://finance.google.com/finance/info?client=ig&q="+ ','.join(tickers))
        info = d.text
        regPair = re.compile('"t" : "(.+)"\n,"e" : "(.+)"\n,"l" : "(.+)"')  #regex used to parse request text
        for pair in regPair.findall(info):                                  #stores ticker and price pairs into string
            stock_price_pair.append([[str(x) for x in pair][0],float(pair[2].replace(',',''))])
            valid_stocks.append([str(x) for x in pair][0])
    for pair in stock_price_pair:                                           #stores values into sql table
        c.execute("UPDATE stockList SET Price = ? WHERE Symbol = ?",(pair[1],pair[0]))
    data.commit()
    not_found = list(set([item for sublist in stock_list_chunks for item in sublist])-set(valid_stocks))
    for mia in not_found:                                                   #removes stock tickers that cannot be extracted from google finance
        c.execute('DELETE FROM stockList Where Symbol = ?',(mia,))
    data.commit()

'''
Updates stock beta within stock list.  Also removes stock tickers that cannot be viewed by nasdaq.
Data from Google: http://finance.google.com/finance/info?client=ig&q=?
'''
def updateBeta():
    c.execute('PRAGMA table_info(stockList)')
    col = [x[1] for x in c.fetchall()]
    if ('Beta' not in col):                                                 #add Beta column if it doesn't exist
        c.execute('ALTER TABLE stockList ADD COLUMN Beta REAL')
    data.commit()
    c.execute("SELECT DISTINCT Symbol FROM stockList")      
    import time
    start_time = time.time()
    error = []                                                 
    for ticker in c.fetchall():
        d = requests.get("http://www.nasdaq.com/symbol/"+[str(x) for x in ticker][0])
        info = d.text                                                    #stock ticker information from NASDAQ
        regBeta = re.compile('In contrast, a stock fund or ETF with a low beta will rise or fall less.\r\n\s+' +
            '</span>\r\n\s+<span class="bottomLG"></span>\r\n\s+</span>\r\n\s+</a>\r\n\s+' + 
            '</td>\r\n\s+<td align="right" nowrap>(.+)</td>')
        try:
            b = float(regBeta.findall(info)[0].replace(',',''))
        except:
            b = 0.0
            error.append(str(ticker[0]))
        print (str(ticker[0]),b)
        c.execute("UPDATE stockList SET Beta = ? WHERE Symbol = ?",(b,str(ticker[0])))
        data.commit()
        #print (time.time()-start_time)/60
        
#a = re.compile('<div id="qwidget_lastsale" class="qwidget-dollar">(.+)</div>')
#b = re.compile('"Beta" is a volatility measurement of a stock mutual fund or ETF versus a comparable benchmark like the S&P 500 stock index. A stock fund or ETF with a higher beta than the S&P 500 will rise or fall to a greater degree. In contrast, a stock fund or ETF with a low beta will rise or fall less.\r\n\s+</span>\r\n\s+<span class="bottomLG"></span>\r\n\s+</span>\r\n\s+</a>\r\n\s+</td>\r\n\s+<td align="right" nowrap>(.+)</td>')

'''
Helper Functions
'''

def getDay():
    return datetime.date.today().day
    
def getMonth():
    return datetime.date.today().month

def getYear():
    return
    


def tableCreate():
    c.execute("CREATE TABLE stockList (ID INT, )")

'''
saves current csv file to desktop
'''   
def saveStockList():
    return