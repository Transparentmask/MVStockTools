#!/usr/bin/env python
# -*- encoding: utf-8 -*- 
import os, sys, string
import urllib2
import sqlite3
import time
from datetime import datetime
import pytz
import threading

currentDate = pytz.timezone('US/Eastern').normalize(pytz.timezone('Asia/Shanghai').localize(datetime.now()).astimezone(pytz.timezone('US/Eastern'))).strftime("%Y%m%d")
symbols = ['aapl', 'msft', 'fb', 'mbly', 'tsla', 'sbux', 'jd', 'ctrp', 'qunr', 'vips', 'baba', 'qihu', 'syna', 'gpro', 'imax', 'fit', 'uvxy', 'afmd', 'yoku', 'wb']
symbols_sqls = {}

def initDB(db = "stocks.sqlite"):
    global symbols
    global symbols_sqls
    
    sqlconn = None
    
    try:
        sqlconn = sqlite3.connect(db)
        cursor = sqlconn.cursor()
        for symbol in symbols:
            print symbol
            sqlString = "CREATE TABLE `%s` ( `symbol` VARCHAR, "\
                                            "`usdatetime` VARCHAR, "\
                                            "`timestamp` INTEGER, "\
                                            "`price` FLOAT, "\
                                            "`change_percent` FLOAT, "\
                                            "`bjdatetime` VARCHAR, "\
                                            "`change` FLOAT, "\
                                            "`open` FLOAT, "\
                                            "`high` FLOAT, "\
                                            "`low` FLOAT, "\
                                            "`high52` FLOAT, "\
                                            "`low52` FLOAT, "\
                                            "`volume` INTEGER, "\
                                            "`market_value` INTEGER, "\
                                            "`pre_ah_market_price` FLOAT, "\
                                            "`pre_market_change` FLOAT, "\
                                            "`ah_market_change` FLOAT, "\
                                            "`yesterday_close` FLOAT, "\
                                            "`data` VARCHAR)" % (symbol)
            try:
                cursor.execute(sqlString)
            except sqlite3.Error as e:
                print "initDB Error 1: %s" % e
            except Error as e:
                print e
            
        sqlconn.commit()
    
    except sqlite3.Error as e:
        print "initDB Error 2: %s" % e
    
    except Error:
        print "other error"
    
    finally:
        if sqlconn:
            return sqlconn

def getPrice():
    global symbols
    
    url = "http://hq.sinajs.cn/list=" + ','.join(map(lambda m: 'gb_'+m, symbols))
    contents = urllib2.urlopen(url).read().decode("gb18030").split("\n")
    
    return dict(zip(symbols, contents))

class GetSinaPriceThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.kill_received = False
    
    def run(self):
        print ""
        global symbols
        global symbols_sqls
        global currentDate
        sqlfile = "stocks_%s.sqlite" % currentDate
        sqlconn = initDB(sqlfile)
        cursor = sqlconn.cursor()
        lastminute = 0
        
        while not self.kill_received:
            timestamp = int(time.time()*1000)
            usdatetime = pytz.timezone('US/Eastern').normalize(pytz.timezone('Asia/Shanghai').localize(datetime.now()).astimezone(pytz.timezone('US/Eastern'))).strftime("%Y-%m-%d %H:%M:%S")
            
            try:
                prices = getPrice()
                sys.stdout.write("\rdata hour: %s" % time.strftime("%Y-%m-%d %H:%M:%S") + ": " + ','.join(symbols))
                sys.stdout.flush()
                for symbol in symbols:
                    datas = (prices[symbol].split('"')[1]).split(',')
                    if int(float(datas[5])) == 0:
                        continue
                    try:
                        cursor.execute(symbols_sqls[symbol], (symbol, usdatetime, timestamp, datas[1], datas[2], datas[3], datas[4], datas[5], datas[6], datas[7], datas[8], datas[9], datas[10], datas[12], datas[21], datas[22], datas[23], datas[26], prices[symbol]))
                    except sqlite3.Error, e:
                        print "insertInfoDB Error: %s" % e
                    
                    ustimes = usdatetime.split(" ")[1].split(":")
                    if int(ustimes[0]) >= 18 and int(ustimes[1]) >= 1:
                        self.kill_received = True
                    
                    if lastminute != int(ustimes[1]):
                        lastminute = int(ustimes[1])
                        try:
                            sqlconn.commit()
                        except sqlite3.Error, e:
                            print "sqlconn.commit Error: %s" % e
            except:
                print "liburl error"
            time.sleep(0.5)
        
        print ""
        sqlconn.close()
    
def main():
    global symbols, symbols_sqls
    
    with open('symbols') as f:
        symbols = [line.rstrip('\n') for line in f]
    
    for symbol in symbols:
        symbols_sqls[symbol] =  "INSERT INTO `%s` VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" % (symbol)
    
    threads = []
    thread = GetSinaPriceThread()
    thread.start()
    threads.append(thread)
    
    while len(threads) > 0:
        try:
            # Join all threads using a timeout so it doesn't block
            # Filter out threads which have been joined or are None
            threads = [t.join(1) for t in threads if t is not None and t.isAlive()]
        except KeyboardInterrupt as e:
            print "Ctrl-c received! Sending kill to threads..."
            for t in threads:
                t.kill_received = True
        except Error as e:
            print e
    
if __name__ == '__main__':
    main()
