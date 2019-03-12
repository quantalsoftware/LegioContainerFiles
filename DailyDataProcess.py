#!/usr/bin/env python
import os
import sys
import warnings
import shutil
import time
import datetime
import calendar
import schedule
import pandas as pd
import numpy as np
import mysql.connector as mysql
from fastparquet import ParquetFile, write
import boto3
import logging
import IBTrader
import socket

s3 = boto3.resource('s3')
s3bucketName = 'legio-data'
bucket = s3.Bucket(name=s3bucketName)

s3_StorageLocation = "database_data/"

dbHostContainer = "ec2-3-104-151-3.ap-southeast-2.compute.amazonaws.com"
dbHostPort = "31330"
user = "root"
passwd = "root"

baseCode = ""
clientId=88888
host=""
port=4002
ibConn = IBTrader.IBTrader()


def RequestSpecificMarketData(endtime, timeframe, lookbackSecs):
    global ibConn
    global baseCode
    ibConn.cancelHistoricalData()
    ibConn.historicalData = {}
    
    contract = int(next(iter(ibConn.contracts)))
        
    ibConn.requestHistoricalData(ibConn.contracts[contract],resolution=timeframe, end_datetime='{}'.format(endtime.strftime("%Y%m%d %H:%M:%S")),lookback=str(lookbackSecs)+" S")

    waiting = True
    while waiting:    
        if len(ibConn.historicalData) == len(ibConn.contracts):            
            waiting = False
        else:
            time.sleep(0.01)
    return ibConn.historicalData[baseCode+"_CASH"]

def CheckDatetime(datetime):
    if datetime.weekday() == 6:
        if datetime.hour >= 9:
            return True
        else:
            return False    
    elif datetime.weekday() == 4:
        if datetime.hour < 21:
            return True
        else:
            return False
    elif datetime.weekday() < 4:
        return True
    elif datetime.weekday() == 5:
        return False

def StartSchedule():
    schedule.clear('data-tasks')
    schedule.every().day.at("21:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("22:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("22:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("23:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("23:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("00:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("00:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("01:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("01:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("02:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("02:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("03:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("03:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("04:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("04:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("05:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("05:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("06:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("06:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("07:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("07:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("08:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("08:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("09:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("09:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("10:00").do(ProcessMarketData).tag('data-tasks')    
    schedule.every().day.at("10:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("11:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("11:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("12:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("12:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("13:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("13:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("14:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("14:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("15:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("15:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("16:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("16:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("17:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("17:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("18:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("18:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("19:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("19:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("20:00").do(ProcessMarketData).tag('data-tasks')
    schedule.every().day.at("20:45").do(CheckLastData).tag('data-tasks')
    schedule.every().day.at("21:00").do(ProcessMarketData).tag('data-tasks')
        
def CheckLastData():
    global baseCode
    global dbHostContainer
    global dbHostPort
    global user
    global passwd

    database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
    dbConnection = database.cursor()
    dbConnection.execute("SELECT * FROM "+baseCode+"_Min ORDER BY Timestamp DESC LIMIT 1")
    minuteData = dbConnection.fetchall()
    minuteRequestSecs = (datetime.datetime.now()-datetime.datetime.strptime(list(minuteData[0])[0], '%Y-%m-%d %H:%M:%S')).total_seconds()
    if minuteRequestSecs > 3600:
        minuteData = RequestSpecificMarketData(datetime.datetime.now(), "1 min", minuteRequestSecs)
        minuteData['Vol'] = 0
        for commitData in list(np.array_split(minuteData,500)):
            vals = [tuple(x) for x in commitData.values]
            sql = "INSERT INTO "+baseCode+"_Min (Timestamp, Open, High, Low, Close, Vol) VALUES (%s, %s, %s, %s, %s, %s)"
            database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
            dbConnection = database.cursor()
            dbConnection.executemany(sql, vals)
            database.commit()
    
    database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
    dbConnection = database.cursor()
    dbConnection.execute("SELECT * FROM "+baseCode+"_Hour ORDER BY Timestamp DESC LIMIT 1")
    hourData = dbConnection.fetchall()
    hourRequestSecs = (datetime.datetime.now()-datetime.datetime.strptime(list(hourData[0])[0], '%Y-%m-%d %H:%M:%S')).total_seconds()
    if hourRequestSecs > 3600:
        hourData = RequestSpecificMarketData(datetime.datetime.now(), "1 hour", hourRequestSecs)
        for commitData in list(np.array_split(hourData,50)):
            vals = [tuple(x) for x in commitData.values]
            sql = "INSERT INTO "+baseCode+"_Hour (Timestamp, Open, High, Low, Close) VALUES (%s, %s, %s, %s, %s)"
            database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
            dbConnection = database.cursor()
            dbConnection.executemany(sql, vals)
            database.commit()
    
    print("Data Checked and Updated")

def ProcessMarketData():
    global ibConn
    global baseCode
    global dbHostContainer
    global dbHostPort
    global user
    global passwd

    ibConn.cancelHistoricalData()
    ibConn.historicalData = {}
    
    startTime = datetime.datetime.now()
    processToTime = datetime.datetime(startTime.year, startTime.month, startTime.day, startTime.hour, 0, 0)
        
    contract = int(next(iter(ibConn.contracts)))
    ibConn.requestHistoricalData(ibConn.contracts[contract],resolution="1 min", end_datetime='{}'.format(processToTime.strftime("%Y%m%d %H:%M:%S")),lookback="3600 S")

    waiting = True
    while waiting:    
        if len(ibConn.historicalData) == len(ibConn.contracts):            
            waiting = False
        else:
            time.sleep(0.01)
    minutes = ibConn.historicalData[baseCode+"_CASH"]
    minutes['Vol'] = 0
    
    ibConn.historicalData = {}
    ibConn.requestHistoricalData(ibConn.contracts[contract],resolution="1 hour", end_datetime='{}'.format(processToTime.strftime("%Y%m%d %H:%M:%S")),lookback="3600 S") 
    waiting = True
    while waiting:    
        if len(ibConn.historicalData) == len(ibConn.contracts):            
            waiting = False
        else:
            time.sleep(0.01)
    hours = ibConn.historicalData[baseCode+"_CASH"]   
    
    for commitData in list(np.array_split(minutes,10)):
        vals = [tuple(x) for x in commitData.values]
        sql = "INSERT INTO "+baseCode+"_Min (Timestamp, Open, High, Low, Close, Vol) VALUES (%s, %s, %s, %s, %s, %s)"
        database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
        dbConnection = database.cursor()
        dbConnection.executemany(sql, vals)
        database.commit()

    for commitData in list(np.array_split(hours,10)):
        vals = [tuple(x) for x in commitData.values]
        sql = "INSERT INTO "+baseCode+"_Hour (Timestamp, Open, High, Low, Close) VALUES (%s, %s, %s, %s, %s)"
        database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
        dbConnection = database.cursor()
        dbConnection.executemany(sql, vals)
        database.commit()
    
    #return [hours,minutes]

def main():
    global ibConn
    global baseCode
    
    print("Connecting to IB Gateway")
    print("Client ID: "+str(clientId))
    print("Host ID: "+str(host))
    ibConn = IBTrader.IBTrader()
    time.sleep(3)
    ibConn.connect(clientId=clientId, host=host, port=port)
    time.sleep(5)

    ibConn.contracts = {}
    time.sleep(5)
    ibConn.contracts = {}
    ibConn.createCashContract(baseCode[:3], currency=baseCode[3:])    
    print("Adding: "+baseCode)
    print("Contracts Processing: "+str(len(ibConn.contracts)))

    StartSchedule()
    schedule.every().day.at("22:00").do(StartSchedule)

    while CheckDatetime(datetime.datetime.now()):
        if ibConn.connected:
            schedule.run_pending()
        else:
            ibConn.connect(clientId=clientId, host=host, port=port)        

        time.sleep(0.2)    

    print("Processor Shutting Down")
    ibConn.cancelHistoricalData()
    ibConn.cancelMarketData()
    ibConn.contracts = {}
    ibConn.disconnect()
    print("IB Connection Closed")
    print("Processor Complete")

if __name__ == '__main__':
    print("Initialising")
    boto3.set_stream_logger('', logging.INFO)
    warnings.filterwarnings("ignore")
        
    print("Retrieving Market Symbol Code")
    baseConfigInfo = pd.read_csv("./BaseConfigInfo.csv")
    containerIP = socket.gethostbyname(socket.gethostname())
    thisContainer = baseConfigInfo.loc[baseConfigInfo['IPAddress']==containerIP]
    baseCode = thisContainer['Symbol'].item()
    clientId = thisContainer['IB Client ID'].item()
    host = thisContainer['IBGatewayIP'].item()

    main()