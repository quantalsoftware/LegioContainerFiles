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
    ibConn.historicalData = {}
    
    contract = int(next(iter(ibConn.contracts)))
    ibConn.requestHistoricalData(ibConn.contracts[contract],resolution=timeframe, 
                                         end_datetime="{}".format(endtime.strftime("%Y%m%d %H:%M:%S")),
                                         lookback=str(lookbackSecs)+" S")
    while not ibConn.dataReqComplete:
        print(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"), end='\r')
        time.sleep(0.2)
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
    global ibConn
    global baseCode
    global dbHostContainer
    global dbHostPort
    global user
    global passwd

    ibConn.historicalData = {}

    endtime = datetime.datetime.utcnow()

    database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
    dbConnection = database.cursor()
    dbConnection.execute("SELECT * FROM "+baseCode+"_Min ORDER BY Timestamp DESC LIMIT 1")
    minuteData = dbConnection.fetchall()

    timeframe = "1 min"
    timeChunk = 25000
    minDF = pd.DataFrame()
    
    lookbackSecs = int((endtime-datetime.datetime.strptime(list(minuteData[0])[0], '%Y-%m-%d %H:%M:%S')).total_seconds())

    if lookbackSecs > 3600:
        lastDBDate = endtime - datetime.timedelta(seconds=lookbackSecs)    
        currentEndDate = endtime
        for i in range(int(lookbackSecs/timeChunk)+1):
            if (lookbackSecs - (timeChunk*i)) > timeChunk:
                minDF = pd.concat([minDF,RequestSpecificMarketData(currentEndDate, timeframe, timeChunk)])
            else:
                minDF = pd.concat([minDF,RequestSpecificMarketData(currentEndDate, timeframe, int((currentEndDate - lastDBDate).total_seconds()))])                    
            currentEndDate = currentEndDate-datetime.timedelta(seconds=timeChunk)
        if len(minDF) > 0:
            minDF.drop(['V','OI','WAP'],1,inplace=True)
            minDF['Vol']=0
            minDF = minDF.sort_values('datetime').reset_index()
             
            for commitData in list(np.array_split(minDF,1000)):
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

    timeframe = "1 hour"    
    hourDF = pd.DataFrame()
    
    lookbackSecs = int((endtime-datetime.datetime.strptime(list(hourData[0])[0], '%Y-%m-%d %H:%M:%S')).total_seconds())

    if lookbackSecs > 3600:
        lastDBDate = endtime - datetime.timedelta(seconds=lookbackSecs)    
        currentEndDate = endtime
        for i in range(int(lookbackSecs/timeChunk)+1):
            if (lookbackSecs - (timeChunk*i)) > timeChunk:
                hourDF = pd.concat([hourDF,RequestSpecificMarketData(currentEndDate, timeframe, timeChunk)])
            else:
                hourDF = pd.concat([hourDF,RequestSpecificMarketData(currentEndDate, timeframe, int((currentEndDate - lastDBDate).total_seconds()))])                    
            currentEndDate = currentEndDate-datetime.timedelta(seconds=timeChunk)
        if len(hourDF) > 0:
            hourDF.drop(['V','OI','WAP'],1,inplace=True)
            hourDF = hourDF.sort_values('datetime').reset_index()
            
            for commitData in list(np.array_split(hourDF,50)):
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
    
    startTime = datetime.datetime.utcnow()
    processToTime = datetime.datetime(startTime.year, startTime.month, startTime.day, startTime.hour, 0, 0)    
    timeChunk = 3600
    
    timeframe = "1 min"
    minutes = RequestSpecificMarketData(processToTime, timeframe, timeChunk)
    minutes.drop(['V','OI','WAP'],1,inplace=True)
    minutes['Vol'] = 0
    minutes = minutes.sort_values('datetime').reset_index()
    
    timeframe = "1 hour"
    hours = RequestSpecificMarketData(processToTime, timeframe, timeChunk)
    hours.drop(['V','OI','WAP'],1,inplace=True)
    hours = hours.sort_values('datetime').reset_index()
    
    if len(minutes) > 0:   
        for commitData in list(np.array_split(minutes,10)):
            vals = [tuple(x) for x in commitData.values]
            sql = "INSERT INTO "+baseCode+"_Min (Timestamp, Open, High, Low, Close, Vol) VALUES (%s, %s, %s, %s, %s, %s)"
            database = mysql.connect(host=dbHostContainer,port=dbHostPort,user=user,passwd=passwd,database=baseCode)
            dbConnection = database.cursor()
            dbConnection.executemany(sql, vals)
            database.commit()
    if len(hours) > 0:
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