#!/usr/bin/env python
import os
import sys
import warnings
import shutil
import time
import datetime
import calendar
#import schedule
import pandas as pd
import mysql.connector as mysql
from fastparquet import ParquetFile, write

import IBTrader
import boto3
import logging

def main():
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


    for contract in ibConn.contracts:
        #baseCode = ibConn.contract_details[contract]['m_summary']['m_localSymbol'].replace('.','')
        print("Processing: "+baseCode)
        
        print("Retrieving Hourly Data")
        ibConn.requestHistoricalData(ibConn.contracts[contract],resolution="1 hour", 
                                    end_datetime='{} 22:00:00'.format((datetime.datetime.today()).strftime("%Y%m%d")),lookback="5 Y")
        waiting = True
        lastLen = 0
        while waiting:
            try:
                if len(ibConn.historicalData[baseCode+'_CASH']) > lastLen:
                    lastLen = len(ibConn.historicalData[baseCode+'_CASH'])
                    time.sleep(2)
                else:
                    waiting = False
            except KeyError:
                pass
        time.sleep(5)
        
        lookbackDays = (datetime.datetime.strptime(ibConn.historicalData[baseCode+'_CASH'].sort_values('datetime').iloc[0].name,"%Y-%m-%d %H:%M:%S")-datetime.datetime(2014,1,1,0,0,0)).days
        endDate = datetime.datetime.strptime(ibConn.historicalData[baseCode+'_CASH'].sort_values('datetime').iloc[0].name,"%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
        
        ibConn.requestHistoricalData(ibConn.contracts[contract],resolution="1 hour", 
                                    end_datetime='{} 22:00:00'.format(endDate),lookback=str(lookbackDays)+" D")
        waiting = True
        lastLen = 0
        while waiting:
            try:
                if len(ibConn.historicalData[baseCode+'_CASH']) > lastLen:
                    lastLen = len(ibConn.historicalData[baseCode+'_CASH'])
                    time.sleep(2)
                else:
                    waiting = False
            except KeyError:
                pass
        time.sleep(5)       
        
        print("Saving Hourly Data")
        hourlyData = ibConn.historicalData[baseCode+'_CASH']
        hourlyData = hourlyData.drop(['V','OI','WAP'],1).reset_index().sort_values('datetime')
        filename = baseCode+'_H'+str((datetime.datetime.today()).strftime("%Y%m%d"))+'.parq'
        write('/root/IBConnection/data/hour/'+filename,hourlyData)
        bucket.upload_file('/root/IBConnection/data/hour/'+filename, s3_StorageLocation+filename)
        ibConn.historicalData = {}    

        years = [2018,2017,2016,2015,2014]

        for year in years:
            print("Retrieving Minute Data "+str(year))
            for i in range(12,0,-1):
                d = datetime.datetime(year,i,calendar.monthrange(year,i)[1])
                dateStr = d.strftime("%Y%m%d")
                print("Month: "+str(d.strftime("%Y %m")))
                for contract in ibConn.contracts:
                    baseCode = ibConn.contract_details[contract]['m_summary']['m_localSymbol'].replace('.','')
                    print("\tProcessing: "+baseCode)

                    ibConn.requestHistoricalData(ibConn.contracts[contract],resolution="1 min", 
                                            end_datetime='{} 22:00:00'.format(dateStr),lookback="1 M")
                    waiting = True
                    lastLen = 0
                    while waiting:
                        try:
                            if len(ibConn.historicalData[baseCode+'_CASH']) > lastLen:
                                lastLen = len(ibConn.historicalData[baseCode+'_CASH'])
                                #print("\tBars Received: "+str(lastLen))
                                time.sleep(10)
                            else:
                                waiting = False
                        except KeyError:
                            pass
                    time.sleep(5)

                    minuteData = ibConn.historicalData[baseCode+'_CASH']
                    minuteData = minuteData.drop(['V','OI','WAP'],1).reset_index().sort_values('datetime')
                    filename = baseCode+'_M'+str(d.strftime("%Y_%m"))+'.parq'
                    write('/root/IBConnection/data/min/'+filename,minuteData)
                    bucket.upload_file('/root/IBConnection/data/min/'+filename, s3_StorageLocation+"min/"+filename)
                    ibConn.historicalData = {}


        ibConn.historicalData = {}
        ibConn.cancelHistoricalData()
        ibConn.cancelMarketData()
        ibConn.contracts = {}
        ibConn.disconnect()
        print("Collection Complete")
        exit()

if __name__ == '__main__':
    print("Initialising")
    boto3.set_stream_logger('', logging.INFO)
    warnings.filterwarnings("ignore")
    s3 = boto3.resource('s3')
    s3bucketName = 'quantal-general'
    bucket = s3.Bucket(name=s3bucketName)
    s3_StorageLocation = "Legio/data/database_data/"

    baseCode = ""
    clientId=""
    host=""
    port=4002

    print("Retrieving Market Symbol Code")
    baseFiles = os.listdir("/")
    baseCode = [x[len(x)-6:] for x in baseFiles if "MarketCode_" in x][0]

    baseConfigInfo = pd.read_csv("./BaseConfigInfo.csv")
    thisContainer = baseConfigInfo.loc[baseConfigInfo['Symbol']==baseCode]
    clientId = thisContainer['IB Client ID'].item()
    host = thisContainer['IBGatewayIP'].item()

    main()
