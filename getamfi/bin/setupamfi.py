import configamfi
from configamfi import hostname, username,password,start_time1
import os
import numpy as np
import pandas as pd
import datetime
import re
import pymysql
import requests
import urllib.request
import glob
import pymysql
import time
def dateparser(dt):
    d=str(dt)[:2]
    m=str(dt)[3:6]
    y=str(dt)[7:]
    m=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'].index(m)+1
    return(str(datetime.date(int(y),m,int(d))))

print('function created ready to use ')

print('connecting to database and selecting max time from exiting table')
amfi_masterConnection=pymysql.connect(hostname,username,password,'amfi_data',autocommit=True)
cur=amfi_masterConnection.cursor()
cur.execute("SELECT max(date) FROM amfihistory")
row = cur.fetchall()
maxtime=str(row[0][0])
cur.close()
if maxtime == 'None':
    maxtime=0
print('max timestamp retrive')
lsty=[]
l1=[2014,2015,2016,2017,2018,2019,2020]
for i in os.listdir('./data/'):
    lsty.append(i[4:8])
lst=[str(i) for i in l1]
try:
    for y in lst:
        print('data downloading for '+y)
        df_master=pd.DataFrame(columns=['Scheme Code', 'Scheme Name', 'ISIN Div Payout/ISIN Growth', 'ISIN Div Reinvestment', 'Net Asset Value', 'Repurchase Price', 'Sale Price', 'Date'])
        urllst=["http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Jan-"+y+"&todt=31-Dec-"+y]
        for url in urllst:
            lst=[]
            for line in urllib.request.urlopen(url):
                line=line.decode('utf-8')
                line=line.split(';')
                if ((line!=[' \r\n'])&(line!=['\r\n'])):
                    lst.append(line)
            lstmaster=[]
            for i in lst:
                if len(i)==8:
                    lstmaster.append(tuple(i))
            df=pd.DataFrame(lstmaster[1:],columns=lstmaster[0])
            df.columns=df.columns.str.replace('\r\n','')
            df.Date=df.Date.apply(lambda x : dateparser(x.replace('\r\n','')))
            df_master=pd.concat([df_master,df],axis=0)
        df_master=df_master.rename(columns={ "Scheme Code": "SchemeCode","Scheme Name":"SchemeName"})
        df_master=df_master.rename(columns={ "ISIN Div Payout/ISIN Growth": "ISINDivPayoutISINGrowth","ISIN Div Reinvestment":"ISINDivReinvestment"})
        df_master=df_master.rename(columns={ "Net Asset Value": "NetAssetValue","Repurchase Price":"RepurchasePrice"})
        df_master=df_master.rename(columns={ "Sale Price": "SalePrice"})
        #df_master1=df_master[df_master.Date > '2020-02-07']
        df_master.to_csv("./data/getAMFI"+y+".csv")
except:
    print("No data to extract...")
print('data downloaded for all')
print('merge all downloaded files ....')
owd = os.getcwd()
os.chdir("./data")
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ],sort=True)
combined_csv.to_csv( "combineamfi.csv", index=False, encoding='utf-8-sig')
os.chdir(owd)
df_master1 = pd.read_csv("./data/combineamfi.csv") 
df_master1 = df_master1[['SchemeCode','SchemeName','ISINDivPayoutISINGrowth',
 'ISINDivReinvestment',
 'NetAssetValue',
 'RepurchasePrice',
 'SalePrice','Date']] 
print('Data is combined successfully')
df_master1=df_master1[df_master1.Date> str(maxtime)]
df_master1=df_master1.fillna(0)
#df_master1=df_master1.head(10000)
print('data loading to database')
import pymysql
print('establishing connection ')
amfi_masterConnection=pymysql.connect(hostname,username,password,'amfi_data',autocommit=True)
cur1=amfi_masterConnection.cursor()
print('Connection  done')
c=0
insertSQL="INSERT INTO amfihistory(SchemeCode, SchemeName,ISINDivPayoutISINGrowth,ISINDivReinvestment,NetAssetValue,RepurchasePrice,SalePrice, Date) VALUES"
print('insertion start--->>>')
start_time=time.time()
for i in df_master1.index:
    val=str(tuple(df_master1.iloc[i,:]))
    cur1.execute(insertSQL+' '+val+';')
    c=c+1
    if c%10000==0:
        print("---time taken for 10000 records:: %s seconds ---" % (time.time() - start_time))
        start_time=time.time()
        
cur1.close()
print('data is loaded successfully ')
print("---total time taken :: %s seconds ---" % (time.time() - start_time1))





