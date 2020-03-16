import os
import time
start_time1 = time.time()


f = open('./requirement.txt')
lst=f.read()
lst1=[]
lib=''
words=lst.split('\n')
lib=words[0]
a1=words[1]
a2=words[2]
a3=words[3]
a1new=a1[9:]
a2new=a2[9:]
a3new=a3[9:]
print('installing packages.')
word1=lib.split(',')
for packag1 in word1:
    package = packag1
    try:
        __import__package
    except:
        try:
            os.system("python3 -m pip install --user "+ package)
        except:
            os.system("python3 -m pip install --user "+ package)

print('packages installed')
hostname=a1new
username=a2new
password=a3new
print('connecting to database')
import pymysql
amfi_masterConnection=pymysql.connect(hostname,username,password,autocommit=True)
Cursor=amfi_masterConnection.cursor()
fb=open('./install/amfiDDL.sql','r')
sqlfile=fb.read()
fb.close()
sqlCommands = sqlfile.split(';')
for cmd in sqlCommands:
    try:
        Cursor.execute(cmd)
    except:
        print("Command on: ")

print('database connected and table is created')

Cursor.close()

