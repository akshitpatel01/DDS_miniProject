import mysql.connector
import getpass
import pandas as pd
import random 
import time
import re

def shard(user, password, non, shardingType, sql, desField, rangesize):
    db = mysql.connector.connect(
        host="localhost",
        user = user,
        password = password  
    )
    dbcursor = db.cursor()   
    sqlsplit=re.split(',|\(|\)',sql)
    flag=0
    j=0
    for i in range(len(sqlsplit)):
        if flag!=0 and i-j==flag:
            break
        if sqlsplit[i].strip()==desField:
            flag=i
        if sqlsplit[i].strip().lower()=='values':
            j=i

    if shardingType==1:
        # sharding approach 1: Enumeration Algorithm
        dsf=int(sqlsplit[i].strip())
        dsf=dsf%non
        dbname="node"+str(dsf)
    elif shardingType==2:
        # sharding approach 2: Range partition Algorithm
        dsf=int(sqlsplit[i].strip())
        dsf=dsf/rangesize
        dsf=int(dsf)
        dsf=dsf%non
        dbname="node"+str(dsf)
    else:
        # sharding approach 3: Date partition Algorithm
        ts=int(time.time())
        ts=ts%non
        dbname="node"+str(ts)
    try:
        dbcursor.execute("USE "+dbname)
    except:    
        print("Unable to access database(Maybe the database doesn't exist on the required node).")

    
    try:            
        dbcursor.execute(sql)
    except:
        print("Unable to insert.")
        # dbcursor.execute("CREATE TABLE football (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, home_team VARCHAR(50), away_team VARCHAR(50), home_score INT, away_score INT, tournament VARCHAR(50), city VARCHAR(50), country VARCHAR(50), neutral VARCHAR(10))")
        # dbcursor.execute("INSERT into football (date, home_team, away_team, home_score, away_score, tournament, city, country, neutral ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(j.iloc[0],j.iloc[1],j.iloc[2],j.iloc[3],j.iloc[4],j.iloc[5],j.iloc[6],j.iloc[7],j.iloc[8]))
            
    db.commit()

# sqlsplit="INSERT into football (date , home_team , away_team , home_score, away_score, tournament, city, country, neutral ) values (%s,%s,3333,%s,%s,%s,%s,%s,%s)"
# sqlsplit=re.split(',|\(|\)',sqlsplit)
# print(sqlsplit)
# flag=0
# j=0
# for i in range(len(sqlsplit)):
#     if flag!=0 and i-j==flag:
#         break
#     if sqlsplit[i].strip()=='away_team':
#         flag=i
#     if sqlsplit[i].strip().lower()=='values':
#         j=i
     
# print(i)
# print(j)
# print(flag)
# print(sqlsplit[i].strip())    