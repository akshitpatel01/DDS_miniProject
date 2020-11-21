import mysql.connector
import getpass
import pandas as pd
import random 

user = input("Enter username: ")
password = getpass.getpass(prompt="Enter password: ", stream=None)

db = mysql.connector.connect(
    host="localhost",
    user = user,
    password = password  
)

# sql=input("Insert query: ")
df=pd.read_csv("results.csv")
non=4
for i,j in df.iterrows():    
    rnd=random.randint(1,non)
    dbcursor = db.cursor()
    dbname="node"+str(rnd)

    try:
        dbcursor.execute("USE "+dbname)
    except:    
        dbcursor.execute("CREATE DATABASE " + dbname)
        print("Unable to access database(Maybe the database doesn't exist on this node). Created a new database with this name.")

    dbcursor.execute("USE " + dbname)


    try:            
        dbcursor.execute("INSERT into football (date, home_team, away_team, home_score, away_score, tournament, city, country, neutral ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(j.iloc[0],j.iloc[1],j.iloc[2],j.iloc[3],j.iloc[4],j.iloc[5],j.iloc[6],j.iloc[7],j.iloc[8]))
    except:
        print("Unable to insert, creating a new table")
        dbcursor.execute("CREATE TABLE football (id INT AUTO_INCREMENT PRIMARY KEY, date DATE, home_team VARCHAR(50), away_team VARCHAR(50), home_score INT, away_score INT, tournament VARCHAR(50), city VARCHAR(50), country VARCHAR(50), neutral VARCHAR(10))")
        dbcursor.execute("INSERT into football (date, home_team, away_team, home_score, away_score, tournament, city, country, neutral ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(j.iloc[0],j.iloc[1],j.iloc[2],j.iloc[3],j.iloc[4],j.iloc[5],j.iloc[6],j.iloc[7],j.iloc[8]))
        
db.commit()