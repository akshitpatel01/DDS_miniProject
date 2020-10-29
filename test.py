import mysql.connector


class QuerryProcessor:
    #Memberr Variable Declaration
    host="localhost"
    user="akshit"
    password="1234"
    database=''
    allDatabases = ['node1','node2','node3','node4']

    def connectToDatabase(self, database):
        self.database=database
        mydb = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            autocommit=True
        )
        return mydb

    def executeQueryAllNodes(self,query):
        for x in self.allDatabases:
            mydb = self.connectToDatabase(x)
            try:
                mycursor = mydb.cursor()
                mycursor.execute(query)  
                querySplit = query.split(' ')
                if querySplit[0]=='update' or querySplit[0]=='UPDATE':
                    print('Rows Affected: %d' % (mycursor.rowcount))
                #count = mycursor.rowcount
                
                if mycursor.with_rows:
                    for y in mycursor.fetchall():
                        print(y)     
            except:
                #pass
                print('Query failed on ' + x + '\n')

    

    
    def main(self):
        query = input("Enter query without joins \n")
        self.executeQueryAllNodes(query)


if __name__ == "__main__":
    print('Querry processor called: ')
    qp = QuerryProcessor()
    qp.main()



