import mysql.connector


class QuerryProcessor:
    #Memberr Variable Declaration
    host="localhost"
    user="dds"
    password="proj@dds"
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

    def executeQuery(self, query, toPrint):
        mydb = self.connectToDatabase('node1')
        try:
            mycursor = mydb.cursor()
            mycursor.execute(query)  
            #querySplit = query.split(' ')
            #if querySplit[0]=='update' or querySplit[0]=='UPDATE' or querySplit[0]=='delete' or querySplit[0]=='DELETE':
            print('Rows Affected: %d' % (mycursor.rowcount))
            
            if toPrint:
                if mycursor.with_rows:
                    for y in mycursor.fetchall():
                        print(y)     
        except:
            print('Query failed on single execution \n')
        mycursor.close()
        mydb.close()

    def executeQueryAllNodes(self,query):
        for x in self.allDatabases:
            mydb = self.connectToDatabase(x)
            try:
                mycursor = mydb.cursor()
                mycursor.execute(query)  
                querySplit = query.split(' ')
                if querySplit[0]=='update' or querySplit[0]=='UPDATE' or querySplit[0]=='delete' or querySplit[0]=='DELETE':
                    print('Rows Affected: %d' % (mycursor.rowcount))
                
                if mycursor.with_rows:
                    for y in mycursor.fetchall():
                        print(y)     
            except:
                #pass
                print('Query failed on ' + x + '\n')  
        mycursor.close()
        mydb.close()
    def aggregateQuery(self, query):
        allRows = []
        Rows = []
        rowCount=0
        colCount=0
        for x in self.allDatabases:
            mydb = self.connectToDatabase(x)
            try:
                mycursor = mydb.cursor()
                mycursor.execute(query)  
                querySplit = query.split(' ')
                if querySplit[0]=='update' or querySplit[0]=='UPDATE' or querySplit[0]=='delete' or querySplit[0]=='DELETE':
                    print('Rows Affected: %d' % (mycursor.rowcount))
                
                if mycursor.with_rows:
                    Rows = mycursor.fetchall()
                    allRows+=Rows
                    for y in mycursor:
                        print(y)  
            except:
                #pass
                print('Query failed on ' + x + '\n')
        #print(allRows)
        rowCount = len(allRows)

        if rowCount>0:
            colCount = len(allRows[0])
            queryTemp = 'create table temp (date varchar(15), home_team varchar(15), away_team varchar(15), home_score varchar(15), away_score varchar(15), tournament varchar(15), city varchar(15), country varchar(15), neutral varchar(15), primary key (date,home_team,away_team));'
            self.executeQuery(queryTemp,False)
        t_string = ""
        for x in range(colCount-1):
            t_string+='%s, '
        t_string+='%s'
        # print(colCount)
        # print(rowCount)
        # print(t_string)

        for row in allRows:
            print(len(row))
            var_string = ', '.join('?' * len(row))
            queryTemp = 'INSERT INTO temp VALUES ('+ t_string +');' 
            mydb = self.connectToDatabase('node1')
            mycursor = mydb.cursor()
            mycursor.execute(queryTemp,row)
            mycursor.close()
            mydb.close()

        querySplit = query.split(' ')
        ind=0
        for x in querySplit:
            if x=='FROM' or x=='from':
                break
            ind+=1
        ind+=1
        querySplit[ind]='temp'
        newQuery = ''
        for x in querySplit:
            newQuery+=x + ' '
        #print(newQuery)
        self.executeQuery(newQuery,True)
        self.executeQuery('select * from temp',True)
        self.executeQuery('drop table temp',False)

        

            

    def main(self):
        query = input("Enter query without joins \n")
        querySplit = query.split(' ')
        if 'order' in querySplit and 'by' in querySplit:
            self.aggregateQuery(query)
        else:
            self.executeQueryAllNodes(query)


if __name__ == "__main__":
    print('Querry processor called: ')
    qp = QuerryProcessor()
    qp.main()



