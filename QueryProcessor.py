import mysql.connector
from mysql.connector import FieldType


class QuerryProcessor:
    #Member Variable Declaration
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

    def executeQuery(self, name, query, toPrint):
        mydb = self.connectToDatabase(name)
        try:
            mycursor = mydb.cursor()
            mycursor.execute(query)  
            #querySplit = query.split(' ')
            #if querySplit[0]=='update' or querySplit[0]=='UPDATE' or querySplit[0]=='delete' or querySplit[0]=='DELETE':
            #print('Rows Affected: %d' % (mycursor.rowcount))
            
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

    def createTempTable(self, name, typeDict, tableDescription):
        queryTemp = 'create table '+ name + ' ('
        for i in range(len(tableDescription)):
            desc = tableDescription[i]
            queryTemp += desc[0] + ' ' +typeDict[desc[1]]
            if i<len(tableDescription)-1:
                queryTemp += ', '
            
        queryTemp += ')'
        return queryTemp

    def executeQueryAllNodesRet(self,query):
        allRows = []
        Rows = []
        tableDescription = []
        for x in self.allDatabases:
            mydb = self.connectToDatabase(x)
            try:
                mycursor = mydb.cursor()
                mycursor.execute(query)  
                if tableDescription == []:
                   tableDescription = mycursor.description
                #print (tableDescription)
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
        return (allRows,tableDescription)
    def createInsertTempTable(self, name, dataBase, allRows, tableDescription):
        #tableDescription = []
        typeDict = {
            3: ' int',
            253: ' varchar(500)'
        }
        colCount = 0
        rowCount = len(allRows)

        if rowCount>0:
            colCount = len(allRows[0])
            queryTemp = self.createTempTable(name,typeDict,tableDescription) #'create table temp (date varchar(15), home_team varchar(15), away_team varchar(15), home_score varchar(15), away_score varchar(15), tournament varchar(15), city varchar(15), country varchar(15), neutral varchar(15), primary key (date,home_team,away_team));'
            #print(queryTemp)
            self.executeQuery(dataBase, queryTemp,False)
        t_string = ""
        for x in range(colCount-1):
            t_string+='%s, '
        t_string+='%s'
        # print(colCount)
        # print(rowCount)
        # print(t_string)

        for row in allRows:
            #print(len(row))
            var_string = ', '.join('?' * len(row))
            queryTemp = 'INSERT INTO '+ name + ' VALUES ('+ t_string +');' 
            mydb = self.connectToDatabase(dataBase)
            mycursor = mydb.cursor()
            mycursor.execute(queryTemp,row)
            mycursor.close()
            mydb.close()

        
    def aggregateQuery(self, query):
        allRows = []
        Rows = []
        rowCount=0
        colCount=0
        tableDescription = []
        typeDict = {
            3: ' int',
            253: ' varchar(500)'
        }
        for x in self.allDatabases:
            mydb = self.connectToDatabase(x)
            try:
                mycursor = mydb.cursor()
                mycursor.execute(query)  
                if tableDescription == []:
                    tableDescription = mycursor.description
                #print (tableDescription)
                querySplit = query.split(' ')
                if querySplit[0]=='update' or querySplit[0]=='UPDATE' or querySplit[0]=='delete' or querySplit[0]=='DELETE':
                    print('Rows Affected: %d' % (mycursor.rowcount))
                
                if mycursor.with_rows:
                    Rows = mycursor.fetchall()
                    allRows+=Rows
                    #for y in mycursor:
                    #    print(y)  
            except:
                #pass
                print('Query failed on ' + x + '\n')
        #print(allRows)
        rowCount = len(allRows)

        if rowCount>0:
            colCount = len(allRows[0])
            queryTemp = self.createTempTable('temp',typeDict,tableDescription) #'create table temp (date varchar(15), home_team varchar(15), away_team varchar(15), home_score varchar(15), away_score varchar(15), tournament varchar(15), city varchar(15), country varchar(15), neutral varchar(15), primary key (date,home_team,away_team));'
            #print(queryTemp)
            self.executeQuery('node1',queryTemp,False)
        t_string = ""
        for x in range(colCount-1):
            t_string+='%s, '
        t_string+='%s'
        # print(colCount)
        # print(rowCount)
        # print(t_string)

        for row in allRows:
            #print(len(row))
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
        print(newQuery)
        self.executeQuery('node1',newQuery,True)
        #self.executeQuery('select * from temp',True)
        self.executeQuery('node1','drop table temp',False)

    def joinQuery(self,query):
        table1 = ''
        table2 = ''
        ind1 = 0
        ind2 = 0
        querySplit = query.split(' ')
        for i in querySplit:
            if i=='FROM' or i=='from':
                break
            ind1+=1
        ind1+=1
        table1 = querySplit[ind1]
    
        for i in querySplit:
            if i=='JOIN' or i=='join':
                break
            ind2+=1
        ind2+=1
        table2 = querySplit[ind2]

        
        entireTable1,descTable1 = self.executeQueryAllNodesRet('select * from '+table1)
        #print(entireTable1)
        #print(descTable1)
        entireTable2,descTable2 = self.executeQueryAllNodesRet('select * from '+table2)

        self.createInsertTempTable('temp1', 'node1',entireTable1, descTable1)
        self.createInsertTempTable('temp2', 'node1',entireTable2, descTable2)

        newQuery = ''

        ind=0
        for x in querySplit:
            split = x.split('.')
            if len(split)>1:
                if split[0]==table1:
                    newQuery+='temp1.'+split[1] + ' '
                if split[0]==table2:
                    newQuery+='temp2.'+split[1] + ' '
            else:
                newQuery += x
        #print(table1)
        #print(table2)
        newQuery = query.replace(table1,'temp1').replace(table2,'temp2')
        #newQuery = query.replace(table2,'temp2')

        print(newQuery)
        # newQuery[ind1] = 'temp1'
        # newQuery[ind2] = 'temp2'

        self.executeQuery('node1',newQuery,True)
        #self.executeQuery('select * from temp',True)
        self.executeQuery('node1','drop table temp1',False)
        self.executeQuery('node1','drop table temp2',False)


    def semijoinQuery(self,query):
        table1 = ''
        table2 = ''
        ind1 = 0
        ind2 = 0
        attributes = []
        querySplit = query.split(' ')
        for i in querySplit:
            if i=='FROM' or i=='from':
                break
            ind1+=1
        ind1+=1
        table1 = querySplit[ind1]
    
        for i in querySplit:
            if i=='JOIN' or i=='join':
                break
            ind2+=1
        ind2+=1
        table2 = querySplit[ind2]

        for x in querySplit:
            split = x.split('.')
            if len(split)>1 and split[0]==table1:
                attributes.append(split[1])
        #print (attributes)
        resultT = []
        tmp = []
        for x in self.allDatabases:
            #if x=='node1':
            mydb = self.connectToDatabase(x)
            try:
                mycursor = mydb.cursor()
                mycursor.execute('select distinct ' + attributes[0] + ' from ' + table1)  
                #querySplit = query.split(' ')
                #if querySplit[0]=='update' or querySplit[0]=='UPDATE' or querySplit[0]=='delete' or querySplit[0]=='DELETE':
                #print('Rows Affected: %d' % (mycursor.rowcount))
                
                if mycursor.with_rows:
                    tmp = mycursor.fetchall()     
                    resultT += tmp
                    
            except:
                #pass
                print('Query failed on ' + x + '\n')  
            #print(resultT)
            result = list(resultT)
            incomingTuplesAll = []
            incomingTuples = []
            for tupl in resultT:
                tup=''
                tup = ''.join(str(tupl))
                # str1=''
                # str1 = ''.join(str(tup))
                # #tup = ''.join(tupl)
                # print(type(tup))
                # print(tup)
                # print(type(str1))
                
                tup = tup.split(',')[0].split('(')[1]
                #print(tup)
                for y in self.allDatabases:
                    tableDescription = []
                    mydb2 = self.connectToDatabase(y)
                    try:
                        mycursor2 = mydb2.cursor()
                        mycursor2.execute('select * from '+ table2 +' where ' + attributes[0] + ' = ' + tup)
                        if mycursor2.with_rows:
                            incomingTuples = mycursor2.fetchall()
                            if tableDescription == []:
                                tableDescription = mycursor2.description
                            incomingTuplesAll += incomingTuples
                    except:
                        print ('Query failed in extracting tuples on ' + y + '\n')
                    if len(incomingTuples)>0:
                        self.createInsertTempTable('temp1',x,incomingTuples,tableDescription)
                        self.executeQuery(x,query.replace(table2,'temp1'),True)
                        #print(query.replace(table2,'temp1'))
                        self.executeQuery(x,'drop table temp1',False)
    

        mycursor.close()
        mydb.close()


    def __init__(self):
        host="localhost"
        user="akshit"
        password="1234"
        database=''
        allDatabases = ['node1','node2','node3','node4']


    def main(self,query):
        #query = input("Enter query \n")
        querySplit = query.split(' ')
        if 'inner' in querySplit and 'join' in querySplit:
            self.semijoinQuery(query)
        elif 'order' in querySplit and 'by' in querySplit:
            self.aggregateQuery(query)
        else:
            self.executeQueryAllNodes(query)


if __name__ == "__main__":
    print('Querry processor called: ')
    qp = QuerryProcessor()
    qp.main()



