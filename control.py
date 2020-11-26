import mysql.connector
from QueryProcessor import QuerryProcessor
from sharding import shard

while 1:
    numNodes = 4
    print('1. Sharding')
    print('2. Other queries')
    print('3. Exit')
    sw = int(input('Enter choice: '))
    if sw==1:
        username = input('Enter username: ')
        password = input('Enter password: ')
        query = input('Enter query: ')
        desField = 'id'
        rangeSize = 0
        sharding_algo_number = int(input('Enter sharding algorithm number: '))
        if sharding_algo_number==2:
            rangeSize = 100
        shard(username,password,numNodes,sharding_algo_number,query,desField,rangeSize) #Call to sharding module
    elif sw==2:
        username = input('Enter username: ')
        password = input('Enter password: ')
        query = input('Enter query: ')
        querySplit = query.split('-')
        listDatabases = []
        if len(querySplit) > 1:
            listDatabases = querySplit[1].split(',')
        qp = QuerryProcessor(listDatabases,username,password) #Call to query processor
        qp.main(querySplit[0])
    elif sw==3:
        break