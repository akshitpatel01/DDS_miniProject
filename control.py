import mysql.connector
from QueryProcessor import QuerryProcessor

while 1:
    print('1. Sharding')
    print('2. Other queries')
    print('3. Exit')
    sw = int(input('Enter choice: '))
    if sw==1:
        username = input('Enter username: ')
        password = input('Enter password: ')
        query = input('Enter query: ')
        sharding_algo_number = int(input('Enter sharding algorithm number: '))
        #Call to sharding module
    elif sw==2:
        query = input('Enter query: ')
        qp = QuerryProcessor() #Call to query processor
        qp.main(query)
    elif sw==3:
        break