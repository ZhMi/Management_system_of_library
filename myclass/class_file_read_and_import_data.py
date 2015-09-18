__author__ = 'zhmi'

##########################################   Part 1 Import Part    #####################################################

import sys
import os
import string
reload(sys)
sys.setdefaultencoding("utf-8")

import MySQLdb

##########################################   Part 2 Read file Part #####################################################

# read data from file "book.txt" and import all the data into list raw_list
book_file_dir = "../data/input/book.txt"
#fp = open("/home/zhmi/Documents/library_book_database/book.txt")
fp = open(book_file_dir)
raw_list = []

# filter '\n','\t' at the begin and end of a line
def filterFun(line):
    line = line.decode('utf-8')
    line = line.strip()
    return line

try:
    raw_list = fp.readlines()
except Exception,ex:
    print Exception,"fp.readlines() error.",ex
    line = fp.readline()
    while line:
        line = line.strip()
        raw_list.append(line)
        line = fp.readline()
finally:
    fp.close()

map(filterFun,raw_list)

def separateFun(line):
    line = line.split("\t")
    line = line[1:7]
    tempstring = line[1] #line[1] includes book name and information else
    symbol = "\xef\xbc\x9a"  # unicode of ":" in chinese symbol
    index = tempstring.find(symbol)
    if index != -1:
        line[1] = line[1][index+3:]
    if len(line) != 6:
        lacked_num = 6-len(line)
        for i in xrange(lacked_num):
            line.append(" ")
    return line

'''
    id : auto increase
    book_num :          pure_list[i][0]
    book_name :         pure_list[i][1]
    author :            pure_list[i][2]
    publish_house :     pure_list[i][3]
    category :          pure_list[i][4]
    summary :           pure_list[i][5]
    total_amount :
'''

pure_list = map(separateFun,raw_list)

##########################################   Part 3 Import data of books into databse ##################################

conn = MySQLdb.connect(host='localhost',user='root',passwd='95120',charset='utf8',port=3306)
cursor = conn.cursor()

try:
    cursor.execute("use libraryDB;")
    def InsertFun(value):
        cursor.execute('insert into book_fundamental_table(book_num,book_name,author,publish_house,category,summary) values(%s,%s,%s,%s,%s,%s);', value)
        conn.commit()
    map(InsertFun,pure_list)
except Exception,ex:
    print Exception,"Error!",ex
finally:
    cursor.close()
    conn.close()
