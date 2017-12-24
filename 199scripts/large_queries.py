from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("Quizzical Queries")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
schema = 'PERMNO,SYMBOL,DATE,itime,mquote'.split(',')

import csv,os,subprocess,time
from os import walk
from os import listdir
from os.path import isfile, join

NUM_QUERIES = 4
QUERY_LIST = ["Basic select","Basic counts", "Small takes","Medium select","Writes basic","Grab Columns","Advanced selects"]
ID = "sparkquery"

def parse_csv(x):
	x = x.replace('\n', '')
	d = csv.reader([x])
	return next(d)
def stdFinish(donewith,nextone):
	return "Just finished "+str(NUM_QUERIES)+" of type " + QUERY_LIST[donewith] + ", now working on: " + QUERY_LIST[nextone] + " ... " + ID 

def giveDirList(d):
	return [f for f in listdir(d) if isfile(join(d,f)) and os.path.getsize(join(d,f))>>20 >= 3]
#currentFiles = giveDirList("/mnt/volume/financial") # USED MERGED DATA IN MNT
currentFiles = eval(open("stock_array","r").readline())
#print len(currentFiles)

for path in currentFiles: 
	stocks = sc.textFile("hdfs:///shared/financial_data/stocks/permno_csv/"+path)
	first = stocks.first()
	csv_payloads = stocks.filter(lambda x: x != first).map(parse_csv)
	t = time.time()
	df = sqlContext.createDataFrame(csv_payloads,schema)
	sqlContext.registerDataFrameAsTable(df, "stocks")
	elapsed = time.time() - t
	print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsed))
	print(stdFinish(4,2))
	for i in range(NUM_QUERIES):
		tt = time.time()
		stocks.take(10)
		elapsedt = time.time() - tt
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedt))
	for i in range(NUM_QUERIES):
		tt = time.time()
		stocks.take(100)
		elapsedt = time.time() - tt
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedt))
	for i in range(NUM_QUERIES):
		tt = time.time()
		stocks.take(1000)
		elapsedt = time.time() - tt
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedt))
	print(stdFinish(2,3))
	for i in range(NUM_QUERIES):
		tm = time.time()
		sqlContext.sql("SELECT itime FROM stocks WHERE mquote>15").collect()
		elapsedtt = time.time() - tm
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedtt))
	print(stdFinish(3,1))
	for i in range(NUM_QUERIES):
		tc = time.time()
		sqlContext.sql("SELECT COUNT(DATE) FROM stocks").collect()
		elapsedtc = time.time() - tc
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str (elapsedtc))
	for i in range(NUM_QUERIES):
		tc = time.time()
		sqlContext.sql("SELECT COUNT(DATE)  FROM stocks WHERE mquote>20").collect()
		elapsedtc = time.time() - tc
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedtc))
	print(stdFinish(1,5))
	for i in range(NUM_QUERIES):
		tco = time.time()
		sqlContext.sql("SELECT PERMNO FROM stocks").collect()
		elapsedtco = time.time() - tco
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedtco))
	for i in range(NUM_QUERIES):
		tco = time.time()
		sqlContext.sql("SELECT DATE FROM stocks").collect()
		elapsedtco = time.time() - tco
		print("newlog 1 2 3 4 5 6 7 8 9 10 " + str(elapsedtco))
	#print(stdFinish(5,6))
	#for i in range(NUM_QUERIES):
	#	sqlContext.sql("SELECT COUNT(DATE) FROM stocks WHERE mquote>30 AND PERMNO>10000")


# TODO : more stuff here! 
