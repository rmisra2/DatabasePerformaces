import psycopg2
import subprocess, time
from subprocess import PIPE,Popen

INSERT_TABLE = "CREATE TABLE %s (Id integer, ProductId varchar, UserId varchar, ProfileName varchar, HelpfulnessNumerator integer, HelpfulnessDenominator integer, Score integer, Time integer, Summary varchar, Text varchar);"

INSERT_STOCKS = "CREATE TABLE %s (PERMNO real,SYMBOL varchar,DATE real,itime time,mquote real);"

SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

schema = "Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text".split(',')

my_file = subprocess.Popen(["hdfs","dfs","-cat","hdfs:///shared/amazon_food_reviews.csv"],stdout=subprocess.PIPE).stdout
stocks = "/mnt/volume/financial_data/stocks/permno_csv/"

conn_str = "host='localhost' dbname='group3' user='group3' password='67hU&GVcmV843SP$VJx!'"

NUM_QUERIES = 4
QUERY_LIST = ["Basic select","Basic counts", "Small takes","Medium select","Writes basic","Grab Columns","Advanced selects"]

ID = "psqlquery"

def stdFinish(donewith,nextone):
	print("Just finished "+str(NUM_QUERIES)+" of type " + QUERY_LIST[donewith] + ", now working on: " + QUERY_LIST[nextone] + " ... " + ID)

def load_table(c, table_name, path, statement):
	cursor = c.cursor()
	cursor.execute(statement % table_name)
	cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=path)
	c.commit()
	cursor.close()

def basic_select(conn):
	for i in range(NUM_QUERIES):
		time_query(conn,"SELECT Text FROM amazon WHERE Id=22010;")
def amazon_count(conn):
	for i in range(NUM_QUERIES):
		time_query(conn,"SELECT COUNT (Score) FROM amazon WHERE ProductId='B000E5C1YE' AND Score =5;")
		time_query(conn,"SELECT COUNT(DISTINCT UserId) FROM amazon;")

def time_insert(conn, name, f, statement):
	t = time.process_time()
	load_table(conn, name, f, statement)
	elapsed_time = time.process_time() - t
	print("zero one two three four five six seven eight nine ten "+str(elapsed_time)) 
def time_query(conn, query):
	c = conn.cursor()
	t = time.time()
	c.execute(query)
	c.fetchall()
	elapsed = time.time() - t
	print("zero one two three four five six seven eight nine ten "+ str(elapsed)) #format this

def time_take(conn,query,num):
	c = conn.cursor()
	t = time.time()
	c.execute(query)
	c.fetchmany(num)
	elapsed = time.time() - t
	print("zero one two three four five six seven eight nine ten " + str(elapsed))

def financial_data(conn):
	arr = eval(open("stock_array","r").readline())
	cur = conn.cursor()
	for elem in arr:
		stdFinish(5,4)
		cat = subprocess.Popen(["hdfs","dfs","-cat","hdfs:///shared/financial_data/stocks/permno_csv/"+elem],stdout=subprocess.PIPE)
		time_insert(conn,"stocks",cat.stdout, INSERT_STOCKS)
		for i in range(NUM_QUERIES):	
			time_take(conn,"SELECT * FROM stocks LIMIT 10", 10)
		for i in range(NUM_QUERIES):
			time_take(conn,"SELECT * FROM stocks LIMIT 100", 100)
		for i in range(NUM_QUERIES):
			time_take(conn,"SELECT * FROM stocks LIMIT 1000",1000)
		stdFinish(0,3)
		for i in range(NUM_QUERIES):
			time_query(conn,"SELECT itime FROM stocks WHERE mquote>15")
		stdFinish(3,1)
		for i in range(NUM_QUERIES):
			time_query(conn,"SELECT COUNT(DATE) FROM stocks")
		for i in range(NUM_QUERIES):
			time_query(conn,"SELECT COUNT(DATE)  FROM stocks WHERE mquote>20")
		stdFinish(1,5)
		for i in range(NUM_QUERIES):
			time_query(conn,"SELECT PERMNO FROM stocks")
		for i in range(NUM_QUERIES):
			time_query(conn,"SELECT DATE FROM stocks")
		'''print(QUERY_LIST[6] + " " + ID)
		for i in range(NUM_QUERIES):
			cur.execute("SELECT COUNT(DATE) FROM stocks WHERE mquote>30 AND PERMNO>10000")
			cur.fetchall()
		'''
		cur.execute("DROP TABLE stocks")
def add_stocks(conn):
	arr = eval(open("stock_array","r").readline())		
	cur = conn.cursor()
	for elem in arr:
		cat = subprocess.Popen(["hdfs","dfs","-cat","hdfs:///shared/financial_data/stocks/permno_csv/"+elem],stdout=subprocess.PIPE)
		time_insert(conn,"stocks",cat.stdout,INSERT_STOCKS)
		cur.execute("DROP TABLE stocks")

try:
	connection = psycopg2.connect(conn_str)
	#basic_select(connection)
	curs = connection.cursor()
	#basic_selectspecific(curs)
	financial_data(connection)
	#add_stocks(connection)
	#for i in range(10):
	#	time_insert(connection,"amazon",my_file,INSERT_TABLE)
	#	curs.execute("DROP TABLE amazon")
	#curs.execute("DROP TABLE amazon")
	#amazon_count(connection)
	
finally:
	connection.close()


