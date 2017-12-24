from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("Quizzical Queries")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
schema = "Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text".split(',')

import csv

NUM_QUERIES = 4
QUERY_LIST = ["Basic select","Basic counts", "BIG select *","Medium select","Writes basic","Yelp writes","Joins pt 1","Joins pt 2","Joins pt 3"]
ID = "sparkquery"

def parse_csv(x):
	x = x.replace('\n', '')
	d = csv.reader([x])
	return next(d)

def stdFinish(donewith,nextone):
	print("Just finished "+str(NUM_QUERIES)+" of type " + QUERY_LIST[donewith] + ", now working on: " + QUERY_LIST[nextone] + " ... " + ID)
 
if True:
	reviews = sc.textFile("hdfs:///shared/amazon_food_reviews.csv")
	first = reviews.first()
	csv_payloads = reviews.filter(lambda x: x != first).map(parse_csv)
	df = sqlContext.createDataFrame(csv_payloads,schema)
	sqlContext.registerDataFrameAsTable(df, "amazon")
'''
stdFinish(4,0)	

for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT Text FROM amazon WHERE Id=22010").collect()

stdFinish(0,1)

for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT COUNT (Score) FROM amazon WHERE ProductId='B000E5C1YE' AND Score =5").collect()

for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT COUNT(DISTINCT UserId) FROM amazon").collect()

stdFinish(1,3)
for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT UserId, COUNT(Score) FROM amazon GROUP BY UserId ORDER BY COUNT(UserId) DESC LIMIT 1").collect()

for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT ProductId, AVG(Score) AS a, COUNT(*) AS c FROM amazon GROUP BY ProductId HAVING c>10 ORDER BY a DESC, c DESC LIMIT 10").collect()

for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT Id, HelpfulnessNumerator/HelpfulnessDenominator FROM amazon WHERE HelpfulnessDenominator > 10 ORDER BY HelpfulnessNumerator / HelpfulnessDenominator DESC, HelpfulnessDenominator DESC LIMIT 10").collect()
stdFinish(3,5)

for i in range(NUM_QUERIES):
	reviews = sqlContext.jsonFile("hdfs:///shared/yelp/yelp_academic_dataset_review.json")
	businesses = sqlContext.jsonFile("hdfs:///shared/yelp/yelp_academic_dataset_business.json")
	checkins = sqlContext.jsonFile("hdfs:///shared/yelp/yelp_academic_dataset_checkin.json")
	users = sqlContext.jsonFile("hdfs:///shared/yelp/yelp_academic_dataset_user.json")


sqlContext.registerDataFrameAsTable(reviews, "reviews")
sqlContext.registerDataFrameAsTable(businesses, "businesses")
sqlContext.registerDataFrameAsTable(checkins, "checkins")
sqlContext.registerDataFrameAsTable(users, "users")

stdFinish(5,6)
for i in range(NUM_QUERIES):
	allstates = sqlContext.sql("SELECT businesses.state, checkins.business_id FROM checkins INNER JOIN businesses ON businesses.business_id=checkins.business_id ORDER BY businesses.state")
	sqlContext.registerDataFrameAsTable(allstates,"allstates")
	sqlContext.sql("SELECT state, COUNT(*) AS c FROM allstates GROUP BY state ORDER BY c DESC LIMIT 1").collect()
stdFinish(6,7)
for i in range(NUM_QUERIES):
	sqlContext.sql("SELECT COUNT(*) FROM users INNER JOIN reviews ON reviews.user_id=users.user_id WHERE reviews.funny=1 AND users.yelping_since>'2012-01-01' LIMIT 1").collect()
stdFinish(7,8)
for i in range(NUM_QUERIES):
	stars_reviews = sqlContext.sql("SELECT users.user_id, reviews.business_id AS review_count, reviews.stars AS stars FROM users INNER JOIN reviews ON users.user_id=reviews.user_id WHERE stars=1 AND review_count>250")
	sqlContext.registerDataFrameAsTable(stars_reviews,"stars_reviews")
	check_state = sqlContext.sql("SELECT stars_reviews.user_id FROM stars_reviews INNER JOIN businesses ON stars_reviews.review_count=businesses.business_id WHERE businesses.city='Champaign' AND businesses.state='IL'")
	sqlContext.registerDataFrameAsTable(check_state,"check_state")
	user_ids = sqlContext.sql("SELECT * FROM check_state").collect()
'''
