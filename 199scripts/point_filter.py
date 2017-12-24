import os, math

ID = "psqlquery"

def average(array):
	return sum(array)/len(array)

def parse_file(arr):
	query_map = {}
	query_count = {}
	curr_arr = []
	cur_query = "Writes basic"

	for line in arr:
		if ID not in line:
			curr_arr.append(float(line.split()[11]))
		else:
			if "Joins" in line:
					cur_query = str(line.split()[5]) + " " + str(line.split()[6]) + " " + str(line.split()[7])[:-1]
			else:
					cur_query = str(line.split()[5]) + " " + str(line.split()[6])[:-1]

			if cur_query in query_map.keys():
				#print cur_query + str(query_count[cur_query])
				query_map[cur_query] += sum(curr_arr)
				query_count[cur_query] += len(curr_arr)#ccount
			else:
				query_map[cur_query] = sum(curr_arr)
				query_count[cur_query] = len(curr_arr)#ccount
			curr_arr = []
	return query_map, query_count

def average_maps(query_map,query_count):
	d = dict()
	for k in query_map.keys():
		d[k] = query_map[k] / (query_count[k])

	return d


def file_to_array(filepath):
	arr = []
	#sum = 0
	for line in open(filepath,"r"):
		if '\n' == line:
			continue
		else:
			arr.append(line)
			#if "sparkquery" not in line:
				#sum += float(line.split()[11])
	#print sum
	return arr

def parse_file_variance(arr, avg_map, countmap):
	dev_map = {}
	cur_query = "Writes basic"
	for line in arr:
		if ID in line:
			if "Joins" in line.split()[5]:
				cur_query = str(line.split()[5]) + " " + str(line.split()[6]) + " " + str(line.split()[7])[:-1]
			else:
				cur_query = str(line.split()[5]) + " " + str(line.split()[6])[:-1]

		if ID not in line:
			if cur_query in dev_map.keys():
				dev_map[cur_query] += (float(line.split()[11]) - avg_map[cur_query]) ** 2
			else:
				dev_map[cur_query] = (float(line.split()[11]) - avg_map[cur_query]) ** 2
	for k in dev_map.keys():
		dev_map[k] = dev_map[k] / (countmap[k]) # 11 is number of logs, so average * 11 * time we did stuff
	return dev_map

def logfile_stddev(var_map):
	return sqrt_map(var_map)

def min_max_map(arr):
	min_map = {}
	max_map = {}
	cur_query = "Writes basic"
	for line in arr:
		if ID not in line:
			cval = float(line.split()[11])
			#print cur_query + " " + str(cval)
			if cur_query not in max_map.keys():
				max_map[cur_query] = cval
				min_map[cur_query] = cval
			else:
				if cval >= max_map[cur_query]:
					max_map[cur_query] = cval
				elif cval < min_map[cur_query]:
					min_map[cur_query] = cval
		else:
			cur_query = str(line.split()[10]) + " " + str(line.split()[11])[:-1]
	return min_map, max_map

def sqrt_map(d):
	dic = {}
	for k,v in d.iteritems():
		dic[k] = math.sqrt(v)
	return dic

'''
super sketchy, probably shouldnt use
'''
def merge_dicts(qarr, carr):
	final_dict = {}
	for key in qarr[0].iterkeys():
		final_dict[key] = tuple(d[key] for d in qarr)
	for key in final_dict.keys():
		final_dict[key] = (sum(final_dict[key]) / ((carr[0][key])*(len(final_dict[key])))) # * len? probably not good
	return final_dict

def build_log_array():
	files = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith(".log")]
	q_arr = []
	c_arr = []
	for f in files:
		qmap, cmap = parse_file(file_to_array(f))
		q_arr.append(qmap)
		c_arr.append(cmap)
	return q_arr, c_arr

def count_logs(f):
		s = open(f,"r")
		c = 0
		for line in s.readlines():
				if ID not in line:
						c+=1
		return c
## Financial
#file = file_to_array("output-largestock.txt.log")
#map1, cmap = parse_file(file)
#print map1
#print cmap
#averagemap = average_maps(map1, cmap)
#print file
#varmap = parse_file_variance(file,averagemap, cmap)
#stddevmap = logfile_stddev(varmap)
#print varmap
#print stddevmap

#print averagemap
#min, max = min_max_map(file)
#print min
#print max
'''
## Amazon
amazonq, amazonc = build_log_array()
amazon_avg = average_maps(amazonq[0],amazonc[0])
#print amazon_avg
bigarr = map(lambda x : file_to_array(x), [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith(".log")])
bigarr = [item for sublist in bigarr for item in sublist]
var_amazon = parse_file_variance(bigarr,amazon_avg,amazonc[0])
stddev_amazon = logfile_stddev(var_amazon)
min_amazon, max_amazon = min_max_map(bigarr)
#for item in bigarr:
#	print item
#print min_amazon
#print max_amazon
'''
## Amazon PSQL

biglog = file_to_array("biglogfile.log")
writefile = file_to_array("logfile.log")
financial_map, financial_count = parse_file(biglog)
write_f, write_c = parse_file(writefile)
print "Average: ",average_maps(write_f,write_c)
print "STDDEV: ", parse_file_variance(writefile,average_maps(write_f,write_c),write_c)
minmaps, maxmaps = min_max_map(writefile)
print "Mins: ", minmaps
print "Maxs: ", maxmaps

