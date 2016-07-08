from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("mintemp")
sc = SparkContext(conf = conf)

def parseLine(line):
	fields = line.split(',')
	stationID = fields[0]
	entryType = fields[2]
	temp = float(fields[3])
	return (stationID, entryType, temp)

lines = sc.textFile("file:///Users/Sandy/Desktop/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))

results = minTemps.collect()

for result in results:
	print result