#!/usr/bin/env python

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext as sqlCtx

conf = SparkConf().setAppName("Non-Redundant Entity Matching")
sc = SparkContext(conf=conf)

def att_key(l):
	"""
		[obj, attr1, attr2, attr3 ...] -> [(attr1, 0bj), (attr2, obj), (attr3, obj) ...]

	"""
	arr = []
	for attr in l[1:]:
		arr.append((attr, l[0]))
	return arr
##Read input
"""
	Assuming file(s) are tab-separated and first field is an object and rest are attributes.
	
	E.g. : 	obj1	attr1	attr2	attr3
		   	obj2	attr1	attr2	attr3
		   	...	
"""
inRDD = sc.textFile(argv[0])
##Convert to Attribut-Object pairs
aoPair = inRDD.flatMap(lambda line: att_key(line.split("\t")))
##Convert to Row objects
aoTable = aoPair.map(lambda (a,o): Row(attr=a, obj=o))


##Convert to DataFrame
"""
	Sample Table
	+----+---+
	|attr|obj|
	+----+---+
	|   1|  x|
	|   1|  y|
	|   2|  x|
	+----+---+
"""
aoDF = sqlCtx.createDataFrame(aoTable)

##Print schema
aoDF.printSchema()
#Window definition
window = Window.orderBy("attr").partitionBy("obj")
##
"""
	Transformed Table
	+----+---+----+
	|attr|obj|prev|
	+----+---+----+
	|   1|  x|null|
	|   2|  x|   1|
	|   1|  y|null|
	+----+---+----+
"""
memorize = aoDF.select("attr", "obj", lag("att", 1, None).over(window).alias("prev"))

"""+++++Incomplete++++++"""