#!/usr/bin/env python

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row, Window
from pyspark.sql.functions import *

if len(sys.argv) != 2:
	sys.exit(1)

conf = SparkConf().setAppName("Non Redundant Entity Matching")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

def attr_key(l):
	"""
		[obj, attr1, attr2, attr3 ...] -> [(attr1, obj), (attr2, obj), (attr3, obj) ...]
	"""
	a = []
	for attr in l[1:]:
		a.append((attr, l[0]))
	return a

"""
	Assuming input file(s) to be tsv, and first field to be object and rest of the fields as attributes 
"""
#Read input
inRDD = sc.textFile(sys.argv[1])

##Generating attribute-object pair from each line
pairRDD = inRDD.flatMap(lambda line: attr_key(line.split("\t")))

##Converting to Row objects
aoPair = pairRDD.map(lambda (a, o): Row(attr=a, obj=o))

##Converting to Dataframe

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

aoDF = sqlCtx.createDataFrame(aoPair)

#Window that moves over rows of same obj and sorted by attr

window = Window.orderBy("attr").partitionBy("obj")

## Prev column contains previous attr of the same object
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
memorize = aoDF.select("attr", "obj", lag("attr",1, None).over(window).alias("prev"))



"""++++++++++Incomplete+++++++++++"""
