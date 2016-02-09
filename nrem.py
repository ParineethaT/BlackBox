#!/usr/bin/env python

from py4j.java_gateway import Py4JJavaError

import sys, os
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 3:
	
	print(""" 
		Error: This program takes 2 arguments
		Usage: bin/spark-submit --master <spark-master> nrem.py <input dir> <output dir>
	""")
	sys.exit(1)

#Parallelism
partitions = 1
#Output Directories
matched_output = os.path.join(sys.argv[2],"matched")
eliminated_output = os.path.join(sys.argv[2], "eliminated")

conf = SparkConf().setAppName("Non Redundant Entity Matching")
sc = SparkContext(conf=conf)
sqlCtx = HiveContext(sc)

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
inRDD = sc.textFile(sys.argv[1], partitions)
#If RDD is empty. Raise error and exit.
if inRDD.isEmpty():
	raise Py4JJavaError("Input Directory/File is empty.")
	sys.exit(2)

##Generating attribute-object pair from each line
aoPair = inRDD.flatMap(lambda line: attr_key(line.split("\t")))


##Converting to Dataframe

"""
	Sample Table
	+----+---+
	|attr|obj|
	+----+---+
	|   1|  a|
	|   2|  a|
	|   3|  a|
	|   1|  b|
	|   2|  b|
	|   3|  b|
	|   1|  c|
	|   3|  c|
	+----+---+
"""

schema = StructType([StructField("attr", StringType(), True), StructField("obj", StringType(), True)])

aoDF = sqlCtx.createDataFrame(aoPair, schema)

#Window that moves over rows of same obj and sorted by attr

window = Window.orderBy("attr").partitionBy("obj")

## Prev column contains previous attr of the same object
"""
	Transformed Table	
	+----+---+----+
	|attr|obj|prev|
	+----+---+----+
	|   1|  a|null|
	|   2|  a|   1|
	|   3|  a|   2|
	|   1|  b|null|
	|   2|  b|   1|
	|   3|  b|   2|
	|   1|  c|null|
	|   3|  c|   1|
	+----+---+----+

"""
memorize = aoDF.select("attr", "obj", lag("attr",1, None).over(window).alias("prev"))


##Back to RDD
#
"""
	DataFrame : [attr, obj, prev] -> RDD : [(attr, (obj, prev))]

	[(1, (u'a', None)),
	 (2, (u'a', 1)),
	 (3, (u'a', 2)),
	 (1, (u'b', None)),
	 (2, (u'b', 1)),
	 (3, (u'b', 2)),
	 (1, (u'c', None)),
	 (3, (u'c', 1))]

"""
mappedRDD = memorize.map(lambda row: (row.attr.encode('utf-8'), (row.obj, row.prev)))


#Group by 'attr' and collect tuple(obj, prev) into lists
"""
	[(1, [(u'a', None), (u'b', None), (u'c', None)]),
	 (2, [(u'a', 1), (u'b', 1)]),
	 (3, [(u'a', 2), (u'b', 2), (u'c', 1)])]

"""
groupedByAttr = mappedRDD.groupByKey(partitions).mapValues(list)

groupedByAttr.cache()
print("Total Attributes: "+ str(groupedByAttr.count()))
##Function to collect matched and eliminated pairs

def pairFilter(elim=False):
	##Return correct function based on 'elim'

	def matchedPair(l):
		s = set()
		#Two loops to iterate through list of obj, prev
		#Outer loop
		for obj_o, prev_o in l:
			#Inner loop
			for obj_i, prev_i in l:
				#Case for matching
				if obj_o != obj_i and ((prev_o != prev_i) or (not prev_i and not prev_o)):
					#No duplicates
					s.add("-".join(sorted([obj_o, obj_i])).encode('utf-8'))
		return s

	def eliminatedPair(l):
		s = set()
		for obj_o, prev_o in l:
			for obj_i, prev_i in l:
				if obj_o != obj_i and ((prev_o == prev_i) and (prev_o and prev_i)):
					s.add("-".join(sorted([obj_o, obj_i])).encode('utf-8'))
		return s

	"""
		>>> matched = pairFilter(elim=False)
		>>> eliminated = pairFilter(elim=True)
	"""	
	if elim:
		return eliminatedPair
	else:
		return matchedPair
## matched is an instace of matchedPair()
matched = pairFilter(elim=False)
##eliminated is an instance of eliminatedPair()
eliminated = pairFilter(elim=True)
##RDD with all the matched pairs
"""
	***** Problem in the algorithm : if objects skip attributes then you get duplicate pairs across keys/attributes ****
		*** Since 'a' and 'c', and 'b' an 'c' did not have common previous attributes. ***

	[(1, {u'a-b', u'a-c', u'b-c'}), (3, {u'a-c', u'b-c'})]
"""
##Filter to eliminate empty sets

matchedPairs = groupedByAttr.mapValues(lambda x: matched(x)).filter(lambda (x, y): True if len(y) else False)


#RDD with all the eliminated pairs
"""
	****  'a' and 'b' both had common previous attributes ****
	[(2, {u'a-b'}), (3, {u'a-b'})]
"""
##Filter to eliminate empty sets
eliminatedPairs = groupedByAttr.mapValues(lambda x: eliminated(x)).filter(lambda (x, y): True if len(y) else False)

##Saving outputs

matchedPairs.saveAsTextFile(matched_output)

eliminatedPairs.saveAsTextFile(eliminated_output)

sc.stop()

## End of Program