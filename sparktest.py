from pyspark import SparkContext
import random
import string
import time
output = open("/home/ubuntu/output.txt","w")

num_words = 100
word_length = 4
letters = string.ascii_lowercase
start_time = time.time()
sc = SparkContext("spark://slurm-head.cloud.compas.cs.stonybrook.edu:7077","first app")
#sc = SparkContext("local","first app")
sc.setLogLevel("WARN")
text_file = sc.textFile("/home/ubuntu/sample.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
output.write(str(counts.collect()))
output.close()
print("--- %s seconds ---" % (time.time() - start_time))
