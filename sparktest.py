from pyspark import SparkContext
import random
import string
import time
output = open("/home/hadoop/output.txt","w")

num_words = 100
word_length = 4
letters = string.ascii_lowercase
start_time = time.time()
sc = SparkContext("spark://ip-172-31-94-126.ec2.internal:7077","first app")
#sc = SparkContext("local","first app")
sc.setLogLevel("WARN")
text_file = sc.textFile("/home/hadoop/sample.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
output.write(str(counts.collect()))
output.close()
print("--- %s seconds ---" % (time.time() - start_time))
