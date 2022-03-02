from pyspark import SparkContext
sc = SparkContext("local","first app")

text_file = sc.textFile("textfile.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("output.txt")
