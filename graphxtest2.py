from pyspark.sql import SQLContext
from pyspark import SparkContext
import time
sc = SparkContext("spark://ip-172-31-94-126.ec2.internal:7077","first app")
#sc = SparkContext("local","first app")
sqlContext = SQLContext(sc)

# Create a GraphFrame
from graphframes import *
from graphframes.examples import Graphs
sc.setCheckpointDir("/home/hadoop/checkpoints")
g = Graphs(sqlContext).friends()  # Get example graph
start_time = time.time()
result = g.bfs("name = 'Esther'", "age < 32")
print("--- %s seconds ---" % (time.time() - start_time))
