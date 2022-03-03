from pyspark.sql import SQLContext
from pyspark import SparkContext
import time
sc = SparkContext("spark://slurm-head.cloud.compas.cs.stonybrook.edu:7077","first app")
#sc = SparkContext("local","first app")
sqlContext = SQLContext(sc)

# Create a GraphFrame
from graphframes import *
from graphframes.examples import Graphs
sc.setCheckpointDir("/home/ubuntu/checkpoints")
g = Graphs(sqlContext).friends()  # Get example graph
start_time = time.time()
result = g.bfs("name = 'Esther'", "age < 32")
print("--- %s seconds ---" % (time.time() - start_time))
