import sys
import os



from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""


def main():
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
    # lines = ssc.textFileStream('log')  #'log/ mean directory name
    lines = ssc.textFileStream('log')
    # line  = lines.flatMap(lambda line: line.split(" "))
    # a = line.flatMap(lambda line:len(line),line)
    # a.pprint()
    # counts = line.count(line),line
    # counts.pprint()
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (len(x),x)) \
        .reduceByKey(lambda a, b: a + "," + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
