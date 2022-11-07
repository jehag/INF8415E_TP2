import sys
 
from pyspark import SparkContext
 
if __name__ == "__main__":
    if len(sys.argv) == 3:
        # create Spark context with necessary configuration
        sc = SparkContext("local","PySpark Word Count Exmaple")

        # read data from text file and split each line into words
        words = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))

        # count the occurrence of each word
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

        # save the counts to output
        wordCounts.saveAsTextFile(sys.argv[2])
    else:
        print("Usage: {} <input> <output>".format(sys.argv[0]), file=sys.stderr)