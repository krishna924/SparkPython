from pyspark import SparkContext, SparkConf
from hdfs import InsecureClient
import os
class Intitalization:

    def sparkContextIntialize(self):
        os.environ["SPARK_HOME"] = "C:\\spark-1.6.3-bin-hadoop2.6"
        # print(.environ['PATH'])
        sc = SparkContext(master="local", appName="Spark Demo")
        return sc