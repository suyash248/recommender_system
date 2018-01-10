from pyspark import SparkConf, SparkContext
from copy import deepcopy

__sc__ = None
def get_or_create_spark_context(app_name, master="local[*]", force=False):
    global __sc__
    if force or __sc__ is None:
        print "[SPARK_CONTEXT] Creating spark context..."
        conf = SparkConf().setMaster(master).setAppName(app_name)
        __sc__ = SparkContext(conf=conf)
        __sc__.setLogLevel("ERROR")
    return __sc__