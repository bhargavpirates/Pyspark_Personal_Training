import sys
''' 
Spark libraries
'''
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import udf

'''
Register classes for performance
Catalog details
Environment specific conf
'''

class SparkEnv:

    def __init__(self,spark_config=[],spark_udf=[],app_name='defaultapp'):
        self.__spark_config=spark_config
        self.__app_name = app_name
        self.__spark_udf = spark_udf
        self.spark = self.__spark_session()
        self.registered_functions = self.__register_functions()

    def __spark_conf(self):
        conf = SparkConf() \
            .setAppName(self.__app_name) \
            .setAll(self.__spark_config)

        print("configuration")
        print(conf)
        print(conf.toDebugString())
        return conf

    def __spark_session(self):
        spark = SparkSession.builder \
            .appName("DBG THTY/HG Application " + self.__app_name) \
            .config(conf=self.__spark_conf()) \
            .enableHiveSupport() \
            .getOrCreate()
        return  spark

    def __register_functions(self):
        ret = [self.spark.udf.register(name,eval(fun)[0],eval(fun)[1]) for name,fun in self.__spark_udf]
        return ret