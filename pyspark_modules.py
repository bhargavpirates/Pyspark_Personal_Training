import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col,struct,when
from pyspark.sql import Row