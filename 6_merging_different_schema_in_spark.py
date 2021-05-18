Merging different schemas in Apache Spark
https://medium.com/data-arena/merging-different-schemas-in-apache-spark-2a9caca2c5ce


def convert_columns_to_string(schema, parent = "", lvl = 0):
    """
    Input:
    - schema: Dataframe schema as StructType
    
    Output: List
    Returns a list of columns in the schema casting them to String to use in a selectExpr Spark function.
    """
    
    lst=[]
    
    for x in schema:
        if lvl > 0 and len(parent.split(".")) > 0:
            parent = ".".join(parent.split(".")[0:lvl])
        else:
            parent = ""
       
        if isinstance(x.dataType, StructType):
            parent = parent + "." + x.name
            nested_casts = ",".join(convert_columns_to_string(x.dataType, parent.strip("."), lvl = lvl + 1))
            lst.append("struct({nested_casts}) as {col}".format(nested_casts=nested_casts, col=x.name))
        else:
            if parent == "":
                lst.append("cast({col} as string) as {col}".format(col=x.name))
            else:
                lst.append("cast({parent}.{col} as string) as {col}".format(col=x.name, parent=parent))
                
    return lst
  
def merge_schemas(dir_path):
    """
    Input: 
    - dir_path: Entity path containing partitions.
   
    Output: JSON RDD
    Returns a JSON RDD containing the union of all partitions with columns converted to String.
    """
    
    idx = 0
    
    print("\nProcessing files:")
    
    # Read each directory and create a JSON RDD making a union of all directories
    for dir in [d for d in os.listdir(dir_path) if d.find("=") != -1]:
        print("idx: " + str(idx) + " | path: " + dir_path + "/" + dir)

        # Get schema
        schema = spark.read.parquet(dir_path + "/" + dir).limit(0).schema

        # Read parquet file converting columns to string
        df_temp = (spark.read
                       .parquet(dir_path + "/" + dir)
                       .selectExpr(convert_columns_to_string(schema))
                       .withColumn(dir.split("=")[0], lit(dir.split("=")[1]))
                  )

        # Convert to JSON to avoid error when union different schemas
        if idx == 0:
            rdd_json = df_temp.toJSON()
        else:
            rdd_json = rdd_json.union(df_temp.toJSON())

        idx = idx + 1

    return rdd_json

# Read partitions and merge schemas
data_path = "/home/jovyan/work/data/raw/test_data_parquet"
df = spark.read.json(merge_schemas(data_path))





=============================================================================================================================


from pyspark.sql.types import StructType, ArrayType  
from pyspark.sql.functions import col, when

def flatten(schema, prefix=None):
  """
  Receives a schema and returns a list of columns flattened to use in select
  """
  
  fields = []
  for field in schema.fields:
      name = prefix + '.' + field.name if prefix else field.name
      dtype = field.dataType
      if isinstance(dtype, ArrayType):
          dtype = dtype.elementType

      if isinstance(dtype, StructType):
          fields += flatten(dtype, prefix=name)
      else:
          fields.append(name + " AS " + name.replace(".","_"))

  return fields
  
# Flatten dataframe
df_flat = df.selectExpr(flatten(df.schema))

# Count by partition and column
df_flat.select(
    ["date"] + 
    [when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df_flat.columns if c != "date"]
).groupBy("date").sum().sort("date").toPandas()