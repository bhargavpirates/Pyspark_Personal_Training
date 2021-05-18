Pyspark error "Could not serialize object" --> https://csyhuang.github.io/2019/09/24/pyspark-could-not-serialize-object/
How to Solve Non-Serializable Errors When Instantiating Objects In Spark UDFs --> https://www.placeiq.com/2017/11/how-to-solve-non-serializable-errors-when-instantiating-objects-in-spark-udfs/
Task not serializable: java.io.NotSerializableException when calling function outside closure only on classes not objects  --> https://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou




RDDs extend the Serialisable interface, so this is not what causing your task to fail. 
Now this doesnt mean that you can serialise an RDD with Spark and avoid NotSerializableException

Spark is a distributed computing engine and its main abstraction is a resilient distributed dataset (RDD), 
which can be viewed as a distributed collection. 
Basically, RDDs elements are partitioned across the nodes of the cluster, but Spark abstracts this away from the user, 
letting the user interact with the RDD (collection) as if it were a local one.

Not to get into too many details, but when you run different transformations on a RDD (map, flatMap, filter and others), 
your transformation code (closure) is:

serialized on the driver node,
shipped to the appropriate nodes in the cluster,
deserialized,
and finally executed on the nodes
You can of course run this locally (as in you


