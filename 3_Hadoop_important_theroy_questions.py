Q: What is the difference between a HCatalog and Metastore in Hive?
A:	Metastore is where all the table metadata is stored.
	Hcatalog is the api that exposes the metastore to external tools, like Pig,madreduce,etc

Q: SparkSQL hiveSupport 
A:  Spark SQL also supports reading and writing data stored in Apache Hive. 
	However, since Hive has a large number of dependencies, these dependencies are not included in the default Spark distribution. 
	If Hive dependencies can be found on the classpath, Spark will load them automatically. 
	Note that these Hive dependencies must also be present on all of the worker nodes, as they will need access to the Hive serialization and deserialization libraries (SerDes) 
	in order to access data stored in Hive.

    Configuration of Hive is done by placing your hive-site.xml, core-site.xml (for security configuration), and hdfs-site.xml (for HDFS configuration) file 
    in conf/.

    When working with Hive, one must instantiate SparkSession with Hive support, including connectivity to a persistent Hive metastore,
    	support for Hive serdes, and Hive user-defined functions. 
    Users who do not have an existing Hive deployment can still enable Hive support.
    When not configured by the hive-site.xml, the context automatically creates metastore_db in the current directory and 
     	creates a directory configured by spark.sql.warehouse.dir, which defaults to the directory spark-warehouse in the 
     	current directory that the Spark application is started. 
    Note that the hive.metastore.warehouse.dir property in hive-site.xml is 
    	deprecated since Spark 2.0.0. 
    Instead, use spark.sql.warehouse.dir to specify the default location of database in warehouse. 
     	You may need to grant write privilege to the user who starts the Spark application.


