spark-submit
-- master yarn
--deploy-mode cluster
--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./pyenv/bin/PYSPARK_DRIVER_PYTHON
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./pyenv/bin/PYSPARK_PYTHON
--conf spark.yarn.dist.archives="s3Path/venv.zip#pyenv"
--jars s3path/jarname.jar
--files s3path/conf.yaml
--py-files



https://medium.com/airbnb-engineering/on-spark-hive-and-small-files-an-in-depth-look-at-spark-partitioning-strategies-a9a364f908

pages/viewpage.action?spaceKey=ADJ&title=Team+PTO+Calendar


https://medium.com/data-arena/merging-different-schemas-in-apache-spark-2a9caca2c5ce