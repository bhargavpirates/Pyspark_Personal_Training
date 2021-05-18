echo "ETLO STEP TEST"

spark-submit 
--master yarn 
--deploy-mode cluster 
--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./pyenv/bin/python 
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./pyenv/bin/python 
--conf spark.yarn.appMasterEnv.beaconEnv='dev' 
--conf spark.yarn.appMasterEnv.awsFltrnsfr_appId='413945064248' 
--conf  spark.yarn.dist.archives="s3://dbg-beacon-dev-codez-gbd-phi-useast1/claim/artifact/hcpipeline-venv-claim-py-3.7.zip#pyenv" 
--jars s3://dbg-beacon-dev-codez-gbd-phi-useast1/claim/artifact/spark-avro_2.11-2.4.0.jar 
--files s3://dbg-beacon-dev-codez-gbd-phi-useast1/claim/conf/claim_conf.yaml  
s3://dbg-beacon-dev-codez-gbd-phi-useast1/claim/job/claim_ingestion.py



NOTE1: https://stackoverflow.com/questions/36461054/i-cant-seem-to-get-py-files-on-spark-to-work/39779271#39779271
Note2: https://github.com/massmutual/sample-pyspark-application/blob/master/setup-and-submit.sh


  cd a
  zip -r ../app_new.zip .
  la -ltr
  ls -ltr
  cd ..
  ls -ltr
  vi test1.py
  spark-submit --master yarn --deploy-mode cluster --py-files "app_new1.zip"  test1.py
  conda deactivate
  history

