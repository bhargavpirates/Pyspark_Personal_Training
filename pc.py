import sys
import logging
import os

import pkg_resources
from logging.config import fileConfig
import dependency_injector.providers as providers
import dependency_injector.containers as containers


from hcpipeline.spark_session import SparkEnv
from hcpipeline.etlpipeline.util.extract.abstarctextractor import AbstractExtractor 
from hcpipeline.etlpipeline.util.abstractoperator import AbstractOperator
from hcpipeline.etlpipeline.util.writer.abstractwriter import AbstractWriter
from hcpipeline.etlpipeline.util.rules.abstractmetadataprep import AbstractMetadataPrep
from hcpipeline.etlpipeline.util.rules.rulesset import RuleSet

class PipelineContainer(containers.DeclarativeContainer):

    CONFIG_PATH = pkg_resources.resource_filename('hcpipeline', 'conf/')

    config = providers.Configuration() 
    config.from_ini(CONFIG_PATH + 'pipeline_confg.ini')
    
    @classmethod
    def getSparkEnv(cls,config):
        sparkenv = providers.Singleton(
            SparkEnv,
            spark_config = config.SPARK_CONFIG().items(),
            spark_udf = config.SPARK_UDF().items(),
            app_name = config.SPARK.app_name()
        )
        return sparkenv
    
    log_config = providers.Callable(
        fileConfig,
        CONFIG_PATH + 'logging_config.ini')

    log_config()

    logger = providers.Singleton(
        logging.Logger,             
        'hcapLogger')

    extractor_factory = providers.AbstractFactory(AbstractExtractor)
    operator_factory = providers.AbstractFactory(AbstractOperator)
    writer_factory = providers.AbstractFactory(AbstractWriter)
    metadata_factory = providers.AbstractFactory(AbstractMetadataPrep)