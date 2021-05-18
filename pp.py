import sys
import os
import inspect


from functools import partial, wraps, reduce

try:
    from itertools import zip_longest as zip_longest, chain
except ImportError:
    from itertools import izip_longest as zip_longest, chain

from pyspark.sql.functions import col, array, explode_outer, expr, regexp_replace
from pyspark.sql import DataFrame

import dependency_injector.providers as providers
import dependency_injector.containers as containers

from hcpipeline.pipeline_container import PipelineContainer
from hcpipeline.etlpipeline.util.extract.csvextractor import CsvExtractor
from hcpipeline.etlpipeline.util.extract.dataextractor import DataExtractor
from hcpipeline.etlpipeline.util.extract.hiveextractor import HiveExtractor
from hcpipeline.etlpipeline.util.extract.hudiextractor import HudiExtractor
from hcpipeline.etlpipeline.util.lkp_operator import LkpOperator
from hcpipeline.etlpipeline.util.generictransformer import GenericTransformer
from hcpipeline.etlpipeline.util.writer.genericwriter import GenericWriter
from hcpipeline.etlpipeline.util.rules.defaultmetadataprep import DefaultMetadataPrep
from hcpipeline.etlpipeline.util.rules.filemetadataprep import FileMetadataPrep
import hcpipeline.dataframe_transform 

class Pipeline:

    __pipelineinstance = None
    __pipelineconfig = PipelineContainer.config
    __pipelinelogger = PipelineContainer.logger
    spark = None
    logger = None 


    def __init__(self):
        
        if Pipeline.__pipelineinstance != None:
            raise Exception("Singleton class, call class method getOrCreateInstance")
        else:
            self.pipelineconfig = Pipeline.__pipelineconfig
            self.logger = Pipeline.__pipelinelogger()
            Pipeline.logger = self.logger
            sparkenv = PipelineContainer.getSparkEnv(Pipeline.__pipelineconfig)
            self.spark = sparkenv().spark
            Pipeline.spark = self.spark
            Pipeline.__pipelineinstance = self

    @classmethod
    def getOrCreateInstance(cls):
        if cls.__pipelineinstance is None:
            Pipeline()
            #cls.__pipelineinstance = cls.__new__(cls)
        return cls.__pipelineinstance

    @classmethod
    def __applyFunction(cls,f,srccol,trgcol):
        if f == '':
            if isinstance(srccol,tuple):
                inpcol = srccol[0][0]
            else:
                inpcol = srccol
            return expr(inpcol).alias(trgcol)
        else:
            fargs = [expr(inp) for inp in srccol[0]] +  [inp for inp in srccol[1]]
            if trgcol == '':
                return f(*fargs) 
            else:
                return f(*fargs).alias(trgcol)
    @classmethod
    def __elementToTuple(cls,argrec):
        tmplist = []
        if not isinstance(argrec,tuple):
            tmplist.append(argrec)
            return tuple(tmplist)
        else:
            return argrec
    @classmethod
    def __filterList(cls,collist=[],filterlist=[]):
        newcollist = []
        for colst in filterlist:
            if isinstance(colst,tuple):
                if (isinstance(colst[0],int) and isinstance(colst[1],int)):
                    newcollist.extend(collist[colst[0]-1:colst[1]])
                if (isinstance(colst[0],str) and isinstance(colst[1],str)):
                    col1,col2 = (collist.index(colst[0]) + 1,collist.index(colst[1] + 1))
                    newcollist.extend(collist[col1-1:col2])
            if isinstance(colst,int):
                newcollist.append(collist[colst - 1])
            if isinstance(colst,str):
                newcollist.append(colst)
        return  newcollist
    
    '''
    Method to update the pipeline config 
    '''
    @classmethod
    def withPipelineConfigIni(cls,configfile):
        cls.__pipelineconfig.from_ini(configfile)
        return cls
    
    @classmethod
    def withPipelineConfigYaml(cls,configfile):
        cls.__pipelineconfig.from_yaml(configfile)
        return cls

    @classmethod
    def withPipelineConfigDict(cls,configdict):
        cls.__pipelineconfig.from_dict(configdict)
        return cls

    @classmethod
    def withPipelineConfigEnv(cls,key1,key2,envvar):
        cls.__pipelineconfig()[key1][key2] = os.environ.get(envvar)
        return cls 

    '''
    Method to update the logger config 
    Current version of the logic requires etl job to get the logger fom pipeline
    '''
    def withLoggerConfig(self,configfile):
        pass

    '''
    Decorator to extract CSV files
    '''
    @classmethod
    def extractCsv(cls,**csvprop):
        def customExtr(extr_func):
            @wraps(extr_func)
            def wrapper(*fargs,**dynprop):
                csv_prop = PipelineContainer.config.EXTRACTOR_CSV()
                csv_prop.update(csvprop)
                csv_prop.update(dynprop)
                
                PipelineContainer.extractor_factory.override(
                    providers.Factory(CsvExtractor,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    csv_config=csv_prop,
                    schema=csv_prop["schema"]),
                )
                csv_extractor = PipelineContainer.extractor_factory()
                csv_df = csv_extractor.extractor(extr_func,*fargs)
                return csv_df
            return wrapper
        return customExtr
    
    '''
    Decorator to extract data from file sources
    '''
    @classmethod
    def dataExtractor(cls,**prop):
        def customExtr(extr_func):
            @wraps(extr_func)
            def wrapper(*fargs,**dynprop):
                config_prop = PipelineContainer.config()
                config_key = 'EXTRACTOR_' + prop['format'].upper()
                extractor_config = config_prop[config_key]
                extractor_config.update(prop)
                extractor_config.update(dynprop)

                PipelineContainer.extractor_factory.override(
                    providers.Factory(DataExtractor,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    config=extractor_config),
                )

                data_extractor = PipelineContainer.extractor_factory()
                extractor_df = data_extractor.extractor(extr_func,*fargs)
                print("Data Frame details")
                print(extractor_df)
                return extractor_df
            return wrapper
        return customExtr
    
    '''
    Decorator to extract data from hive sources
    '''
    @classmethod
    def hiveExtractor(cls,**prop):
        def customExtr(extr_func):
            @wraps(extr_func)
            def wrapper(*fargs,**dynprop):
                config_prop = PipelineContainer.config()
                config_key = 'EXTRACTOR_' + 'HIVE'
                extractor_config = config_prop[config_key]
                extractor_config.update(prop)
                extractor_config.update(dynprop)

                PipelineContainer.extractor_factory.override(
                    providers.Factory(HiveExtractor,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    config=extractor_config),
                )

                data_extractor = PipelineContainer.extractor_factory()
                extractor_df = data_extractor.extractor(extr_func,*fargs)
                print("Data Frame details")
                print(extractor_df)
                return extractor_df
            return wrapper
        return customExtr
    
    '''
    Decorator to extract data from hive sources
    '''
    @classmethod
    def hudiExtractor(cls,**prop):
        def customExtr(extr_func):
            @wraps(extr_func)
            def wrapper(*fargs,**dynprop):
                config_prop = PipelineContainer.config()
                config_key = 'EXTRACTOR_' + 'HUDI'
                extractor_config = config_prop[config_key]
                extractor_config.update(prop)
                extractor_config.update(dynprop)

                PipelineContainer.extractor_factory.override(
                    providers.Factory(HudiExtractor,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    config=extractor_config),
                )

                data_extractor = PipelineContainer.extractor_factory()
                extractor_df = data_extractor.extractor(extr_func,*fargs)
                print("Data Frame details")
                print(extractor_df)
                return extractor_df
            return wrapper
        return customExtr

    '''
    operator decorators/functions 
    '''
    @classmethod
    def lookupDf(cls,**lkpprop):
        def customLkp(lkp_func):
            @wraps(lkp_func)
            def wrapper(*fargs,**dynprop):
                lkp_prop = PipelineContainer.config.LKP_OPERATOR()
                lkp_prop.update(lkpprop)
                lkp_prop.update(dynprop)

                PipelineContainer.operator_factory.override(
                    providers.Factory(LkpOperator,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    lkp_config=lkp_prop),
                )
                lkp_operator = PipelineContainer.operator_factory()
                lkp_df = lkp_operator.operator(lkp_func,*fargs)
                return lkp_df
            return wrapper
        return customLkp

    @classmethod
    def columnsToRowsDf(cls,df,columnsdict):
        columnset = set()
        for key,values in columnsdict.items():
            values = tuple(values)
            df = df.withColumn(key,explode_outer(array(*values)))
            columnset.update(values)
        df_columns = tuple(df.columns)
        dfcoltupl = filter(lambda x: x not in columnset,df_columns)
        return df.select(*dfcoltupl)
    
    @classmethod
    def routerDf(cls,df,conddict):
        filterdict = dict([(key,df.filter(values)) for key,values in conddict.items()])
        def wrapper(filterdict):
            def get(dfname):
                return filterdict[dfname]
            return get
        return wrapper(filterdict)
   
    '''
    validator/test functions
    '''
    @classmethod
    def primaryKeyDf(cls,df,collist):
        
        groupeddf = df.groupBy(*collist).count()
        
        duplicatekeys = groupeddf.filter(col('count') > 1).drop(col('count'))
        nonduplicatekeys = groupeddf.filter(col('count') == 1).drop(col('count'))
        nonduplicatesdf = df.dropDuplicates(collist)
        
        if len(duplicatekeys.take(1)) == 0:
            status = 'noduplicates'
        else:
            status = 'duplicatesexists'
        
        pkexprlist = ["((not isnull({pk1})))".format(pk1=rec) for rec in  collist]
        pkexpr = reduce(lambda expr1, expr2: 'and'.join([expr1,expr2]),pkexprlist)
        
        nullpkdf = df.withColumn('ispknull',expr(pkexpr))
        validpkdf  = nonduplicatesdf.withColumn('ispknull',expr(pkexpr))
        
        newcollist = collist
        newcollist.append("ispknull")
        nullkeys  =  nullpkdf.select(*newcollist).filter(col('ispknull') == False).drop(col('ispknull'))
        nonnullkeys = nullpkdf.select(*newcollist).filter(col('ispknull') == True).drop(col('ispknull'))
        nonnulldf = nullpkdf.filter(col('ispknull') == True).drop(col('ispknull'))
        validdf = validpkdf.filter(col('ispknull') == True).drop(col('ispknull')) 

        if len(nullkeys.take(1)) == 0:
            status = status + " & " + "nonullkeys" 
        else:
            status = status + " & " + "nullkeysexists"
        
        filterdict = dict([("duplicatekeys",duplicatekeys)
        ,("nonduplicatekeys",nonduplicatekeys)
        ,("nonduplicatesdf",nonduplicatesdf)
        ,("nullkeys",nullkeys)
        ,("nonnullkeys",nonnullkeys)
        ,("nonnulldf",nonnulldf)
        ,("validdf",validdf)
        ,("status",status)
        ])

        def wrapper(filterdict):
            def get(dfname):
                return filterdict[dfname]
            return get
        return wrapper(filterdict)

    @classmethod
    def testConditionDf(cls,df,cond):
        neg_cond = 'not (' + cond + ')'
        return cls.routerDf(df,{'true': cond,'false': neg_cond})

    @classmethod
    def projColumnExpressionDF(cls,df,exp1,outcollist=[]):
        dfcollist = df.columns
        outexplist = dfcollist + [expr(exp1 + ' as ' + cl) for cl in outcollist]
        return df.select(*outexplist)
    
    '''
    projection functions
    '''
    @classmethod
    def projectionDf(cls,df,pkcollist=[],collist=[],skipdup=False):
        completelist = df.columns
        print("#######################################")
        print("Inside Projection DF function")
        print("Before:")
        print(df)
        newcollist = cls.__filterList(completelist,collist)
        newpklist = cls.__filterList(completelist,pkcollist)
        if skipdup:
            trg_df = df.select(*newcollist).dropDuplicates(newpklist)
        else:
             trg_df = df.select(*newcollist)
        print("After")
        print(trg_df)
        print("#######################################")
        return trg_df

    @classmethod
    def multiProjectionDf(cls,df,pkcollist=[],collist=[],skipdup=[],namelist=[]):
        projdict = dict([(name,cls.projectionDf(pkl,coll,flg)) for pkl,coll,flg,name in zip_longest(pkcollist,collist,skipdup,namelist)])
        def wrapper(projdict):
            def get(dfname):
                return projdict[dfname]
            return get
        return wrapper(projdict)

    @classmethod
    def projColumnTransformationDf(cls,df,f,argslist=[],inpcollist=[],outcollist=[]):
        dfcollist = df.columns
        newargslist = [cls.__elementToTuple(argrec) for argrec in argslist]
        if len(inpcollist) == 0:
            inpcollist = dfcollist
        newinplist = [cls.__elementToTuple(inprec) for inprec in inpcollist]
        if len(outcollist) == 0:
            outcollist = ['-'.join(rec) for rec in newinplist]
        if not inspect.isfunction(f):
            print("Functional argument is empty")
        transformdict = dict([(coln,(f,fargs,trg)) for fargs,trg in zip_longest(tuple(zip_longest(newinplist,newargslist,fillvalue='')),outcollist,fillvalue='') for coln in fargs[0]])
        ##collist = []
        collist = dfcollist
        trancollist = []
        #colset=set()
        colset=set()
        for rec in dfcollist + list(set(inpcollist) - set(dfcollist)):
            if rec in transformdict.keys():
                if transformdict[rec][2] not in colset:
                    trancollist.append(transformdict[rec])
                if transformdict[rec][2] in collist:
                    collist.remove(transformdict[rec][2])
                colset.add(transformdict[rec][2])
            #collist.append(('',rec,rec))
        newcollist = [('',rec,rec) for rec in collist] 
        return df.select(*[cls.__applyFunction(f,fargs,trcol) for f,fargs,trcol in newcollist + trancollist])
    
    '''
    transformer generic decorator
    '''
    @classmethod
    def transformerGeneric(cls,**transformerprop):
        def customTranformer(transformer_func):
            @wraps(transformer_func)
            def wrapper(df,*fargs,**dynprop):
                transformer_prop = PipelineContainer.config.OPERATOR()
                transformer_prop.update(PipelineContainer.config.TRANSFORMER_GENERIC())
                transformer_prop.update(transformerprop)
                transformer_prop.update(dynprop)
                
                PipelineContainer.operator_factory.override(
                    providers.Factory(GenericTransformer,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    transformer_config=transformer_prop,
                    ),
                )
                generic_transformer = PipelineContainer.operator_factory()
                transformer_df = generic_transformer.transformer(df,transformer_func,*fargs)
                return transformer_df
            return wrapper
        return customTranformer

    '''
    write data source specific decorator/generic decorator
    '''
    @classmethod
    def writerGeneric(cls,**writerprop):
        def customWriter(write_func):
            @wraps(write_func)
            def wrapper(df,*fargs,**dynprop):
                
                writer_prop = PipelineContainer.config.WRITER()
                writer_prop.update(PipelineContainer.config.WRITER_GENERIC())
                writer_prop.update(writerprop)
                writer_prop.update(dynprop)

                PipelineContainer.writer_factory.override(
                    providers.Factory(GenericWriter,
                    spark_session=cls.spark,
                    logger=cls.logger,
                    writer_config=writer_prop,
                    ),
                )
                generic_writer = PipelineContainer.writer_factory()
                generic_writer.writer(df,write_func,*fargs)
            return wrapper
        return customWriter

    @classmethod
    def executeRulesDf(cls,df,ruleset):
        config=PipelineContainer.config
        metadata_selector = providers.Selector(
            config.METADATA_RULES.rules_metadata_prepclass,
            defaultmetadataprep=providers.Factory(DefaultMetadataPrep,
            config=config.provider
            ),
            filemetadataprep=providers.Factory(FileMetadataPrep,
            config=config.provider
            ),
        )
        ##PipelineContainer.metadata_factory.override(
        ##            providers.Factory(DefaultMetadataPrep,
        ##            config=config.provider
        ##            ),
        ##)
        ##metadata_prep = PipelineContainer.metadata_factory()
        metadata_prep = metadata_selector()
        ruleset = metadata_prep.rulesprep(ruleset)
        key_columns = ruleset.keycolumns
        rule_columns = key_columns + [expr(rl.getsqlexp()) for rl in ruleset.rulelist]  
        rule_and = expr(' and '.join([rl.getcolname() for rl in ruleset.rulelist]))
        rule_result_df = df.select(*rule_columns)       \
                           .withColumn('result_and',rule_and)
        rule_category =  ruleset.category
        
        if rule_category == 'filter':
            filter_df = rule_result_df.filter(rule_result_df.result_and =='true')
            result_df = df.join(filter_df,key_columns,'inner').select(*df.columns)
        else:
            result_df = df

        ruledict = dict([
            ('validdf',result_df),
            ('originaldf',df),
            ('keycolumnslist',key_columns),
            ('rulecategory',rule_category),
            ('rulesetname',ruleset.rulesetname)
        ])
        def wrapper(ruledict):
            def get(dfname):
                return ruledict[dfname]
            return get
        return wrapper(ruledict)

    @classmethod
    def cleanseDataDf(cls,df,inpcollist=[]):
        dfcollist = inpcollist if len(inpcollist) > 0   else  df.columns
        dfarslist = [('[^\x09\x20-\x7E]','') for rec in dfcollist]
        resultdf = cls.projColumnTransformationDf(df,regexp_replace,argslist=dfarslist,inpcollist=dfcollist,outcollist=dfcollist)
        return resultdf

    @classmethod
    def getMetadataObj(cls):
        config=PipelineContainer.config
        metadata_selector = providers.Selector(
            config.METADATA_RULES.metadata_prepclass,
            defaultmetadataprep=providers.Factory(DefaultMetadataPrep,
            config=config.provider
            ),
            filemetadataprep=providers.Factory(FileMetadataPrep,
            config=config.provider
            ),
        )
        return metadata_selector()