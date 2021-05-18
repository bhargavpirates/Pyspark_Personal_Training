spark.read.text('/user/af72092/sampledata.txt',lineSep='\n').select(f.expr("regexp_replace(value,'[^\x09\x20-\x7E]','') as cl")).filter(f.expr("length(cl) > 0"))



df1.select(*(expr("regexp_replace("+cll+",'[^\x09\x20-\x7E]','')").alias(cll) for cll in df1.columns)).show()



df1.select(
	*(expr("regexp_replace("+cll+",'[^\x09\x20-\x7E]','')").alias(cll) for cll in df1.columns)


	).show()


spark
.read
.format("csv")
.option('multiLine',True)
.load("/user/af72092/sampledata.txt")
.select(*(f.expr("regexp_replace("+cll+",'[^\x09\x20-\x7E]','')").alias(cll) for cll in df1.columns)).show()
