# Commonly used pyspark scripts

"""
Run this command in terminal before running pyspark code :


export PYSPARK_PYTHON=/usr/bin/python35
export PYSPARK_DRIVER_PYTHON=python35
source ~/.bashrc

Reading and writing directly from s3 will need

"""

### Change and print default configuration (in case of problems like executors running out of memory)

# Hunch - some of these settings are fixed while cluster creation

import pyspark


config = pyspark.SparkConf().setAll([('spark.driver.memory','32g'), ("spark.sql.crossJoin.enabled", "true")])
sc.stop()
sc = pyspark.SparkContext(conf=config)

print(sc._jsc.sc().getExecutorMemoryStatus().keySet().size())
sc.getConf().getAll()

### Imports


from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

sqlContext = SQLContext(sc)

### Read pipe separated data files present in a folder - Can read plaintxt, csv or even zip

columns = ['column1', 'column2', 'column3'] # list of column names


df = sqlContext.read.option("delimiter","|").option("quote", "").csv("s3://path_to_data/")

df = sqlContext.read.option("delimiter","|").option("quote", "").csv("s3://path_to_data/prefix*") # Can also do a prefix search on the files

df = df.toDF(*columns)

### Data Munging


df = df.where(df['column'].contains('29'))
df.select('column').distinct().count()
df = df.withColumn("column", col("column").cast("double"))
df = df.dropna(how="any",subset=[col_list])
df = df.drop_duplicates(subset=['column'])


### Group by having - aggregations

# only keep those rows in the dataframe, which have a subject_id containing more than 50 unique students

intentGroupBy = df.groupby('subject_id').agg(func.countDistinct("student_id").alias("unique_students")).sort(func.desc("unique_students"))
intentGroupBy = intentGroupBy.filter(intentGroupBy.unique_students >= 50)

intentList = intentGroupBy.select('subject_id').collect()
intentList = set([row['subject_id'] for row in intentList])


df = df.filter(func.col('subject_id').isin(intentList))

# more aggregations

student_subject = df.groupby(['student_id', 'subject_id']).agg(func.avg("grade").alias('average_grades'), func.count("grade").alias('total_times_tested'))

# write data to s3

user_merchant.write.mode('overwrite').option("delimiter","|").option("header", "false").csv('s3://s3yodlee/samiran/lightroom/data_swimming/user_merchant/')

# write data as json

data.write.format('json').save("s3://file_path.json")

### Simple EMR functions

import re

def parse_string(x):
	return re.sub('[^A-Za-z0-9 ]+', '', x)


parse_string = func.udf(parse_string, StringType()) # return type needs to be specified
df = df.withColumn("column", parse_merchant(df["column"]))

### Groupby dataframe apply

# This code does a quantile calculation for how much each student performs each year in all subjects

from pyspark.sql.functions import pandas_udf,PandasUDFType

def return_schema(cols):
	schema = []
	for col in cols:
		schema.append(StructField(col, StringType()))
	for i in range(0,10):
		schema.append(StructField('QUANTILE' + str(i*10), DoubleType()))
	return StructType(schema)


schema = return_schema(['student_id', 'year'])

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def g(df):
	import numpy as np, pandas as pd
	student = list(df['student_id'].sample(1))[0]
	year = list(df['year'].sample(1))[0]
	grades = np.array(df['grades'])
	percentiles = []
	for i in range(0,10):
		percentiles += [np.percentile(grades, i*10)]
	return pd.DataFrame([[student] + [year] + percentiles])

yearly_student_subject_percentiles = merch_monetary_value.groupby(['student_id', 'year', 'subject_id']).agg(func.sum("grades").alias('yearly_grades'))

student_performance_quantile_scores = yearly_student_subject_percentiles.groupby(["student_id", "year"]).apply(g).collect()

### Using windows to filter data

window = Window.partitionBy(df['student_id'], df['has_passed'])
df = df.withColumn('count', func.count('*').over(window)).where('count > 4') # select only those students who have passed in more than 4 subjects

### Define global accumalators and update them

from pyspark.accumulators import AccumulatorParam


class DictParam(AccumulatorParam):
    def zero(self,  value = ""):
        return {}
    def addInPlace(self, acc1, acc2):
        acc1.update(acc2)
        return acc1

model_dict = sc.accumulator({}, DictParam())


schema = StructType(['some schema' ])


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def testFunc(pdf):
	# do some computation
    global model_dict
    model_dict+={(key1,key2):[val2, val2]}

    return appropriate_schema


df.groupby([col1, col2]).apply(testFunc).collect()



# Large scale word count

importance_threshold = 50 # A word must occur this many times in the new dataset to be considered significant


descriptions = df.select(["text_field"])

wc = descriptions.withColumn('word', func.explode(func.split(func.col('text_field'), ' '))).groupBy('word').count().filter(func.col('count')>importance_threshold).toPandas()
wc = pd.Series(wc['count'].values,index=wc.word).to_dict()

