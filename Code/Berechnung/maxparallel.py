import time
import sys
import re
import numpy as np
import pandas as pd
import copy
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode
from pyspark.sql.functions import collect_list
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


confCluster = SparkConf().setAppName("HashingCluster")
sc = SparkContext(conf=confCluster)
sqlContext = SQLContext(sc)

print("Program start!")

#Algorithmus:

# csv file in RDD 
rdd = sc.textFile("bitstrings_b8_k1.csv") 
rdd = rdd.map(lambda x: x.split(','))
df = rdd.toDF(["key","bitstrings"])
n = df.count()

# change for different csv files
b = 8
k= 1

# Mapper
def mapper(key,values):
    keypair = []
    
    for j in range(b):
            # sj berechnen
        s_ = copy.copy(values)
                     
        if values[j] == '1':
            s_ = s_[:j] + '0' + s_[j+1:]
            keypair.append(set((values,s_))) 
        
        elif values[j] == '0':
            s_ = s_[:j] + '1' + s_[j+1:]
            keypair.append(set((values,s_)))

    return(keypair) # ist der key, zu values, unordered set
    



# Reducer
def reducer(key,values):
    output = []

    if len(values) == 2:
        output.append("["+values[0]+ "," + values[1]+"]")
    return(output)


# Max - Parallel
def max_parallel():

    # mapper
    mapudf = udf(lambda k,b: mapper(k,b), ArrayType(StringType()))
    df_mapper = df.withColumn("key", mapudf('key','bitstrings'))
    df_mapper = df_mapper.withColumn("key", explode('key'))

    # Group by key
    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))

    # reducer
    reduceudf = udf(lambda k, v: reducer(k, v), ArrayType(StringType()))
    df_reducer = df_grouped.withColumn("result", reduceudf('key','bitstrings'))

    result = df_reducer.select("result").withColumn("result", explode('result'))
    print(result.show())

    return result



# Run
time_dict = {}
tic1 = int(round(time.time() * 1000))
melSim = max_parallel().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1


print("Program ended!")