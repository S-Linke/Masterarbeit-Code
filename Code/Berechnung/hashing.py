import time
import sys
import re
import numpy as np
import pandas as pd
from scipy.spatial import distance

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode
from pyspark.sql.functions import collect_list
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import spark_partition_id


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
k = 1
J = 2

# blockid
repartition_count = J
df = df.repartition(repartition_count).withColumn("BlockID", spark_partition_id()) 
print(df.show())



def mapper(blockid, values):
    keypair = []
    i = hash(blockid) # 0<=blockid<J
    for j in range(J):

        if j >= i: 
                keypair.append(str(i)+','+str(j))
        else:
                keypair.append(str(j)+','+str(i))
    return(keypair)

# Reducer
def reducer(key,values):
    result = []
    q = len(values)
    for k in range(1,q): 
        for l in range(k): 
            # [0:b], because distance.hamming does not work with the total range
            dist = distance.hamming(list(values[k][0:b]),list(values[l][0:b])) * b
            if dist == 1 :
                result.append("["+values[k]+ "," + values[l]+"]")
          
    return(result)


# Hashing
def hashing():
    
    # mapper
    # key-Spalte mithilfe von mapper Fkt. erzeugen
    mapudf = udf(lambda k,b: mapper(k,b), ArrayType(StringType()))
    df_mapper = df.withColumn("key", mapudf('BlockID','bitstrings'))
    df_mapper = df_mapper.withColumn("key", explode('key'))

    # print("====================================\n")
    # print(df_mapper.show())
    # print("====================================\n")
    # print(df_mapper.count())
    # print("====================================\n")
   
    map_count = df_mapper.count()
    print("====================================\n")
    print("df mapper count: --------", map_count)

    # Group by key
    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))

    # print("df grouped")
    # print("====================================\n")
    # print(df_grouped.show())
    # print("====================================\n")
    # print("df grouped count:", df_grouped.count())
    # print("====================================\n")

    # reducer
    reduceudf = udf(lambda k, v: reducer(k, v), ArrayType(StringType()))
    df_reducer = df_grouped.withColumn("result", reduceudf('key','bitstrings'))

    
    print("df reducer")
    print("====================================\n")
    print(df_reducer.show())
    print("====================================\n")
    print("df reducer count:", df_reducer.count())
    print("====================================\n")

    
    result = df_reducer.select("result").withColumn("result", explode('result'))
    print(result.show())    


    print("Programm endet!")
    return result


# Run
time_dict = {}
tic1 = int(round(time.time() * 1000))
melSim = hashing().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1