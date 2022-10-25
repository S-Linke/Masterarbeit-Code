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
# Mapper
b = 8 
k= 1

u = 2 # n*u = b/2; n natural number
# groups:
group_number = b/(2*u)


# Mapper
# zum ausprobieren b > 2
def mapper(key,values):
    keypair = []

    # part string in 2 parts
    l= int(b/2)

    # groups
    weight_left = 0
    weight_right = 0
    for i in range(0,l):
        # weight: count ones
        if int(values[i])==1:
            weight_left += 1
            
    if weight_left==b/2:
        I = int(weight_left/u)
    else: I = int(weight_left/u) + 1

    for j in range(l,b): 
        # weight: count ones
        if int(values[j])==1:
            weight_right += 1

    if weight_right==b/2:
        J = int(weight_right/u)
    else: J = int(weight_right/u) + 1

    keypair.append(str(I)+ "," + str(J))


    # check if i or j at border:
    if weight_left==(I-1)*u and I>1:
        keypair.append(str(I -1)+ "," + str(J))

    if weight_right==(J-1)*u and J>1: 
        keypair.append(str(I)+ "," + str(J-1))

    return(keypair)


# Reducer
def reducer(key,values):
    result = []
    q = len(values)
    
    for i in range(1,q): 
        for j in range(i): 
            dist = distance.hamming(list(values[i][0:b]),list(values[j][0:b])) * b
            if dist == 1 :
                result.append("["+values[j]+ "," + values[i]+"]")
          
    return(result)


# Group
def group():
    
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
melSim = group().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1

print("Programm endet!")