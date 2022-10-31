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
confCluster.set("spark.driver.memory", "64g")
confCluster.set("spark.executor.memory", "64g")
confCluster.set("spark.driver.memoryOverhead", "32g")
confCluster.set("spark.executor.memoryOverhead", "32g")
#Be sure that the sum of the driver or executor memory plus the driver or executor memory overhead is always less than the value of yarn.nodemanager.resource.memory-mb
#confCluster.set("yarn.nodemanager.resource.memory-mb", "196608")
#spark.driver/executor.memory + spark.driver/executor.memoryOverhead < yarn.nodemanager.resource.memory-mb
confCluster.set("spark.yarn.executor.memoryOverhead", "4096")
#set cores of each executor and the driver -> less than avail -> more executors spawn
confCluster.set("spark.driver.cores", "1")
confCluster.set("spark.executor.cores", "1")
#confCluster.set("spark.shuffle.service.enabled", "True")
confCluster.set("spark.dynamicAllocation.enabled", "True")
#confCluster.set("spark.dynamicAllocation.initialExecutors", "16")
#confCluster.set("spark.dynamicAllocation.executorIdleTimeout", "30s")
confCluster.set("spark.dynamicAllocation.minExecutors", "576")
confCluster.set("spark.dynamicAllocation.maxExecutors", "576")
confCluster.set("yarn.nodemanager.vmem-check-enabled", "false")
repartition_count = 36


sc = SparkContext(conf=confCluster)
sqlContext = SQLContext(sc)

print("Program start!")

#Algorithmus:

# Csv Datei in RDD Einlesen
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
    #string = []
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

# count reducersizes for Volume
def counter(values):
    count = len(values)

    return(count)

# Reducer
def reducer(key,values):
    result = []
    q = len(values)
    
    for i in range(1,q): 
        for j in range(i): 
            # [0:b], because distance.hamming does not work with the total range
            dist = distance.hamming(list(values[i][0:b]),list(values[j][0:b])) * b
            if dist == 1 :
                result.append("["+values[j]+ "," + values[i]+"]")
          
    return(result)


# Group
def group():
    
     # mapper
    
    # key-Spalte mithilfe von mapper Fkt. erzeugen
    mapudf = udf(lambda k,b: mapper(k,b), ArrayType(StringType()))
    # Add neue Spalten zum Dataframe hinzu: key-Spalte
    tic1 = int(round(time.time() * 1000))
    
    df_mapper = df.withColumn("key", mapudf('key','bitstrings'))
    df_mapper.first()

    tac1 = int(round(time.time() * 1000))
    time_mapper = tac1 - tic1
    print("Time Mapper:\n", time_mapper)

    df_mapper = df_mapper.withColumn("key", explode('key'))


    # print("====================================\n")
    # print("RDD Mapper:\n")
    # print("====================================\n")
    # print(df_mapper.collect())
    # print("====================================\n")

    # Group by key
    tic2 = int(round(time.time() * 1000))

    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))
    df_grouped.first()

    tac2 = int(round(time.time() * 1000))
    time_groupbykey = tac2 - tic2
    print("Time roup by key: \n",time_groupbykey)


    # print("df grouped")
    # print("====================================\n")
    # print(df_grouped.show())
    # print("====================================\n")
    # print("df grouped count:", df_grouped.count())
    # print("====================================\n")

    # reducer
    reduceudf = udf(lambda k, v: reducer(k, v), ArrayType(StringType()))
    tic3 = int(round(time.time() * 1000))

    df_reducer = df_grouped.withColumn("result", reduceudf('key','bitstrings'))
    df_reducer.first()

    tac3 = int(round(time.time() * 1000))
    time_reducer = tac3 - tic3
    print("Time Reducer: \n", time_reducer)

    
    # print("df reducer")
    # print("====================================\n")
    # print(df_reducer.show())
    # print("====================================\n")
    # print("df reducer count:", df_reducer.count())
    # print("====================================\n")

    
    result = df_reducer.select("result").withColumn("result", explode('result'))
    print(result.show())  

    # write data in a file.
    file1 = open("group.txt","a")
    file1.write("\n")
    L = ["Mapper Time:",str(time_mapper),"GBK Time:",str(time_groupbykey),"Reducer Time:",str(time_reducer)] 
    file1.write('\n'.join(L))
    L1 = ["group:",str(group_number),'\n'] 
    file1.write('\n'.join(L1))
    file1.close()  

    return result


# Run

tic1 = int(round(time.time() * 1000))
melSim = group().persist()
tac1 = int(round(time.time() * 1000))
time_dict= tac1 - tic1
print(time_dict)

# write data in a file.
file1 = open("group.txt","a")
file1.write("\n")
L = ["overall Time group:",str(time_dict),'\n'] 
file1.write('\n'.join(L))
file1.close()

print("Programm endet!")