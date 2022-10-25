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
b = 8 
k = 1
J = 2

# blockid
repartition_count = J
df = df.repartition(repartition_count).withColumn("BlockID", spark_partition_id()) 
print(df.show())


# count reducersizes for Volume
def counter(values):
    count = len(values)

    return(count)

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
    # Add neue Spalten zum Dataframe hinzu: key-Spalte
    tic1 = int(round(time.time() * 1000))
    
    df_mapper = df.withColumn("key", mapudf('BlockID','bitstrings'))

    tac1 = int(round(time.time() * 1000))
    time_mapper = tac1 - tic1
    print("Time Mapper:\n", time_mapper)
    
    df_mapper = df_mapper.withColumn("key", explode('key'))

    # print("====================================\n")
    # print(df_mapper.show())
    # print("====================================\n")
    # print(df_mapper.count())
    # print("====================================\n")
   

    # Group by key
    tic2 = int(round(time.time() * 1000))

    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))

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
    file1 = open("hashing.txt","a")
    file1.write("\n")
    L = ["Mapper Time:",str(time_mapper),"GBK Time:",str(time_groupbykey),"Reducer Time:",str(time_reducer)] 
    file1.write('\n'.join(L))
    L1 = ["J:",str(J),'\n'] 
    file1.write('\n'.join(L1))
    file1.close()

    return result


# Run

tic1 = int(round(time.time() * 1000))
melSim = hashing().persist()
tac1 = int(round(time.time() * 1000))
time_dict= tac1 - tic1

# write data in a file.
file1 = open("hashing.txt","a")
file1.write("\n")
L = ["overall Time hashing:",str(time_dict),'\n'] 
file1.write('\n'.join(L))
file1.close()

print("Programm endet!")