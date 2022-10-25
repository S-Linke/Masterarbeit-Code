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

# change for different csv files
b = 8 
k= 1

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


# Seriell
def serial():
    
    # mapper
    # RDD, weil map action nur in RDD funktioniert
    tic1 = int(round(time.time() * 1000))
    
    rdd_mapper = rdd.map(lambda s: (1,s[1]))

    tac1 = int(round(time.time() * 1000))
    time_mapper = tac1 - tic1
    print("Time Mapper:\n", time_mapper)
    
    # print("====================================\n")
    # print("RDD Mapper:\n")
    # print("====================================\n")
    # print(rdd_mapper.collect())
    # print("====================================\n")

    
    df_mapper=rdd_mapper.toDF(["key","bitstrings"]   )
    
    #df_mapper = df.withColumn("key", myudf('bitstrings','data_volume k'))
    #df_mapper = df_mapper.withColumn("key", explode('key'))
    # print("====================================\n")
    # print(df_mapper.show(3))
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
    file1 = open("serial.txt","a")
    file1.write("\n")
    L = ["Mapper Time:",str(time_mapper),"GBK Time:",str(time_groupbykey),"Reducer Time:",str(time_reducer)] 
    file1.write('\n'.join(L))
    file1.close()
    
    return result


# Run
tic1 = int(round(time.time() * 1000))
melSim = serial().persist()
tac1 = int(round(time.time() * 1000))
time_dict = tac1 - tic1

# write data in a file.
file1 = open("serial.txt","a")
file1.write("\n")
L = ["overall Time serial:",str(time_dict),'\n'] 
file1.write('\n'.join(L))
file1.close()

print("Programm endet!")