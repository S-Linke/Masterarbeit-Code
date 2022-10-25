import time
import sys
import re
import numpy as np
import pandas as pd
import copy
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, IntegerType, FloatType
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
#rdd = sc.textFile("bitstrings.csv")
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

# count reducersizes for Volume
def counter(values):
    count = len(values)

    return(count)


# Max - Parallel
def max_parallel():

    # mapper

    #Input Mapper
    input_mapper = df.count()
    
    # key-Spalte mithilfe von mapper Fkt. erzeugen
    mapudf = udf(lambda k,b: mapper(k,b), ArrayType(StringType()))
    # Add neue Spalten zum Dataframe hinzu: key-Spalte
    df_mapper = df.withColumn("key", mapudf('key','bitstrings'))
    df_mapper = df_mapper.withColumn("key", explode('key'))

    # print("====================================\n")
    # print("RDD Mapper:\n")
    # print("====================================\n")
    # print(df_mapper.collect())
    # print("====================================\n")


    # Output Mapper
    output_mapper = df_mapper.count() 
    print("====================================\n")
    print("rdd mapper count: --------", output_mapper)

    # Group by key
    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))

    # print("df grouped")
    # print("====================================\n")
    # print(df_grouped.show())
    # print("====================================\n")
    # print("df grouped count:", df_grouped.count())
    # print("====================================\n")

    # number of reducers
    reducercount = df_grouped.count()

    # reducer
    reduceudf = udf(lambda k, v: reducer(k, v), ArrayType(StringType()))
    df_reducer = df_grouped.withColumn("result", reduceudf('key','bitstrings'))

    # print("====================================\n")
    # print("rdd reducer")
    # print(df_reducer.collect())
    # print("====================================\n")
    # print("rdd reducer count:", df_reducer.count())
    # print("====================================\n")


    # df_reducer['output_result'] = df_reducer.df['result'].astype(str).str[:b]
    result = df_reducer.select("result").withColumn("result", explode('result'))
    print(result.show())

    
    # Volume and Reducer size
    
    # Datavolume key
    key_volume = sys.getsizeof(df_reducer['key'][1])/1000 # datavolume in KByte
    # Reducersize q_i
    countdf = udf(lambda v: counter(v),IntegerType())
    df_counter = df_grouped.withColumn("number", countdf('bitstrings'))
    
    # number of same reducer counts
    df_one_reducer = df_counter.groupBy('number').count()
    row = df_one_reducer.collect()

    # Datavolume Reducer Output
    end_datavolume = result.count()
    output_reducer = (k*2+key_volume)* end_datavolume # *2, because every output is pair of elements
    
    # Reducersize q
    max = df_counter.agg({'number': 'max'}).collect()[0][0]

    #sum reducersizes j
    sum = df_counter.agg({'number': 'sum'}).collect()[0][0]

    # Datavolume Reducer Input
    input_reducer = (k+key_volume)*int(sum)


    # write data in a file.
    file1 = open("maxparallel.txt","a")
    file1.write("\n")
    L1 = ["Input alle Mapper",str(input_mapper),"Output alle Mapper",str(output_mapper),'\n'] 
    file1.write('\n'.join(L1))
    L2 = ["Datenvolumen Input alle Mapper",str(input_mapper*(k)),"Datenvolumen Output alle Mapper",str(output_mapper*(k+key_volume)),'\n'] 
    file1.write('\n'.join(L2))
    L6 = ["Datenvolumen Input alle Reducer",str(input_reducer),"Datenvolumen Output alle Reducer",str(output_reducer),"key Volume:",(str(key_volume)),'\n'] 
    file1.write('\n'.join(L6))
    L3 = ['Reducersize q:',str(max),'sum reducersizes j:', str(sum),'\n']
    file1.write('\n'.join(L3))
    L4 = ["Anzahl Reducer",str(reducercount),"Reducersize q_i:",'\n']
    file1.write('\n'.join(L4))
    for j in range (df_one_reducer.count()):
        L = [str(row[j]),'\n']
        file1.write('\n'.join(L))
    L5 = ["b=",str(b),"k=", str(k),"====================================",'\n']
    file1.write('\n'.join(L5))
    file1.close()

    return result



# Run
time_dict = {}
tic1 = int(round(time.time() * 1000))
melSim = max_parallel().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1


print("Program ended!")