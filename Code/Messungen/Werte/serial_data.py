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

    #Input Mapper
    input_mapper = rdd.count()

    # RDD, weil map action nur in RDD funktioniert
    rdd_mapper = rdd.map(lambda s: (1,s[1]))
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

    # Output Mapper
    output_mapper = df_mapper.count() 
    print("====================================\n")
    print("rdd mapper count: --------", output_mapper)
   


    # Group by key
    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))

    print("df grouped")
    print("====================================\n")
    print(df_grouped.show())
    print("====================================\n")
    print("df grouped count:", df_grouped.count())
    print("====================================\n")

    # number of reducers
    reducercount = df_grouped.count()

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
    file1 = open("serial.txt","a")
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

    print("Programm endet!")
    return result


# Run
time_dict = {}
tic1 = int(round(time.time() * 1000))
melSim = serial().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1