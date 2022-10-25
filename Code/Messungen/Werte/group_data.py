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
df = rdd.toDF(["key","bitstrings"])
n = df.count()

# change for different csv files
# Mapper
b = 8
k= 1

u = 2 # n*u = b/2; n natural number, size of group
# groups:
group_number = b/(2*u)


# Mapper
# zum ausprobieren b > 2
def mapper(key,values):
    keypair = []
    # number of outputs from each mapper
    r=0

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
    r = r + 1

    # check if i or j at border:
    if weight_left==(I-1)*u and I>1:
        keypair.append(str(I -1)+ "," + str(J))
        r = r + 1

    if weight_right==(J-1)*u and J>1: 
        keypair.append(str(I)+ "," + str(J-1))
        r = r + 1

    file1 = open("group.txt","a")
    file1.write("\n")
    L = ["emits per mapper:",str(r)]
    file1.write('\n'.join(L))
    file1.close()

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
    # result['output_result'] = result['result'].str[:1] # und str[k:k+b]
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
    file1 = open("group.txt","a")
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
    L5 = ["b=",str(b),"k=", str(k),"u=", str(u),"====================================",'\n']
    file1.write('\n'.join(L5))
    file1.close()
    


    print("Programm endet!")
    return result


# Run
time_dict = {}
tic1 = int(round(time.time() * 1000))
melSim = group().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1