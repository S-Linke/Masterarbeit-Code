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

# change for different csv files
b = 8 
k= 1


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
    rdd_mapper = rdd.map(lambda s: (1,s[1]))
    
    
    df_mapper=rdd_mapper.toDF(["key","bitstrings"]   )
    
    #df_mapper = df.withColumn("key", myudf('bitstrings','data_volume k'))
    #df_mapper = df_mapper.withColumn("key", explode('key'))
    # print("====================================\n")
    # print(df_mapper.show(3))
    # print("====================================\n")
    # print(df_mapper.count())
    # print("====================================\n")
   
    map_count = df_mapper.count()
    print("====================================\n")
    print("df mapper count: --------", map_count)

    # Group by key
    df_grouped = df_mapper.groupby('key').agg(collect_list('bitstrings').alias("bitstrings"))

    print("df grouped")
    print("====================================\n")
    print(df_grouped.show())
    print("====================================\n")
    print("df grouped count:", df_grouped.count())
    print("====================================\n")

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


    print("Programm endet!")
    return result


# Run
time_dict = {}
tic1 = int(round(time.time() * 1000))
melSim = serial().persist()
tac1 = int(round(time.time() * 1000))
time_dict['Time: ']= tac1 - tic1