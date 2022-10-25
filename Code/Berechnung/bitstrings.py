import pandas as pd
import numpy as np
from itertools import product
import sys

# Enter Parameters:
b = 8 # length of bitstrings at the beginning that can be different from each other
k = 1 #  datavolume in KByte, k<=68, because of np.array in line 33


datavolume = int(k * 1000) - 49 # if size of object instead of size of string is important
n = 2**b # number of bitstrings (n<= 2**b)


def bitstringOrdered(n,b,datavolume):
    df = pd.DataFrame(columns = ['bitstring'])
    if n > 2**b:
            print("ERROR: n > 2**b !")
    else:
        # generate bitstrings:
        
        # generate product in reverse lexicographic order
        bin_str = [''.join(p) for p in product('01', repeat=b)]

        # generate zeros
        nuller = ('0' * (datavolume-b))
        string = ''

        # join the binary string and the zeros to one bitstring
        for i in range(len(bin_str)):
            seq = (bin_str[i],nuller)
            bin_str[i] = string.join(seq)
        df['bitstring'] = np.array(bin_str)
        if n-len(df) < 0: 
            df.drop(index=df.index[n-len(df)], axis=0, inplace=True)
        
        # get size
        def utf8len(s):
            return len(s.encode('utf-8'))
        s = df['bitstring']
        print("size of object:",(sys.getsizeof(s[0])/1000), "KByte")
        print("size of string:",(utf8len(s[0])/1000), "KByte")

        # delete header
        df.columns = df.iloc[0] 
        df = df[1:]
    
        # save dataframe in csv file
        df.to_csv('bitstrings_b8_k1.csv')# for k: decimal after first number

    return df

bitstringOrdered(n,b,datavolume)




