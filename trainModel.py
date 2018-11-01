import csv
import os
import pandas as pd
import numpy as np

# the data input in this case is a large csv. 
# This program produces a log file with all the execution times, and a model that I can load afterwards.

def DataReader (datasetPath, headers):

    df = pd.read_csv(datasetPath, names= headers)
    # testing
    print(df)
    DataProcessor(df)

def DataProcessor (df):
    timeHeaders =  ["ENDTIME", "STARTTIME"] #convert to timestamp
    countHeaders = ["PACKETINCOUNT", "PACKETINCOUNT", "PACKETOUTCOUNT", "BYTEINCOUNT"] #convert to int32
    flowHeaders = ["TRANSPORTFLAGS", "FLOWS"] #convert to int8

    for column in df:
        if column in timeHeaders:
            print (df[column].dtypes)
            for rows in df[column]:
               #convert to timestamps must know the format before converting
               

        if df[column].values in countHeaders:
            print (df[column].dtypes)
            for rows in df[column]:
                rows=rows.astype(np.int32)

        if df[column].values in flowHeaders:
            print (df[column].dtypes)
            for rows in df[column]:
                rows=rows.astype(np.int8)
    
    #sort the dataframe by endtime
    df = df.sort(columns = "ENDTIME", ascending=True)

    #add duration column as a engineered feature.
    df["Duration"] = Series(len(df["ENDTIME"]), index=df.index)
    for entry in df["Duration"]:
        entry = df["ENDTIME"][entry.rows] - df["STARTTIME"][entry.rows]

    #FeatureGenerator_TimeBased(df)

#def FeatureGenerator_TimeBased (sortedDf):

# def FeatureGenerator_ConnectionBased (sortedDF):

# def DataDiscretizer (sortedDF):

# def DataPreparation (sortedDF):

# def ModelRunner (sortedDF):

#get Dataset path and headers
datasetPath = "NSLKDDDataset\\NSL_KDD-master\\CSVData\\SmallTrainingSet.csv"
headers = ["duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised", "root_shell","su_attempted", "num_root", "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login", "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate", "dst_host_rerror_rate", "dst_host_srv_rerror_rate"] #add columns afterwards

DataReader(datasetPath, headers)