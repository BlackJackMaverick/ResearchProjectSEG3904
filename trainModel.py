import csv
import os
import pandas as pd
import numpy as np

# the data input in this case is a large csv. 
# This program produces a log file with all the execution times, and a model that I can load afterwards.

def DataReader (datasetPath):

    #read the dataset
    df = pd.read_csv(datasetPath)
    # test by printing the number of rows
    print("Dataset contains " + str(df.STARTTIME.size) + " rows.")
    #send the dataset to the data processor 
    DataProcessor(df)

def DataProcessor (df):
    timeHeaders =  ["ENDTIME", "STARTTIME"] #headers that will be converted to timestamps
    countHeaders = ["PACKETINCOUNT", "PACKETINCOUNT", "PACKETOUTCOUNT", "BYTEINCOUNT"] #headers that will be converted to int32
    flowHeaders = ["TRANSPORTFLAGS", "FLOWS"] #Headers that will be converted to int8

    for column in df:
        #if the column lies in the time headers array then convert to timestamp.
        if column in timeHeaders:
            df[column] = pd.to_datetime(df[column])
            print (df[column])

        #if the column lies in the count headers array then convert to int32.
        if column in countHeaders:
            df[column] = df[column].astype(np.int32)
            print(df[column])

        #if the column lies in the flow headers array then convert to int8.   
        if column in flowHeaders:
            df[column] = df[column].astype(np.int8)
            print(df[column])

    df = df.sort_values("ENDTIME")  #sort the dataframe by endtime
    print (df)
    
    #add duration column as a engineered feature.
    df = df.assign(DURATION = (df.ENDTIME-df.STARTTIME))
    print(df)
    FeatureGenerator(df)

#connection based and time based features will be created in this function
def FeatureGenerator (df):
    #first connection based engineered features by source address
    windowSize = 10 #will be increased to 1000 in full datatset
    windowIndex = windowSize 
    
    #create the columns to store the engineered features and append to existing dataframe
    featureHeaders = [
    "CONN_BASED_SRCADDRESS_DISTINCT_DSTPORTS", "CONN_BASED_SRCADDRESS_DISTINCT_DSTADDRESS", "CONN_BASED_SRCADDRESS_DISTINCT_SRCPORTS", "CONN_BASED_SRCADDRESS_DISTINCT_AVGPACKETIN", "CONN_BASED_SRCADDRESS_DISTINCT_AVGBYTEIN",
    "CONN_BASED_DSTADDRESS_DISTINCT_DSTPORTS", "CONN_BASED_DSTADDRESS_DISTINCT_DSTADDRESS", "CONN_BASED_DSTADDRESS_DISTINCT_SRCPORTS", "CONN_BASED_DSTADDRESS_DISTINCT_AVGPACKETIN", "CONN_BASED_DSTADDRESS_DISTINCT_AVGBYTEIN",
    "TIME_BASED_SRCADDRESS_DISTINCT_SRCADDRESS", "TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS", "TIME_BASED_SRCADDRESS_DISTINCT_AVGPACKETIN", "TIME_BASED_SRCADDRESS_DISTINCT_AVGBYTEIN",
    "TIME_BASED_DSTADDRESS_DISTINCT_SRCADDRESS", "TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS", "TIME_BASED_DSTADDRESS_DISTINCT_AVGPACKETIN", "TIME_BASED_DSTADDRESS_DISTINCT_AVGBYTEIN", 
    ]
    featureDf = pd.DataFrame(columns = featureHeaders)
    df = pd.concat([df, featureDf], axis=1, join_axes=[df.index])

    #TESTING
    #df.to_csv("Dataset\\testing\\concatTest.csv", encoding='utf-8', index=False)

    while windowIndex < len(df.index):
        #save the source address of the end of the rolling window
        targetSourceAddress = df["SRCADDRESS"].iloc[windowIndex]
        targetDestinationAddress = df["DSTADDRESS"].iloc[windowIndex]

        #create the rolling window which is a subset of the dataframe. 
        #The rolling window is of size windowSize and looks at the previous rows from the index 
        #that match the source address of the target.
        srcRWdf = df.iloc[windowIndex-windowSize : windowIndex]
        index = srcRWdf.index[srcRWdf["SRCADDRESS"] == targetSourceAddress]
        srcRWdf = srcRWdf.loc[index]

        #repeat for destination address
        dstRWdf = df.iloc[windowIndex-windowSize : windowIndex]
        index = dstRWdf.index[dstRWdf["DSTADDRESS"] == targetDestinationAddress]
        dstRWdf = dstRWdf.loc[index]

        #from these rolling windows count distinct destination ports, source IPs, source ports, average packet count, average byte count
        
        windowIndex = windowIndex + windowSize #at the end go to the next rolling window            

#helper function counts the unique rows based on the column
def getDistinctRowCount(df, columnName):
    uniqueRows = []
    count = 0
    for row in df[columnName]:
        if row in uniqueRows:
            continue
        else:
            count = count + 1
            uniqueRows.append(row)
    return count       


#get Dataset path and headers
datasetPath = "Dataset\\TestDataSet.csv"
DataReader(datasetPath)