import csv
import os
import pandas as pd
import numpy as np
import itertools as IT

def CreateTimeBasedFeatures (dfref, dfSize, windowIndex):
    
    #size of the rolling window based on time
    #will be increased to 10 minutes in full dataset
    timeSize = pd.Timedelta(Minute(1))

    #generate the first rolling window by starting with the first row and going down. 
    startTime = pd.read_csv(dfref, skiprows=range(0, windowIndex), nrows=1)
    startTime = startTime["STARTTIME"].iloc[0]
    endTime = startTime + timeSize

#get the rows that are within the time frame
    chunksize = 10 #can be increased in full data set
    chunks = pd.read_csv(dfref, chunksize=chunksize)
    chunks = IT.takewhile(lambda chunk: startTime <= chunk["STARTTIME"].iloc[-1] <= endTime, chunks)
    df = pd.concat(chunks)
    mask = startTime <= df["STARTTIME"] <= endTime
    df = df.loc[mask]

#get the rows that are equal to the last source address
    targetSourceAddress = df["SRCADDRESS"].iloc[-1]
    index = df.index[df["SRCADDRESS"] == targetSourceAddress]
    srcdf = df.loc[index]

#do the same for the last destination address
    targetDestinationAddress = df["DSTADDRESS"].iloc[-1]
    index = df.index[df["DSTADDRESS"] == targetDestinationAddress]
    dstdf = df.loc[index]

    #populate the dataframe with the engineered features
    df["TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES"].iloc[-1] = len(df.index)
    df["TIME_BASED_SRCADDRESS_OCCURENCES"].iloc[-1] = len(srcdf.index)
    df["TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS"].iloc[-1] = srcdf["DSTADDRESS"].nunique()
    df["TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS"].iloc[-1] = srcdf["DSTPORT"].nunique()
    df["TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS"].iloc[-1] = srcdf["SRCPORT"].nunique()
    df["TIME_BASED_SRCADDRESS_AVGPACKETIN"].iloc[-1] = srcdf["PACKETINCOUNT"].mean()
    df["TIME_BASED_SRCADDRESS_AVGBYTEIN"].iloc[-1] = srcdf["BYTEINCOUNT"].mean()

    #repeat for the destination address
    df["TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES"].iloc[-1] = len(df.index)
    df["TIME_BASED_DSTADDRESS_OCCURENCES"].iloc[-1] = len(dstdf.index)
    df["TIME_BASED_DSTADDRESS_DISTINCT_SRCADDRESS"].iloc[-1] = dstdf["SRCADDRESS"].nunique()
    df["TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS"].iloc[-1] = dstdf["DSTPORT"].nunique()
    df["TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS"].iloc[-1] = dstdf["SRCPORT"].nunique()
    df["TIME_BASED_DSTADDRESS_AVGPACKETIN"].iloc[-1] = dstdf["PACKETINCOUNT"].mean()
    df["TIME_BASED_DSTADDRESS_AVGBYTEIN"].iloc[-1] = dstdf["BYTEINCOUNT"].mean()

    #return the last row of the dataframe  that will contain all the information about rolling window
    return df.iloc[-1]

#Function used to create connection based features. 
#These features will be created based off a rolling window and and rows before will be left blank.
def CreateConnectionBasedFeatures (dfref, dfSize):

    #name of new dataframe and create it
    newDfRef = "Dataset\\testing\\FeatureGenerator.csv"
    

    #rows 0-windowSize will remain blank for the engineered connection based features.
    #WindowSize will be increased to 1000 in full datatset.
    windowSize = 10 
    windowIndex = 0
    
    while windowIndex < dfSize:

        #fetch a subset of the dataframe of length windowsize
        df = pd.read_csv(dfref, skiprows=range(1,windowIndex), nrows=windowSize)
        
        #save the source and destination IP address of the last row of the rolling window
        targetSourceAddress = df["SRCADDRESS"].iloc[-1]
        targetDestinationAddress = df["DSTADDRESS"].iloc[-1]

        #The rolling window is of size windowSize and looks at the previous rows from the index 
        #that match the source address of the target.
        index = df.index[df["SRCADDRESS"] == targetSourceAddress]
        srcdf = df.loc[index]

        #repeat for destination address
        index = df.index[df["DSTADDRESS"] == targetDestinationAddress]
        dstdf = df.loc[index]

        #from the rolling window fill out the engineered features for the source address
        df["CONN_BASED_SRCADDRESS_OCCURENCES"].iloc[-1] = len(srcdf.index)
        df["CONN_BASED_SRCADDRESS_DISTINCT_DSTPORTS"].iloc[-1] = srcdf["DSTPORT"].nunique()
        df["CONN_BASED_SRCADDRESS_DISTINCT_DSTADDRESS"].iloc[-1] = srcdf["DSTADDRESS"].nunique()
        df["CONN_BASED_SRCADDRESS_DISTINCT_SRCPORTS"].iloc[-1] = srcdf["SRCPORT"].nunique()
        df["CONN_BASED_SRCADDRESS_AVGPACKETIN"].iloc[-1] = srcdf["PACKETINCOUNT"].mean()
        df["CONN_BASED_SRCADDRESS_AVGBYTEIN"].iloc[-1] = srcdf["BYTEINCOUNT"].mean()

        #repeat the features for the destination address
        df["CONN_BASED_DSTADDRESS_OCCURENCES"].iloc[-1] = len(dstdf.index)
        df["CONN_BASED_DSTADDRESS_DISTINCT_DSTPORTS"].iloc[-1] = dstdf["DSTPORT"].nunique()
        df["CONN_BASED_DSTADDRESS_DISTINCT_SRCADDRESS"].iloc[-1] = dstdf["DSTADDRESS"].nunique()
        df["CONN_BASED_DSTADDRESS_DISTINCT_SRCPORTS"].iloc[-1] = dstdf["SRCPORT"].nunique()
        df["CONN_BASED_DSTADDRESS_AVGPACKETIN"].iloc[-1] = dstdf["PACKETINCOUNT"].mean()
        df["CONN_BASED_DSTADDRESS_AVGBYTEIN"].iloc[-1] = dstdf["BYTEINCOUNT"].mean()

        #use helper method to get a dataframe of the timebased features
        timeBasedDf = CreateConnectionBasedFeatures(dfref, dfSize, df.index[df.iloc[-1]])
        df.update(timeBasedDf)

        # cleanup for next round of loop.
        # Apply the editted row to the dataframe and save.
        # Increase index to next row. 
        with open(newDfRef, 'a') as f:
                df.iloc[-1].to_csv(f, header=False)

dataframeReference = "Dataset\\testing\\TrainModel.csv"

#create the columns to store the engineered features and append to existing dataframe. Then save it.
#this operation may take a long time based on the size of the dataframe, but it only needs to be done once.
    
featureHeaders = [
"CONN_BASED_SRCADDRESS_OCCURENCES", "CONN_BASED_SRCADDRESS_DISTINCT_DSTPORTS", "CONN_BASED_SRCADDRESS_DISTINCT_DSTADDRESS", "CONN_BASED_SRCADDRESS_DISTINCT_SRCPORTS", "CONN_BASED_SRCADDRESS_AVGPACKETIN", "CONN_BASED_SRCADDRESS_AVGBYTEIN",
"CONN_BASED_DSTADDRESS_OCCURENCES", "CONN_BASED_DSTADDRESS_DISTINCT_DSTPORTS", "CONN_BASED_DSTADDRESS_DISTINCT_SRCADDRESS", "CONN_BASED_DSTADDRESS_DISTINCT_SRCPORTS", "CONN_BASED_DSTADDRESS_AVGPACKETIN", "CONN_BASED_DSTADDRESS_AVGBYTEIN",
"TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES", "TIME_BASED_SRCADDRESS_OCCURENCES", "TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS", "TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS", "TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS", "TIME_BASED_SRCADDRESS_AVGPACKETIN", "TIME_BASED_SRCADDRESS_AVGBYTEIN",
"TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES", "TIME_BASED_DSTADDRESS_OCCURENCES", "TIME_BASED_DSTADDRESS_DISTINCT_SRCADDRESS", "TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS", "TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS", "TIME_BASED_DSTADDRESS_AVGPACKETIN", "TIME_BASED_DSTADDRESS_AVGBYTEIN",
]

featureDf = pd.DataFrame(columns = featureHeaders)
df = pd.read_csv(dataframeReference)
df = pd.concat([df, featureDf], axis=1, join_axes=[df.index])
df.to_csv(dataframeReference, encoding='utf-8', index=False)
size = len(df.index)

#after adding in the new columns create the connection based and time based features
CreateConnectionBasedFeatures(dataframeReference, size)
CreateTimeBasedFeatures(dataframeReference, size)