import csv
import os
import pandas as pd
import numpy as np
import itertools as IT

#This helper method returns an array of data that will be used to populate the connection based features last row.
#WindowIndex in this case refers to the last row of the dataframe.
def CreateTimeBasedFeatures (dfref, dfSize, windowIndex, endTime):
    
    #size of the rolling window based on time
    #will be increased to 10 minutes in full dataset
    startTime = pd.Timestamp(endTime) - pd.Timestamp('00:01:00')
    
    #get the rows that are within the time frame [startTime, endTime] based on the 'ENDTIME' column
    chunkSize = 10 #can be increased in full data set
    chunkIndex = 1
    df = pd.DataFrame(columns = headers)
    while True:
        chunks = pd.read_csv(dfref, skiprows=range(1,chunkIndex), nrows=chunkSize)
        chunks = chunks.between_time(start_time = startTime, end_time = endTime, include_start=True, include_end=True)
        
        #if the first end time in the chunks is greater than the frame break out of loop
        if chunks["ENDTIME"].iloc[0] >= endTime:
            break
        #search for the rows where 'ENDTIME' lies within the time frame and append them to a temporary dataframe
        df.append(chunks.between_time)

        #increase the index to look through the next chunk
        chunkIndex = chunkIndex + chunkSize

    import pdb;pdb.set_trace()
    chunks = IT.takewhile(lambda chunk: startTime <= chunk["ENDTIME"].iloc[-1] <= endTime, chunks)
    df = pd.concat(chunks)
    mask = startTime <= df["ENDTIME"] <= endTime
    df = df.loc[mask]

    #get the rows that are equal to the last source address in the time based dataframe.
    targetSourceAddress = df["SRCADDRESS"].iloc[-1]
    index = df.index[df["SRCADDRESS"] == targetSourceAddress]
    srcdf = df.loc[index]

    #do the same for the last destination address
    targetDestinationAddress = df["DSTADDRESS"].iloc[-1]
    index = df.index[df["DSTADDRESS"] == targetDestinationAddress]
    dstdf = df.loc[index]

    #populate the last row of dataframe with the engineered features
    timeBasedValues = {
    "TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES": len(df.index),
    "TIME_BASED_SRCADDRESS_OCCURENCES":len(srcdf.index),
    "TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS":srcdf["DSTADDRESS"].nunique(),
    "TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS":srcdf["DSTPORT"].nunique(),
    "TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS":srcdf["SRCPORT"].nunique(),
    "TIME_BASED_SRCADDRESS_AVGPACKETIN":srcdf["PACKETINCOUNT"].mean(),
    "TIME_BASED_SRCADDRESS_AVGBYTEIN":srcdf["BYTEINCOUNT"].mean(),

    #repeat for the destination address
    "TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES":len(df.index),
    "TIME_BASED_DSTADDRESS_OCCURENCES":len(dstdf.index),
    "TIME_BASED_DSTADDRESS_DISTINCT_SRCADDRESS":dstdf["SRCADDRESS"].nunique(),
    "TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS":dstdf["DSTPORT"].nunique(),
    "TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS":dstdf["SRCPORT"].nunique(),
    "TIME_BASED_DSTADDRESS_AVGPACKETIN":dstdf["PACKETINCOUNT"].mean(),
    "TIME_BASED_DSTADDRESS_AVGBYTEIN":dstdf["BYTEINCOUNT"].mean()
    }

    # df["TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES":len(df.index)
    # df["TIME_BASED_SRCADDRESS_OCCURENCES":len(srcdf.index)
    # df["TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS":srcdf["DSTADDRESS"].nunique()
    # df["TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS":srcdf["DSTPORT"].nunique()
    # df["TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS":srcdf["SRCPORT"].nunique()
    # df["TIME_BASED_SRCADDRESS_AVGPACKETIN":srcdf["PACKETINCOUNT"].mean()
    # df["TIME_BASED_SRCADDRESS_AVGBYTEIN":srcdf["BYTEINCOUNT"].mean()

    # #repeat for the destination address
    # df["TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES":len(df.index)
    # df["TIME_BASED_DSTADDRESS_OCCURENCES":len(dstdf.index)
    # df["TIME_BASED_DSTADDRESS_DISTINCT_SRCADDRESS":dstdf["SRCADDRESS"].nunique()
    # df["TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS":dstdf["DSTPORT"].nunique()
    # df["TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS":dstdf["SRCPORT"].nunique()
    # df["TIME_BASED_DSTADDRESS_AVGPACKETIN":dstdf["PACKETINCOUNT"].mean()
    # df["TIME_BASED_DSTADDRESS_AVGBYTEIN":dstdf["BYTEINCOUNT"].mean()

    #return the last row of the dataframe  that will contain all the information about rolling window
    return timeBasedValues

#Function used to create connection based features based off a rolling window of size 1000.
def CreateConnectionBasedFeatures (dfref, dfSize, targetDfRef, headers):

    #rows 0-windowSize will remain blank for the connection based features.
    #WindowSize will be increased to 1000 in full datatset.
    windowSize = 10 
    windowIndex = 0
    
    while windowIndex < dfSize:

        #fetch a subset of the dataframe of length windowsize
        df = pd.read_csv(dfref, skiprows=range(1,windowIndex), nrows=windowSize)
        #import pdb;pdb.set_trace() #REMOVE ONCE DONE
        saveRow = pd.DataFrame(df, columns = headers).iloc[-1] #TODO: why doesnt this get saved properly?

        #save the source and destination IP address of the last row of the rolling window
        targetSourceAddress = df["SRCADDRESS"].iloc[-1]
        targetDestinationAddress = df["DSTADDRESS"].iloc[-1]

        #The rolling window is of size windowSize and looks at the previous rows from the index that match the source address of the target.
        #get the rows that are equal to the last source address
        #repeat for destination address
        index = df.index[df["SRCADDRESS"] == targetSourceAddress]
        srcdf = df.loc[index]

        index = df.index[df["DSTADDRESS"] == targetDestinationAddress]
        dstdf = df.loc[index]

        #from the rolling window fill out the engineered features for the source address
        saveRow["CONN_BASED_SRCADDRESS_OCCURENCES"] = len(srcdf.index)
        saveRow["CONN_BASED_SRCADDRESS_OCCURENCES"] = len(srcdf.index)
        saveRow["CONN_BASED_SRCADDRESS_DISTINCT_DSTPORTS"]= srcdf["DSTPORT"].nunique()
        saveRow["CONN_BASED_SRCADDRESS_DISTINCT_DSTADDRESS"] = srcdf["DSTADDRESS"].nunique()
        saveRow["CONN_BASED_SRCADDRESS_DISTINCT_SRCPORTS"] = srcdf["SRCPORT"].nunique()
        saveRow["CONN_BASED_SRCADDRESS_AVGPACKETIN"] = srcdf["PACKETINCOUNT"].mean()
        saveRow["CONN_BASED_SRCADDRESS_AVGBYTEIN"] = srcdf["BYTEINCOUNT"].mean()

        #repeat the features for the destination address
        saveRow["CONN_BASED_DSTADDRESS_OCCURENCES"] = len(dstdf.index)
        saveRow["CONN_BASED_DSTADDRESS_DISTINCT_DSTPORTS"] = dstdf["DSTPORT"].nunique()
        saveRow["CONN_BASED_DSTADDRESS_DISTINCT_SRCADDRESS"] = dstdf["DSTADDRESS"].nunique()
        saveRow["CONN_BASED_DSTADDRESS_DISTINCT_SRCPORTS"] = dstdf["SRCPORT"].nunique()
        saveRow["CONN_BASED_DSTADDRESS_AVGPACKETIN"] = dstdf["PACKETINCOUNT"].mean()
        saveRow["CONN_BASED_DSTADDRESS_AVGBYTEIN"] = dstdf["BYTEINCOUNT"].mean()

        #use helper method to get the timebased features.
        #The time based features will begin at the last index of the dataframe for the connection based features.
        #the helper method will return a dictionary, 
        #so use the values from the dictionary to populate the time based features in the last row of the dataframe
        timeBasedRow = CreateTimeBasedFeatures(dfref, dfSize, windowIndex + windowSize, saveRow["ENDTIME"])
        saveRow["TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES"] = timeBasedRow["TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES"]
        saveRow["TIME_BASED_SRCADDRESS_OCCURENCES"] = timeBasedRow["TIME_BASED_SRCADDRESS_OCCURENCES"]
        saveRow["TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS"] = timeBasedRow["TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS"]
        saveRow["TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS"] = timeBasedRow["TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS"]
        saveRow["TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS"] = timeBasedRow["TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS"]
        saveRow["TIME_BASED_SRCADDRESS_AVGPACKETIN"] = timeBasedRow["TIME_BASED_SRCADDRESS_AVGPACKETIN"]
        saveRow["TIME_BASED_SRCADDRESS_AVGBYTEIN"] = timeBasedRow["TIME_BASED_SRCADDRESS_AVGBYTEIN"]

        saveRow["TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES"] = timeBasedRow["TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES"]
        saveRow["TIME_BASED_DSTADDRESS_OCCURENCES"] = timeBasedRow["TIME_BASED_DSTADDRESS_OCCURENCES"]
        saveRow["TIME_BASED_DSTADDRESS_DISTINCT_DSTADDRESS"] = timeBasedRow["TIME_BASED_DSTADDRESS_DISTINCT_DSTADDRESS"]
        saveRow["TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS"] = timeBasedRow["TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS"]
        saveRow["TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS"] = timeBasedRow["TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS"]
        saveRow["TIME_BASED_DSTADDRESS_AVGPACKETIN"] = timeBasedRow["TIME_BASED_DSTADDRESS_AVGPACKETIN"]
        saveRow["TIME_BASED_DSTADDRESS_AVGBYTEIN"] = timeBasedRow["TIME_BASED_DSTADDRESS_AVGBYTEIN"]

        print(saveRow)
        # Save the last row of the engineered features to a separate CSV file.
        # Increase index to next row. 
        windowIndex = windowIndex + 1
        df.iloc[-1].to_csv(targetDfRef, mode = 'a', header = false)

dataframeReference = "Dataset\\testing\\TrainModel.csv"
featureGeneratedDfRef = "Dataset\\testing\\FeatureGenerator.csv"

#get the headers of the dataframe and features then combine them.
columnNames = pd.read_csv(dataframeReference, nrows = 1)
columnNames = columnNames.columns.values
featureHeaders = [
"CONN_BASED_SRCADDRESS_OCCURENCES", "CONN_BASED_SRCADDRESS_DISTINCT_DSTPORTS", "CONN_BASED_SRCADDRESS_DISTINCT_DSTADDRESS", "CONN_BASED_SRCADDRESS_DISTINCT_SRCPORTS", "CONN_BASED_SRCADDRESS_AVGPACKETIN", "CONN_BASED_SRCADDRESS_AVGBYTEIN",
"CONN_BASED_DSTADDRESS_OCCURENCES", "CONN_BASED_DSTADDRESS_DISTINCT_DSTPORTS", "CONN_BASED_DSTADDRESS_DISTINCT_SRCADDRESS", "CONN_BASED_DSTADDRESS_DISTINCT_SRCPORTS", "CONN_BASED_DSTADDRESS_AVGPACKETIN", "CONN_BASED_DSTADDRESS_AVGBYTEIN",
"TIME_BASED_SRCADDRESS_TOTAL_OCCURENCES", "TIME_BASED_SRCADDRESS_OCCURENCES", "TIME_BASED_SRCADDRESS_DISTINCT_DSTADDRESS", "TIME_BASED_SRCADDRESS_DISTINCT_DSTPORTS", "TIME_BASED_SRCADDRESS_DISTINCT_SRCPORTS", "TIME_BASED_SRCADDRESS_AVGPACKETIN", "TIME_BASED_SRCADDRESS_AVGBYTEIN",
"TIME_BASED_DSTADDRESS_TOTAL_OCCURENCES", "TIME_BASED_DSTADDRESS_OCCURENCES", "TIME_BASED_DSTADDRESS_DISTINCT_SRCADDRESS", "TIME_BASED_DSTADDRESS_DISTINCT_DSTPORTS", "TIME_BASED_DSTADDRESS_DISTINCT_SRCPORTS", "TIME_BASED_DSTADDRESS_AVGPACKETIN", "TIME_BASED_DSTADDRESS_AVGBYTEIN",
]
headers = np.concatenate ((columnNames, featureHeaders), axis =0)

#create new csv file to store the feature generated values and save the headers as the first row.
featureDf = pd.DataFrame(columns = headers)
featureDf.to_csv(featureGeneratedDfRef, encoding='utf-8', index=False)

#get number of rows from the original reference dataframe
with open(dataframeReference) as f:
    size = sum(1 for line in f)

#create the connection based and time based features.
CreateConnectionBasedFeatures(dataframeReference, size, featureGeneratedDfRef, headers)