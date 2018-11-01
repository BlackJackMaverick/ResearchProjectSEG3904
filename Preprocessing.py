#data preprocessing of data set. dataset is passed as a reference to optimize performance.
import csv
import os
import pandas as pd
import numpy as np

#copy all the attack types labelled as dos to another CSV file. 
def CopyAttackRows(SourceDataset, DestDataset):
    dosAttackTypes = ["Back", "land", "neptune","pod", "smurf", "teardrop"] #dos attack labels

    #csv reader for the source dataset, and writer for the destination. 
    # Rows will be copied to the destination if they are attack rows.
    with open(SourceDataset, 'r') as Source:
        with open(DestDataset, 'w', newline='') as Dest:
            SourceReader = csv.reader(Source)
            DestWriter = csv.writer(Dest)
            for row in SourceReader:
                if row[41] in dosAttackTypes:
                    DestWriter.writerow(row) 
 
#prints out all the rows to the terminal. 
def PrintAll(DatasetRef):
    datasetReader = csv.reader(open(DatasetRef))
    for row in datasetReader:
        print (row)    

#prints number of total records, how many are  labelled as dos, and how many are normal. 
def PrintSummary(datasetRef):
    #counters for records
    normalCount = 0
    attackCount = 0
    recordCount = 0
    dosAttackTypes = ["Back", "land", "neptune","pod", "smurf", "teardrop"]
    with open(datasetRef, 'r') as csvFile:
        #iterate through each row in the csv file.
        datasetReader = csv.reader(csvFile)
        for row in datasetReader: 
            if row[41] in dosAttackTypes:
                attackCount += 1
            if row[41] == "normal":
                normalCount += 1
            recordCount += 1
    print ("There are a total of " + str(recordCount) + " records of traffic")
    print ("There are " + str(attackCount) + " attack records")
    print ("There are " + str(normalCount) + " normal records")
    
#the different data sets file names along with their location
folderPath ='NSLKDDDataset\\NSL_KDD-master\\CSVData\\DOSattacks'
DatasetPath ='NSLKDDDataset\\NSL_KDD-master\\CSVData'
Tests = ['\\SmallTrainingSet.csv', '\\20PercentTrainingSet.csv', '\\FullTrainingSet.csv']

#create a folder to store only the DOS attacks from each of the datasets.  
if not os.path.exists(folderPath):
    os.makedirs(folderPath)

#If the CSV file does not exist then create it. 
for test in Tests:
    if not os.path.exists(folderPath+test):
        open(folderPath+test, 'wb')
#If the CSV file is empty then populate it with the attack rows of the intended dataset.    
    try:
        pd.read_csv(folderPath+test)
    except:
        CopyAttackRows((DatasetPath+test), (folderPath + test))

    #print out the rows and a summary of the number of records. 
    PrintAll(folderPath+test)
    PrintSummary(DatasetPath+test)