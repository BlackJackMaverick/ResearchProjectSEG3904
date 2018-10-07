#data preprocessing of data set. dataset is passed as a reference to optimize performance.
import csv
import os
import pandas as pd

#TODO: make sure that no empty rows are added to the destination CSV file. 
#copy all the attack types labelled as dos to another CSV file. 
def CopyAttackRows(SourceDataset, DestDataset):
    dosAttackTypes = ["Back", "land", "neptune","pod", "smurf", "teardrop"]
    numberOfAttacks = 0
    numberOfNormal = 0

    #csv reader for the source dataset, and writer for the destination. Rows will be copied
    SourceReader = csv.reader(open(SourceDataset))
    DestWriter = csv.writer(open(DestDataset, 'a'), delimiter=',')
    for row in SourceReader:
        if row[41] in dosAttackTypes:
            DestWriter.writerow(row)
            numberOfAttacks = numberOfAttacks + 1
        if row[41] == 'normal':  
            numberOfNormal = numberOfNormal + 1
    
    #summary for the total number of attack records and normal records.
    print ("In total there are:" + SourceReader.count + "records. Of these records there are:" + numberOfAttacks + "number of DOS attack records. Finally there are " + numberOfNormal + "number of  normal records.")

#prints out all the rows to the terminal
def PrintAll(DatasetRef):
    datasetReader = csv.reader(open(DatasetRef))
    for row in datasetReader:
        print (row)    

#Print all column types and names, how many records in total, how many are Dos and how many are normal. 
def PrintSummary(DatasetRef):

    pass

#the different data sets file names along with their location
folderPath ='NSLKDDDataset\\NSL_KDD-master\\CSVData\\DOSattacks'
DatasetPath ='NSLKDDDataset\\NSL_KDD-master\\CSVData'
Tests = ['\\SmallTrainingSet.csv', '\\20PercentTrainingSet.csv', '\\FullTrainingSet.csv']

#create a csv file to store only the DOS attacks from each of the datasets. Store all these in a folder.  
if not os.path.exists(folderPath):
    os.makedirs(folderPath)

for test in Tests:
    if not os.path.exists(folderPath+test):
        with open(folderPath+test, 'wb') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',')

#Add all the rows that contain DOS attacks to the appropriate file.
PrintAll((DatasetPath + Tests[0]))

#check if the CSV file is empty or not. If it is empty then fill it with the attack data. 
if pd.read_csv((DatasetPath + Tests[0])).empty:
    CopyAttackRows((DatasetPath + Tests[0]), (folderPath + Tests[0]))

PrintAll((folderPath + '\\' + Tests[0]))