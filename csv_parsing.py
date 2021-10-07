'''
Created on 08.05.2021

@author: Alexander Fuchs

This module implements the loading of all .csv files.
So the 4 tables of the 2 datasets are loaded in this module. Also
the joined and grouped data is loaded in this module. The sorting
of the lists is also done in this module after loading. Some
data preparation is also done in this module.
'''

import csv
import data_types
import datetime

#standard list for analyzing

qualityControlPersonShifts = []
qualityControlDatas = []

machineErrorPersonShifts = []
machineErrorDatas = []

#joinOfProductionMetricsAndWorkingTimesOfWorkers

groupToMachineError = []
groupToQualityControl = []

#performance metrics for groups

averageMachineErrorTime = []
percentageOfMachineErrorInTotalTime = []

percentageOfErrorCodeAndErrorGroup = []
percentageOfErrorGroup = []
percentageOfErrorGroupInErrorCode = []

def loadMachineErrorData(personFilename, machineErrorFilename):
    '''
    Loads all the data from the tables of the machineErrorDataset to the lists. 'Processed' means how much entries
    were saved in the table. 'Saved' means how much entries are saved to the lists. the difference
    was data that is wrong.
    '''
    global machineErrorDatas, machineErrorPersonShifts
    with open(personFilename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        lineCount = 0
        for row in csv_reader:
            if lineCount != 0:
                personShift = data_types.PersonShift.createPersonShift(row[4], row[5], row[3], row[1], row[2])
                if personShift is not None:
                    machineErrorPersonShifts.append(personShift)
            lineCount += 1
    print(f'MachineError Persons: Processed {lineCount}. Saved {len(machineErrorPersonShifts)} at {datetime.datetime.now()}.')
            
    with open(machineErrorFilename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        lineCount = 0
        for row in csv_reader:
            if lineCount != 0:
                machineData = data_types.MachineErrorData.createMachineErrorData(row[4], row[3], row[1], row[2])
                if machineData is not None:
                    machineErrorDatas.append(machineData)
            lineCount += 1
    print('--------------------------------------------------------------------------')
    print(f'MachineError Data: Processed {lineCount}. Saved {len(machineErrorDatas)} at {datetime.datetime.now()}.')

        
def loadQualityControlData(personFilename, qualityControlFilename):
    '''
    Loads all the data from the tables of the qualityControlDataSet to the lists. 'Processed' means how much entries
    were saved in the table. 'Saved' means how much entries are saved to the lists. the difference
    was data that is wrong.
    '''
    global qualityControlDatas, qualityControlPersonShifts
    with open(personFilename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        lineCount = 0
        for row in csv_reader:
            if lineCount != 0:
                personShift = data_types.PersonShift.createPersonShift(row[4], row[5], row[3], row[1], row[2])
                if (personShift is not None):
                    qualityControlPersonShifts.append(personShift)
            lineCount += 1
    print('--------------------------------------------------------------------------')
    print(f'QualityControl Persons: Processed {lineCount}. Saved {len(qualityControlPersonShifts)} at {datetime.datetime.now()}.')
            
    with open(qualityControlFilename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        lineCount = 0
        for row in csv_reader:
            if lineCount != 0:
                qualityData = data_types.QualityControlData.createQalityControleData(row[1], row[2], row[3], row[4], row[5])
                if (qualityData is not None):
                    qualityControlDatas.append(qualityData)
            lineCount += 1
    print('--------------------------------------------------------------------------')
    print(f'QualityControl Data: Processed {lineCount}. Saved {len(qualityControlDatas)} at {datetime.datetime.now()}.')
    
            
def loadAllData(machineErrorPersons, machineErrors, qualityControlPersons, qualityControls):
    '''
    Loads all tables from the machineErrorDataset and the qualityControlDataset. Also all
    lists are sorted by the date of the entries (DESC). If there are two dates per entry
    the list ist sorted by the end of the intervall (DESC).
    '''
    global machineErrorDatas, machineErrorPersonShifts, qualityControlDatas, qualityControlPersonShifts
    
    loadMachineErrorData(machineErrorPersons, machineErrors) 
    loadQualityControlData(qualityControlPersons, qualityControls)
    
    #sort the two lists by __to attribute. This is needed in the next step
    machineErrorDatas.sort(key=lambda x: x.getTo(), reverse=True)
    machineErrorPersonShifts.sort(key=lambda x: x.getTo(), reverse=True)
    print('--------------------------------------------------------------------------')
    print(f'Sorted machineError at {datetime.datetime.now()}.')
    
    #sort the two lists by __to and __datum attribute. This is needed in the next step
    qualityControlDatas.sort(key=lambda x: x.getDatum(), reverse=True)
    qualityControlPersonShifts.sort(key=lambda x: x.getTo(), reverse=True)
    print('--------------------------------------------------------------------------')
    print(f'Sorted QualityControl at {datetime.datetime.now()}.')
    
    
def loadGroupData(pathOfAnalysis):
    '''
    Try to load the files of the joined Data to the lists. If there are no files or the parsing
    is not successful, the loading is stopped and the calculation is started.
    Calculation lasts many hours!!! Therefore the calculation is only done once and then
    saved so the joined data can be loaded and has not to be calculated again.
    The lists are also sorted by different criteria.
    '''
    global groupToMachineError, groupToQualityControl
    try:
        with open(pathOfAnalysis + 'groupToMachineError.csv') as groupToMachineErrorFile:
            csv_reader = csv.reader(groupToMachineErrorFile, delimiter = ',')
            for row in csv_reader:
                stringList = row[0].strip('][').split(', ')
                groupToMachineError.append(([int(i) for i in stringList], int(row[1])))
        print('--------------------------------------------------------------------------')
        print(f'Join and Grouping of Machine Error Data has been loaded at {datetime.datetime.now()}.')
        
        with open(pathOfAnalysis + 'groupToQualityControl.csv') as groupToQualityControlFile:
            csv_reader = csv.reader(groupToQualityControlFile, delimiter = ',')
            for row in csv_reader:
                stringList = row[0].strip('][').split(', ')
                groupToQualityControl.append(([int(i) for i in stringList], int(row[1])))
        print('--------------------------------------------------------------------------')
        print(f'Join and Grouping of Quality Control Data has been loaded at {datetime.datetime.now()}.')
        
        #sort the two lists by different criteria. This is needed in the next step
        groupToMachineError.sort(key=lambda x: (x[0], machineErrorDatas[x[1]].getErrorCode()))
        print('--------------------------------------------------------------------------')
        print(f'Sorted groupToMachineError at {datetime.datetime.now()}.')
        
        groupToQualityControl.sort(key=lambda x: (x[0], qualityControlDatas[x[1]].getProductType(), \
        qualityControlDatas[x[1]].getErrorCode(), qualityControlDatas[x[1]].getErrorGroup()))
        print('--------------------------------------------------------------------------')
        print(f'Sorted groupToQualityControl at {datetime.datetime.now()}.')
            
        return True
    
    except IOError:
        print('--------------------------------------------------------------------------')
        print('Loading of Join and Grouping not successful, has to be calculated!')
        groupToMachineError = []
        groupToQualityControl = []
        
        return False
    