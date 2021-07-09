'''
Created on 08.05.2021

@author: alexanderfuchs
'''

import csv
import data_types
import datetime

#standard list for analyzing

qualityControlPersonShifts = []
qualityControlDatas = []

machineErrorPersonShifts = []
machineErrorDatas = []

groupToMachineError = []
groupToQualityControl = []

#results of analysis

averageMachineErrorTime = []
percentageOfMachineErrorInTotalTime = []

percentageOfErrorCodeAndErrorGroup = []
percentageOfErrorGroup = []
percentageOfErrorGroupInErrorCode = []

def loadMachineErrorData(personFilename, machineErrorFilename):
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
    print(f'MachineError Persons: Processed {lineCount}. Added {len(machineErrorPersonShifts)} at {datetime.datetime.now()}.')
            
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
    print(f'MachineError Data: Processed {lineCount}. Added {len(machineErrorDatas)} at {datetime.datetime.now()}.')

        
def loadQualityControlData(personFilename, qualityControlFilename):
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
    print(f'QualityControl Persons: Processed {lineCount}. Added {len(qualityControlPersonShifts)} at {datetime.datetime.now()}.')
            
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
    print(f'QualityControl Data: Processed {lineCount}. Added {len(qualityControlDatas)} at {datetime.datetime.now()}.')
    
            
def loadAllData(machineErrorPersons, machineErrors, qualityControlPersons, qualityControls):
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
        
        #sort the two lists by groups. This is needed in the next step
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
    