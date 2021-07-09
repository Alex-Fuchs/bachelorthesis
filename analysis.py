'''
Created on 12.05.2021

@author: alexanderfuchs
'''

import numpy
import collections

import csv
import datetime
import csv_parsing as cp
    
def joinAndGroupMachineError(pathOfAnalysis):
    machineErrorToPerson = []
    
    print('--------------------------------------------------------------------------')
    print(f'machineErrorToPerson: Join started at {datetime.datetime.now()}!')
    
    '''
    The machineErrorDatas and machineErrorPersonShifts must be sorted by the date of getTo() (DESC).
    Pairs every machine errors to every person who worked at that time the error ocurred.
    The algorithm below is modified due to performance reasons but therefore the lists
    have to be sorted.
    
    After the algorithm, machineErrorToPerson is lexicographically sorted.
    This is needed in the next step.
    '''        
    for u in range(len(cp.machineErrorDatas)):
        for i in range(len(cp.machineErrorPersonShifts)):
            if cp.machineErrorPersonShifts[i].getTo() >= cp.machineErrorDatas[u].getTo():
                if cp.machineErrorPersonShifts[i].getOf() <= cp.machineErrorDatas[u].getOf():
                    machineErrorToPerson.append((u, i))
            else:
                break
            
    print('--------------------------------------------------------------------------')
    print(f'machineErrorToPerson: Join done at {datetime.datetime.now()}.')
      
    print('--------------------------------------------------------------------------')
    print(f'groupToMachineError: Grouping started at {datetime.datetime.now()}.')
    
    '''
    machineErrorToPerson has to be sorted by the first element otherwise the following algorithm
    does not work. The algorithm maps all the persons of one machine error to this error as a
    group.
    '''
    with open(pathOfAnalysis + 'groupToMachineError.csv', mode='w') as groupToMachineErrorFile:
        writer = csv.writer(groupToMachineErrorFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        groupList = [cp.machineErrorPersonShifts[machineErrorToPerson[0][1]]]
        for i in range(1, len(machineErrorToPerson)):
            if machineErrorToPerson[i-1][0] == machineErrorToPerson[i][0]:
                groupList.append(cp.machineErrorPersonShifts[machineErrorToPerson[i][1]])
            else:
                mostFrequentItem = collections.Counter([x.getGroup() for x in groupList]).most_common(1)[0]
                
                if mostFrequentItem[1] > len(groupList) / 2: 
                    tmp = []  
                    for u in range(len(groupList)):
                        if groupList[u].getGroup() == mostFrequentItem[0]:
                            tmp.append(groupList[u].getHash()) 
                        
                    writer.writerow([tmp, machineErrorToPerson[i-1][0]])
                groupList = [cp.machineErrorPersonShifts[machineErrorToPerson[i][1]]] 
        
        if len(groupList) > 0:
            mostFrequentItem = collections.Counter([x.getGroup() for x in groupList]).most_common(1)[0]
            if mostFrequentItem[1] > len(groupList) / 2:
                    
                result = []
                for u in range(len(groupList)):
                    if groupList[u].getGroup() == mostFrequentItem[0]:
                        result.append(groupList[u].getHash()) 
                    
                writer.writerow([result, machineErrorToPerson[len(machineErrorToPerson) - 1][0]])
             
    print('--------------------------------------------------------------------------')
    print(f'groupToMachineError: Grouping done at {datetime.datetime.now()}.')
       
    
def joinAndGroupQualityControl(pathOfAnalysis):
    qualityControlToPerson = []
    
    print('--------------------------------------------------------------------------')
    print(f'qualityControlToPerson: Join started at {datetime.datetime.now()}.')
    
    '''
    The qualityControlDatas and qualityControlPersonShifts must be sorted by the date of getTo()
    and date (DESC).
    Pairs every quality Control to every person who worked at that time.
    The algorithm below is modified due to performance reasons but therefore the lists
    have to be sorted.
    
    After the algorithm, qualityControlToPerson is lexicographically sorted.
    This is needed in the next step.
    '''        
    for u in range(len(cp.qualityControlDatas)):
        for i in range(len(cp.qualityControlPersonShifts)):
            if cp.qualityControlPersonShifts[i].getTo() >= cp.qualityControlDatas[u].getDatum():
                if cp.qualityControlPersonShifts[i].getOf() <= cp.qualityControlDatas[u].getDatum():
                    qualityControlToPerson.append((u, i))
            else:
                break
            
    print('--------------------------------------------------------------------------')
    print(f'qualityControlToPerson: Join done at {datetime.datetime.now()}.')
                
    print('--------------------------------------------------------------------------')
    print(f'groupToQualityControl: Grouping started at {datetime.datetime.now()}.')   
    
    '''
    qualityControlToPerson has to be sorted by the first element otherwise the following algorithm
    does not work. The algorithm maps all the persons of one quality control.
    '''
    with open(pathOfAnalysis + 'groupToQualityControl.csv', mode='w') as groupToQualityControlFile:
        writer = csv.writer(groupToQualityControlFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        groupList = [cp.qualityControlPersonShifts[qualityControlToPerson[0][1]]]
        for i in range(1, len(qualityControlToPerson)):
            if qualityControlToPerson[i-1][0] == qualityControlToPerson[i][0]:
                groupList.append(cp.qualityControlPersonShifts[qualityControlToPerson[i][1]])
            else:
                mostFrequentItem = collections.Counter([x.getGroup() for x in groupList]).most_common(1)[0]
                
                if mostFrequentItem[1] > len(groupList) / 2:
                    tmp = []
                    for u in range(len(groupList)):
                        if groupList[u].getGroup() == mostFrequentItem[0]:
                            tmp.append(groupList[u].getHash()) 
                        
                    writer.writerow([tmp, qualityControlToPerson[i-1][0]])
                groupList = [cp.qualityControlPersonShifts[qualityControlToPerson[i][1]]]
                
        if len(groupList) > 0:
            mostFrequentItem = collections.Counter([x.getGroup() for x in groupList]).most_common(1)[0]
            if mostFrequentItem[1] > len(groupList) / 2:
                    
                result = []
                for u in range(len(groupList)):
                    if groupList[u].getGroup() == mostFrequentItem[0]:
                        result.append(groupList[u].getHash()) 
                        
                writer.writerow([result, qualityControlToPerson[len(qualityControlToPerson) - 1][0]])
    
    print('--------------------------------------------------------------------------')
    print(f'groupToQualityControl: Grouping done at {datetime.datetime.now()}.')
    
    
def joinAndGroup(pathOfAnalysis):
    '''
    Joins and groups the machine Error and quality Control Data. Because it needs so much time
    it first trys to load. If it cannot be loaded it is calculated.
    '''
    if not cp.loadGroupData(pathOfAnalysis):
        joinAndGroupMachineError(pathOfAnalysis)
        joinAndGroupQualityControl(pathOfAnalysis)
        if not cp.loadGroupData(pathOfAnalysis):
            raise RuntimeError('Illegal State: Bug in loading or joinAndGroup!!!')
     

def divideGroupToMachineError(earlyTwelve, nightTwelve, early, late, night):
    '''
    Divides the groupToMachineErrors into the 5 shifts. This is necessary because it´s not
    possible to compare the shifts. 
    '''
    for i in range(len(cp.groupToMachineError)):
        of = cp.machineErrorDatas[cp.groupToMachineError[i][1]].getOf()
        to = cp.machineErrorDatas[cp.groupToMachineError[i][1]].getTo()
        
        if of.weekday() == 0:
            if 6 <= of.hour <= 17 and 6 <= to.hour <= 17:
                earlyTwelve.append(cp.groupToMachineError[i])
            elif 18 <= of.hour <= 23 and (0 <= to.hour <= 5 or 18 <= to.hour <= 23):
                nightTwelve.append(cp.groupToMachineError[i])
            elif 0 <= of.hour <= 5 and 0 <= to.hour <= 5:
                night.append(cp.groupToMachineError[i])
        else:
            if of.weekday() == 1 and 0 <= of.hour <= 5 and 0 <= to.hour <= 5:
                nightTwelve.append(cp.groupToMachineError[i])
            elif 14 <= of.hour <= 21 and 14 <= to.hour <= 21:
                late.append(cp.groupToMachineError[i])
            elif 6 <= of.hour <= 13 and 6 <= to.hour <= 13:
                early.append(cp.groupToMachineError[i])
            elif 22 <= of.hour <= 23 and (0 <= to.hour <= 5 or 22 <= to.hour <= 23):
                night.append(cp.groupToMachineError[i])     

    
def analyzeMachineError(groupToMachineErrorOfShift, shift):    
    '''
    groupToMachineError has to be sorted by group first and errorCode second. Therefore for every
    group and errorCode the average Time of the machineErrors with that errorCode and that group
    are calculated. Also the percentage of a errorCode in the total worktime of a group is
    calculated.
    
    The results are saved in cp.averageMachineErrorTime and cp.percentageOfMachineErrorInTotalTime.
    '''
    
    groupToTime = []
    calculateTimeOfAllGroups(groupToTime, groupToMachineErrorOfShift)
    
    timeOfGroupSaved = groupToTime[0][1]
    elementToList = cp.machineErrorDatas[groupToMachineErrorOfShift[0][1]]
    timedeltas = [elementToList.getTo() - elementToList.getOf()]
    for i in range(1, len(groupToMachineErrorOfShift)):
        lastTuple = groupToMachineErrorOfShift[i-1]
        thisTuple = groupToMachineErrorOfShift[i]
        
        if lastTuple[0] == thisTuple[0] and cp.machineErrorDatas[lastTuple[1]].getErrorCode()\
        == cp.machineErrorDatas[thisTuple[1]].getErrorCode():
        
            elementToList = cp.machineErrorDatas[thisTuple[1]]
            timedeltas.append(elementToList.getTo() - elementToList.getOf())
        else:   
            if lastTuple[0] != thisTuple[0]:
                for i in range(len(groupToTime)):
                    if groupToTime[i][0] == thisTuple[0]:
                        timeOfGroupSaved = groupToTime[i][1]
                     
            avgTime = sum(timedeltas, datetime.timedelta(0)) / len(timedeltas)
            if timeOfGroupSaved != datetime.timedelta(0):
                percentage = sum(timedeltas, datetime.timedelta(0)) / timeOfGroupSaved
            else:
                percentage = - 1.5
            
            cp.averageMachineErrorTime.append((lastTuple[0], shift, \
            cp.machineErrorDatas[lastTuple[1]].getErrorCode(), avgTime))
            
            cp.percentageOfMachineErrorInTotalTime.append((lastTuple[0],shift, \
            cp.machineErrorDatas[lastTuple[1]].getErrorCode(), percentage))
            
            elementToList = cp.machineErrorDatas[thisTuple[1]]
            timedeltas = [elementToList.getTo() - elementToList.getOf()]
            
    if len(timedeltas) > 0:
        lastTuple = groupToMachineErrorOfShift[len(groupToMachineErrorOfShift) - 1]
        avgTime = sum(timedeltas, datetime.timedelta(0)) / len(timedeltas)
        percentage = sum(timedeltas, datetime.timedelta(0)) \
        / groupToTime[len(groupToTime) - 1][1]
        
        cp.averageMachineErrorTime.append((lastTuple[0], shift, \
        cp.machineErrorDatas[lastTuple[1]].getErrorCode(), avgTime))
        
        cp.percentageOfMachineErrorInTotalTime.append((lastTuple[0], shift, \
        cp.machineErrorDatas[lastTuple[1]].getErrorCode(), percentage))
                    
                    
def calculateTimeOfAllGroups(groupToTime, groupToMachineErrorOfShift):
    '''
    Calculates the time of all groups in the right way. timeOfGroup does not bring the right total
    time.
    '''
    group = groupToMachineErrorOfShift[0][0]
    for i in range(1, len(groupToMachineErrorOfShift)):
        if group != groupToMachineErrorOfShift[i][0]:
            groupToTime.append((group, timeOfGroupSnd(group)))
            group = groupToMachineErrorOfShift[i][0]
            
    groupToTime.append((group, timeOfGroupSnd(group)))
      
    for i in range(len(groupToTime)):
        for u in range(len(groupToTime)):
            if i != u and set(groupToTime[i][0]).issubset(set(groupToTime[u][0])):
                groupToTime[i] = (groupToTime[i][0], groupToTime[i][1] - groupToTime[u][1])
      
    sumOfDeltas = datetime.timedelta(0)
    for i in range(len(groupToTime)):
        sumOfDeltas += groupToTime[i][1]
                
    realSum = datetime.datetime(2020, 4, 29, 5, 47, 0) - datetime.datetime(2019, 2, 7, 16, 0, 0)
    
    print('--------------------------------------------------------------------------')
    print(f'MachineError Data: Accuracy: {sumOfDeltas / realSum} at {datetime.datetime.now()}.')
    
    
def timeOfGroupSnd(group):
    personShifts = []
    for i in range(len(group)):
        personShifts.append(getShiftsOfPerson(group[i]))
        
    maxIndex = 0
    for i in range(len(personShifts)):
        if len(personShifts[i]) > len(personShifts[maxIndex]):
            maxIndex = i
            
    sumOfDeltas = datetime.timedelta(0)   
    for i in range(len(personShifts[maxIndex])):  
        timeIntersection = personShifts[maxIndex][i] 
        allHit = True
        
        for y in range(len(personShifts)):
            if y != maxIndex:
                hit = False
                x = 0
                
                while x < len(personShifts[y]) and not hit:
                    toIntersect = personShifts[y][x]
                    
                    if timeIntersection[0] <= toIntersect[0] <= timeIntersection[1]:
                        timeIntersection = (toIntersect[0], timeIntersection[1])
                        hit = True
                    elif toIntersect[0] <= timeIntersection[0] <= toIntersect[1]:
                        timeIntersection = (timeIntersection[0], toIntersect[1])
                        hit = True
                    x += 1
                
                if not hit:
                    allHit = False
            
        if allHit:       
            sumOfDeltas += timeIntersection[1] - timeIntersection[0]
  
    return sumOfDeltas
        
        
def getShiftsOfPerson(person):
    '''
    Returns a list with all shifts of one person.
    '''
    shifts = []
    for i in range(len(cp.machineErrorPersonShifts)):
        if cp.machineErrorPersonShifts[i].getHash() == person:
            shifts.append((cp.machineErrorPersonShifts[i].getOf(), cp.machineErrorPersonShifts[i].getTo()))
    return shifts
                    
            
def timeOfGroup(group, groupToMachineError):
    '''
    This function calculates the time a group spent together working. This is necessary for
    calculating the percentage of every machineError to the whole working time.
    '''
    machineErrorsOfGroup = []
    for i in range(len(groupToMachineError)):
        if groupToMachineError[i][0] == group:
            machineErrorsOfGroup.append(cp.machineErrorDatas[groupToMachineError[i][1]])
    
    personShifts = []
    for i in range(len(group)):
        personShifts.append(getShiftsOfPersonAndOfMachineErrors(group[i], machineErrorsOfGroup))
        
    maxIndex = 0
    for i in range(len(personShifts)):
        if len(personShifts[i]) > len(personShifts[maxIndex]):
            maxIndex = i
      
      
    sumOfDeltas = datetime.timedelta(0)   
    for i in range(len(personShifts[maxIndex])):  
        timeIntersection = personShifts[maxIndex][i] 
        
        for y in range(len(personShifts)):
            if y != maxIndex:
                hit = False
                x = 0
                
                while x < len(personShifts[y]) and not hit:
                    toIntersect = personShifts[y][x]
                    
                    if timeIntersection[0] <= toIntersect[0] <= timeIntersection[1]:
                        timeIntersection = (toIntersect[0], timeIntersection[1])
                        hit = True
                    elif toIntersect[0] <= timeIntersection[0] <= toIntersect[1]:
                        timeIntersection = (timeIntersection[0], toIntersect[1])
                        hit = True
                    x += 1
                   
        sumOfDeltas += timeIntersection[1] - timeIntersection[0]
  
    return sumOfDeltas
                
                
def getShiftsOfPersonAndOfMachineErrors(person, machineErrors):
    '''
    Returns a list with all shifts of one person, in that at least one machine Error of 
    machineErrors occured.
    '''
    shifts = []
    for i in range(len(cp.machineErrorPersonShifts)):
        if cp.machineErrorPersonShifts[i].getHash() == person:
            shift = cp.machineErrorPersonShifts[i]
            
            for u in range(len(machineErrors)):
                if shift.getOf() <= machineErrors[u].getOf() \
                and machineErrors[u].getTo() <= shift.getTo():
                    shifts.append((shift.getOf(), shift.getTo()))
                    break
    return shifts

     
def divideGroupToQualityControl(earlyTwelve, nightTwelve, early, late, night):
    '''
    Divides the groupToQualityControls into the 5 shifts. This is necessary because it´s not
    possible to compare the shifts. 
    '''
    for i in range(len(cp.groupToQualityControl)):
        date = cp.qualityControlDatas[cp.groupToQualityControl[i][1]].getDatum()
        
        if date.weekday() == 0:
            if 6 <= date.hour <= 17:
                earlyTwelve.append(cp.groupToQualityControl[i])
            elif 18 <= date.hour <= 23:
                nightTwelve.append(cp.groupToQualityControl[i])
            else:
                night.append(cp.groupToQualityControl[i])
        else:
            if date.weekday() == 1 and 0 <= date.hour <= 5:
                nightTwelve.append(cp.groupToQualityControl[i])
            elif 14 <= date.hour <= 21:
                late.append(cp.groupToQualityControl[i])
            elif 6 <= date.hour <= 13:
                early.append(cp.groupToQualityControl[i])
            else:
                night.append(cp.groupToQualityControl[i])  
                

def analyzeErrorGroup(groupToQualityControlOfShift, shift):
    '''
    Calculates the % of errorGroups for all groups and productTypes.
    '''
    sortedByErrorGroup = sorted(groupToQualityControlOfShift, \
    key=lambda x: (x[0], cp.qualityControlDatas[x[1]].getProductType(), \
    cp.qualityControlDatas[x[1]].getErrorGroup(), cp.qualityControlDatas[x[1]].getErrorCode()))
    
    thisQualityControl = cp.qualityControlDatas[sortedByErrorGroup[0][1]]
    elementsOfProductType = [thisQualityControl]
    counterOfProductType = thisQualityControl.getNumber()
    for i in range(1, len(sortedByErrorGroup)):
        lastTuple = sortedByErrorGroup[i-1]
        thisTuple = sortedByErrorGroup[i]
        lastQualityControl = cp.qualityControlDatas[lastTuple[1]]
        thisQualityControl = cp.qualityControlDatas[thisTuple[1]]
        
        if lastTuple[0] == thisTuple[0] and lastQualityControl.getProductType() \
        == thisQualityControl.getProductType():
            elementsOfProductType.append(thisQualityControl)
            counterOfProductType += 0 if thisQualityControl.getNumber() < 0 else thisQualityControl.getNumber()
        else:
            groupQualityControlsPerErrorGroup(lastTuple[0], shift, \
            lastQualityControl.getProductType(), elementsOfProductType, counterOfProductType)
                
            elementsOfProductType = [thisQualityControl]
            counterOfProductType = 0 if thisQualityControl.getNumber() < 0 else thisQualityControl.getNumber()
            
    if len(elementsOfProductType) > 0:
        lastTuple = sortedByErrorGroup[len(sortedByErrorGroup) - 1]
        
        groupQualityControlsPerErrorGroup(lastTuple[0], shift, \
        cp.qualityControlDatas[lastTuple[1]].getProductType(), elementsOfProductType, counterOfProductType)
            
            
def groupQualityControlsPerErrorGroup(group, shift, productType, elementsOfProductType, counterOfProductType):
    '''
    Calculates the % of a every Error Group of one product Type of one group. The elements of the
    productType and the group must already been filtered. Also a counter for all elements of the
    productType (getNumber()) is given, so it is not necessary to calculate it here.
    '''
    sumOfErrorGroup = 0 if elementsOfProductType[0].getNumber() < 0 else elementsOfProductType[0].getNumber()
    
    for i in range(1, len(elementsOfProductType)):
        if elementsOfProductType[i-1].getErrorGroup() == elementsOfProductType[i].getErrorGroup():
            sumOfErrorGroup += 0 if elementsOfProductType[i].getNumber() < 0 else elementsOfProductType[i].getNumber()
        else:
            if sumOfErrorGroup != 0 and counterOfProductType != 0:
                cp.percentageOfErrorGroup.append((group, shift, productType, \
                elementsOfProductType[i-1].getErrorGroup(), sumOfErrorGroup / counterOfProductType))
            
            sumOfErrorGroup = 0 if elementsOfProductType[i].getNumber() < 0 else elementsOfProductType[i].getNumber()
            
    if sumOfErrorGroup != 0 and counterOfProductType != 0:
        cp.percentageOfErrorGroup.append((group, shift, productType, \
        elementsOfProductType[len(elementsOfProductType) - 1].getErrorGroup(), sumOfErrorGroup / counterOfProductType))

    
def analyzeErrorCodes(groupToQualityControlOfShift, shift):    
    '''
    groupToQualityControl has to be sorted by group first, productType second, errorCode third and
    errorGroup fourth. Therefore for every group and productType, the % of every errorCode and
    errorGroup is calculated. With this information the % of every errorGroup can be easily calculated.
    Also the % of every errorGroup in one errorCode is calculated.
    
    The results are saved in cp.percentageOfErrorCodeAndErrorGroup 
    and cp.percentageOfErrorGroup and cp.percentageOfErrorGroupInErrorCode.
    '''
    thisQualityControl = cp.qualityControlDatas[groupToQualityControlOfShift[0][1]]
    elementsOfProductType = [thisQualityControl]
    counterOfProductType = thisQualityControl.getNumber()
    for i in range(1, len(groupToQualityControlOfShift)):
        lastTuple = groupToQualityControlOfShift[i-1]
        thisTuple = groupToQualityControlOfShift[i]
        lastQualityControl = cp.qualityControlDatas[lastTuple[1]]
        thisQualityControl = cp.qualityControlDatas[thisTuple[1]]
        
        if lastTuple[0] == thisTuple[0] and lastQualityControl.getProductType() \
        == thisQualityControl.getProductType():
            elementsOfProductType.append(thisQualityControl)
            counterOfProductType += 0 if thisQualityControl.getNumber() < 0 else thisQualityControl.getNumber()
        else:
            groupQualityControlsPerErrorCode(lastTuple[0], shift, \
            lastQualityControl.getProductType(), elementsOfProductType, counterOfProductType)
                
            elementsOfProductType = [thisQualityControl]
            counterOfProductType = 0 if thisQualityControl.getNumber() < 0 else thisQualityControl.getNumber()
            
    if len(elementsOfProductType) > 0:
        lastTuple = groupToQualityControlOfShift[len(groupToQualityControlOfShift) - 1]
        
        groupQualityControlsPerErrorCode(lastTuple[0], shift, \
        cp.qualityControlDatas[lastTuple[1]].getProductType(), elementsOfProductType, counterOfProductType)
            
    
def groupQualityControlsPerErrorCode(group, shift, productType, elementsOfProductType, counterOfProductType):  
    '''
    Does the grouping of errorCodes. This is done seperately because the number per
    productType is needed.
    '''  
    elementsOfErrorCode = [elementsOfProductType[0]]
    
    for i in range(1, len(elementsOfProductType)):
        if elementsOfProductType[i-1].getErrorCode() == elementsOfProductType[i].getErrorCode():
            elementsOfErrorCode.append(elementsOfProductType[i])
        else:
            calculatePercentages(group, shift, productType, elementsOfErrorCode, counterOfProductType)
            elementsOfErrorCode = [elementsOfProductType[i]]
            
    if len(elementsOfErrorCode) > 0:
        calculatePercentages(group, shift, productType, elementsOfErrorCode, counterOfProductType)
        
    
def calculatePercentages(group, shift, productType, elementsOfErrorCode, counterOfProductType):
    '''
    Calculates the percentages for a group and a productType in a shift. Calculates percentages
    of errorGroups and errorCodes and also percentages of errorgroups in specific errorCodes.
    If the number of the whole productType is 0 or of the errorCode is 0 (Possible due to
    correction) the calculation is not finished.
    '''
    counterErrorGroupZero = 0
    counterErrorGroupOne = 0
    counterErrorGroupTwo = 0
    counterErrorGroupThree = 0
    
    for i in range(len(elementsOfErrorCode)):
        errorGroup = elementsOfErrorCode[i].getErrorGroup()
        if errorGroup == 0 :
            counterErrorGroupZero += 0 if elementsOfErrorCode[i].getNumber() < 0 else elementsOfErrorCode[i].getNumber()
        elif errorGroup == 1:
            counterErrorGroupOne += 0 if elementsOfErrorCode[i].getNumber() < 0 else elementsOfErrorCode[i].getNumber()
        elif errorGroup == 2:
            counterErrorGroupTwo += 0 if elementsOfErrorCode[i].getNumber() < 0 else elementsOfErrorCode[i].getNumber()
        elif errorGroup == 3:
            counterErrorGroupThree += 0 if elementsOfErrorCode[i].getNumber() < 0 else elementsOfErrorCode[i].getNumber()
        else:
            print(f'Unusual errorGroup: {errorGroup}')
            
    counterOfErrorCode = counterErrorGroupZero + counterErrorGroupOne \
    + counterErrorGroupTwo + counterErrorGroupThree
    
    if counterOfErrorCode != 0 and counterOfProductType != 0:        
        if counterErrorGroupZero != 0:
            cp.percentageOfErrorGroupInErrorCode.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 0, counterErrorGroupZero / counterOfErrorCode))
             
            cp.percentageOfErrorCodeAndErrorGroup.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 0, counterErrorGroupZero / counterOfProductType))
        
        if counterErrorGroupOne != 0:
            cp.percentageOfErrorGroupInErrorCode.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 1, counterErrorGroupOne / counterOfErrorCode))
        
            cp.percentageOfErrorCodeAndErrorGroup.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 1, counterErrorGroupOne / counterOfProductType))
        
        if counterErrorGroupTwo != 0:
            cp.percentageOfErrorGroupInErrorCode.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 2, counterErrorGroupTwo / counterOfErrorCode))
        
            cp.percentageOfErrorCodeAndErrorGroup.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 2, counterErrorGroupTwo / counterOfProductType))
        
        if counterErrorGroupThree != 0:
            cp.percentageOfErrorGroupInErrorCode.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 3, counterErrorGroupThree / counterOfErrorCode))
        
            cp.percentageOfErrorCodeAndErrorGroup.append((group, shift, productType, \
            elementsOfErrorCode[0].getErrorCode(), 3, counterErrorGroupThree / counterOfProductType))
            
            
def analyzeQualityControl(groupToQualityControlOfShift, shift):
    analyzeErrorCodes(groupToQualityControlOfShift, shift)
    analyzeErrorGroup(groupToQualityControlOfShift, shift)
        
        
def writeAnalysisToCSV(pathOfAnalysis):
    '''
    Writes out the whole analysis of groups to seperate .csv files 
    '''
    with open(pathOfAnalysis + 'averageMachineErrorTime.csv', mode='w') as averageMachineErrorTimeFile:
        writer = csv.writer(averageMachineErrorTimeFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        for i in range(len(cp.averageMachineErrorTime)):
            element = cp.averageMachineErrorTime[i]
            writer.writerow([element[0], element[1], element[2], str(element[3])])
            
    with open(pathOfAnalysis + 'percentageOfMachineErrorInTotalTime.csv', mode='w') \
    as percentageOfMachineErrorInTotalTimeFile:
        writer = csv.writer(percentageOfMachineErrorInTotalTimeFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        for i in range(len(cp.percentageOfMachineErrorInTotalTime)):
            element = cp.percentageOfMachineErrorInTotalTime[i]
            writer.writerow([element[0], element[1], element[2], element[3]])
            
    with open(pathOfAnalysis + 'percentageOfErrorCodeAndErrorGroup.csv', mode='w') \
    as percentageOfErrorCodeAndErrorGroupFile:
        writer = csv.writer(percentageOfErrorCodeAndErrorGroupFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        for i in range(len(cp.percentageOfErrorCodeAndErrorGroup)):
            element = cp.percentageOfErrorCodeAndErrorGroup[i]
            writer.writerow([element[0], element[1], element[2], element[3], element[4], element[5]])
    
    with open(pathOfAnalysis + 'percentageOfErrorGroupInErrorCode.csv', mode='w') \
    as percentageOfErrorGroupInErrorCodeFile:
        writer = csv.writer(percentageOfErrorGroupInErrorCodeFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        for i in range(len(cp.percentageOfErrorGroupInErrorCode)):
            element = cp.percentageOfErrorGroupInErrorCode[i]
            writer.writerow([element[0], element[1], element[2], element[3], element[4], element[5]])
            
    with open(pathOfAnalysis + 'percentageOfErrorGroup.csv', mode='w') \
    as percentageOfErrorGroupFile:
        writer = csv.writer(percentageOfErrorGroupFile, delimiter=',', \
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        for i in range(len(cp.percentageOfErrorGroup)):
            element = cp.percentageOfErrorGroup[i]
            writer.writerow([element[0], element[1], element[2], element[3], element[4]])
            
            
def analyzeGroups():    
    '''
    This analysis checks with what data the analysis is started. The hours at personal change are
    already filtered in cp.machineErrorDatas and cp.qualityControlDatas. On top of that, in every
    group all not wanted members are filtered out. This is done through the planGroup. So only
    members of the majority of the planGroup are saved in the group. Also groups with no
    majority are filtered out. After that all unusual group Lengths are also filtered.
    
    So this step calculates every important information that is needed, to understand with what
    data the analysis is done.
    '''    
    groupLengthsOfEvents = []
    for i in range(len(cp.groupToMachineError)):
        groupLengthsOfEvents.append(len(cp.groupToMachineError[i][0]))
        
    groupLengthAppearance = []
    for length in range(35):
        tmp = []
        for i in range(len(cp.groupToMachineError)):
            if len(cp.groupToMachineError[i][0]) == length:
                tmp.append(cp.machineErrorDatas[cp.groupToMachineError[i][1]].getOf())
        
        onePercentOfLength = len(tmp) / 100
        tmp = map(lambda x: datetime.date(x.year, x.month, 1), tmp)
        tmp = collections.Counter(tmp).most_common(None)
            
        counter = 0
        for u in range(len(tmp)):
            if tmp[u][1] > onePercentOfLength:
                counter += 1
        groupLengthAppearance.append(counter)
    
    print('--------------------------------------------------------------------------')
    print(f'MachineError: Group Lengths of Events Avg: {numpy.mean(groupLengthsOfEvents)}.')
    print(f'MachineError: Group Lengths of Events Var: {numpy.var(groupLengthsOfEvents)}.')
    print(f'MachineError: Group Lengths of Events Dict: {collections.Counter(groupLengthsOfEvents).most_common(None)}.')
    print(f'MachineError: Appearance of Group Lengths: {groupLengthAppearance}.')
    
    groupLengthsOfEvents = []
    for i in range(len(cp.groupToQualityControl)):
        groupLengthsOfEvents.append(len(cp.groupToQualityControl[i][0]))
        
    groupLengthAppearance = []
    for length in range(24):
        tmp = []
        for i in range(len(cp.groupToQualityControl)):
            if len(cp.groupToQualityControl[i][0]) == length:
                tmp.append(cp.qualityControlDatas[cp.groupToQualityControl[i][1]].getDatum())
        
        if tmp != []:
            if 10 <= length <= 14:
                tmp = map(lambda x: datetime.date(x.year, x.month, 1), tmp)
                groupLengthAppearance.append(collections.Counter(tmp))
        else:
            groupLengthAppearance.append('No Date')
    
    print('--------------------------------------------------------------------------')
    print(f'QualityControl: Group Length of Events Avg: {numpy.mean(groupLengthsOfEvents)}.')
    print(f'QualityControl: Group Length of Events Var: {numpy.var(groupLengthsOfEvents)}.')
    print(f'QualityControl: Group Lengths of Events Dict: {collections.Counter(groupLengthsOfEvents)}.')
    print(f'QualityControl: Appearance of Group Lengths: {groupLengthAppearance}.')
        
        
def calculateAnalyze(pathOfAnalysis):
    analyzeGroups()
    print('--------------------------------------------------------------------------')
    print(f'MachineError Data: Start Filtering at {datetime.datetime.now()}.')
    
    '''
    Filter due to groupLength
    '''
    #tmp = []
    #for i in range(len(cp.groupToMachineError)):
    #    if 23 <= len(cp.groupToMachineError[i][0]) <= 32:
    #        tmp.append(cp.groupToMachineError[i])
    #cp.groupToMachineError = tmp
    
    #tmp = []
    #for i in range(len(cp.groupToQualityControl)):
    #    if 17 <= len(cp.groupToQualityControl[i][0]) <= 23:
    #        tmp.append(cp.groupToQualityControl[i])
    #cp.groupToQualityControl = tmp

    
    '''
    Calculates the whole analysis of both data sets. The analysis is done for groups.
    '''
    print('--------------------------------------------------------------------------')
    print(f'MachineError Data: Start Analyze at {datetime.datetime.now()}.')
    
    earlyTwelve = []
    nightTwelve = []
    early = []
    late = []
    night = []
    divideGroupToMachineError(earlyTwelve, nightTwelve, early, late, night)
    
    analyzeMachineError(earlyTwelve, 'F12')
    analyzeMachineError(nightTwelve, 'N12')
    analyzeMachineError(early, 'F8')
    analyzeMachineError(late, 'L8')
    analyzeMachineError(night, 'N8')
    
    print('--------------------------------------------------------------------------')
    print(f'QualityControl Data: Start Analyze at {datetime.datetime.now()}.')
    
    earlyTwelve = []
    nightTwelve = []
    early = []
    late = []
    night = []
    divideGroupToQualityControl(earlyTwelve, nightTwelve, early, late, night)
    
    analyzeQualityControl(earlyTwelve, 'F12')
    analyzeQualityControl(nightTwelve, 'N12')
    analyzeQualityControl(early, 'F8')
    analyzeQualityControl(late, 'L8')
    analyzeQualityControl(night, 'N8')
    
    print('--------------------------------------------------------------------------')
    print(f'Analyzing done: Start Writing at {datetime.datetime.now()}.')
    
    writeAnalysisToCSV(pathOfAnalysis)
    
        
if __name__ == '__main__':
    pathOfAnalysis = '/Users/alexanderfuchs/Desktop/SS_21/Bachelorarbeit Material/Dataset/'
    
    cp.loadAllData( \
    pathOfAnalysis + 'Personen_exp1.csv', pathOfAnalysis + 'Daten_exp1.csv', \
    pathOfAnalysis + 'Personen_exp2.csv', pathOfAnalysis + 'Daten_exp2.csv')
    joinAndGroup(pathOfAnalysis)
    calculateAnalyze(pathOfAnalysis)