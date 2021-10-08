'''
Created on 12.05.2021

@author: Alexander Fuchs

This module has all the implementation, that was used for the analysis.
So the join, the grouping and the calculation of the performance metrics
is included in this module. 

Also most of the data preparation is done in this module. The majority
filtering of the groups is done in this module.
'''

import collections

import csv
import datetime
import csv_parsing as cp
    
def joinAndGroupMachineError(pathOfAnalysis):
    '''
    Joins the 2 lists of the machineErrorDataset. Do not change the Sorting
    of the entries of the 2 lists, that was done at loading the tables!!! The
    whole join only works with the Sorting, that was done in csv_parsing.
    
    machineErrorDatas.sort(key=lambda x: x.getTo(), reverse=True)
    machineErrorPersonShifts.sort(key=lambda x: x.getTo(), reverse=True)
    '''
    machineErrorToPerson = []
    
    print('--------------------------------------------------------------------------')
    print(f'machineErrorToPerson: Join started at {datetime.datetime.now()}!')
    
    '''
    The machineErrorDatas and machineErrorPersonShifts must be sorted by the date of getTo() (DESC).
    Pairs every machine error to every shift of a worker in which the error occurred.
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
    does not work. machineErrorToPerson is already sorted due to the algorithm before.
    The algorithm maps all the persons of one machine error to the machine error as a
    group.
    
    the algorithm also includes one step of the  data preparation.
    '''
    with open(pathOfAnalysis + 'groupToMachineError.csv', mode='w') as groupToMachineErrorFile:
        writer = csv.writer(groupToMachineErrorFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        groupList = [cp.machineErrorPersonShifts[machineErrorToPerson[0][1]]]
        for i in range(1, len(machineErrorToPerson)):
            if machineErrorToPerson[i-1][0] == machineErrorToPerson[i][0]:
                groupList.append(cp.machineErrorPersonShifts[machineErrorToPerson[i][1]])
            else:
                '''
                this data preparation that was done due to the oversized groups.
                Many groups include workers that are not working at the time of the
                machine error. The reason for that is that they are preparing for their
                work or the are leaving the work during the machine error.
                '''
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
    '''
    Joins the 2 lists of the qualityControlDataset. Do not change the Sorting
    of the entries of the 2 lists, that was done at loading the tables!!! The
    whole join only works with the Sorting, that was done in csv_parsing.
    
    qualityControlDatas.sort(key=lambda x: x.getDatum(), reverse=True)
    qualityControlPersonShifts.sort(key=lambda x: x.getTo(), reverse=True)
    '''
    qualityControlToPerson = []
    
    print('--------------------------------------------------------------------------')
    print(f'qualityControlToPerson: Join started at {datetime.datetime.now()}.')
    
    '''
    The qualityControlDatas and qualityControlPersonShifts must be sorted by the date of getTo()
    and date (DESC). Pairs every quality Control to every shift of a worker who worked at that time.
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
    does not work. qualityControlToPerson is already sorted due to the algorithm before.
    The algorithm maps all the persons of one quality control to the quality control as a group.
    
    the algorithm also includes one step of the data preparation.
    '''
    with open(pathOfAnalysis + 'groupToQualityControl.csv', mode='w') as groupToQualityControlFile:
        writer = csv.writer(groupToQualityControlFile, delimiter=',',\
        quotechar='"', quoting=csv.QUOTE_ALL)
        
        groupList = [cp.qualityControlPersonShifts[qualityControlToPerson[0][1]]]
        for i in range(1, len(qualityControlToPerson)):
            if qualityControlToPerson[i-1][0] == qualityControlToPerson[i][0]:
                groupList.append(cp.qualityControlPersonShifts[qualityControlToPerson[i][1]])
            else:
                '''
                this data preparation that was done due to the oversized groups.
                Many groups include workers that are not working at the time of the
                quality control. The reason for that is that they are preparing for their
                work or the are leaving the work during the quality control.
                '''
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
    Joins and groups the machine Error dataset and quality Control dataset. Lasts many hours!!!
    Because it needs so much time it first tries to load the joined data from files which already
    contain the joined Data. So the calculation has only to be done once. If it cannot be loaded
    it is calculated instead.
    '''
    if not cp.loadGroupData(pathOfAnalysis):
        joinAndGroupMachineError(pathOfAnalysis)
        joinAndGroupQualityControl(pathOfAnalysis)
        if not cp.loadGroupData(pathOfAnalysis):
            raise RuntimeError('Illegal State: Bug in loading or joinAndGroup!')
     

def divideGroupToMachineError(earlyTwelve, nightTwelve, early, late, night):
    '''
    Divides the groupToMachineErrors by the 5 shifts. This is necessary because it´s not
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
    groupToMachineError has to be sorted by group first and errorCode second!!! Do not change
    the Sorting in csv_parsing. For every group and errorCode the average Time of the machineErrors
    with that errorCode and that group are calculated. Also For every group and errorCode the % of
    the sum of the intervals of all machineErrors with that errorCode and that group to the total working time
    of the group is calculated. 
    
    The total working time of groups is calculated approximative, so the results of 
    cp.percentageOfMachineErrorInTotalTime are inaccurate and have to be viewed with caution!!!
    More details in calculateTimeOfAllGroups(groupToTime, groupToMachineErrorOfShift).
    
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
            
            cp.averageMachineErrorTime.append((lastTuple[0], shift, \
            cp.machineErrorDatas[lastTuple[1]].getErrorCode(), avgTime))
            
            #because of the inaccurate calculation, timeOfGroup can be 0!
            if timeOfGroupSaved != datetime.timedelta(0):
                percentage = sum(timedeltas, datetime.timedelta(0)) / timeOfGroupSaved
            
                cp.percentageOfMachineErrorInTotalTime.append((lastTuple[0],shift, \
                cp.machineErrorDatas[lastTuple[1]].getErrorCode(), percentage))
            
            elementToList = cp.machineErrorDatas[thisTuple[1]]
            timedeltas = [elementToList.getTo() - elementToList.getOf()]
            
    if len(timedeltas) > 0:
        lastTuple = groupToMachineErrorOfShift[len(groupToMachineErrorOfShift) - 1]
        avgTime = sum(timedeltas, datetime.timedelta(0)) / len(timedeltas)
        percentage = sum(timedeltas, datetime.timedelta(0)) / groupToTime[len(groupToTime) - 1][1]
        
        cp.averageMachineErrorTime.append((lastTuple[0], shift, \
        cp.machineErrorDatas[lastTuple[1]].getErrorCode(), avgTime))
        
        cp.percentageOfMachineErrorInTotalTime.append((lastTuple[0], shift, \
        cp.machineErrorDatas[lastTuple[1]].getErrorCode(), percentage))
                    
                    
def calculateTimeOfAllGroups(groupToTime, groupToMachineErrorOfShift):
    '''
    Calculates the total working time of all groups. The algorithm does only
    provide approximated times, not the real ones. There is no assurance
    how accurate these times are. So the results have to be viewed with caution!!!
    '''
    group = groupToMachineErrorOfShift[0][0]
    for i in range(1, len(groupToMachineErrorOfShift)):
        if group != groupToMachineErrorOfShift[i][0]:
            groupToTime.append((group, timeOfGroup(group)))
            group = groupToMachineErrorOfShift[i][0]
            
    groupToTime.append((group, timeOfGroup(group)))
      
    for i in range(len(groupToTime)):
        for u in range(len(groupToTime)):
            if i != u and set(groupToTime[i][0]).issubset(set(groupToTime[u][0])):
                groupToTime[i] = (groupToTime[i][0], groupToTime[i][1] - groupToTime[u][1])
    
    
def timeOfGroup(group):
    '''
    calculates the working time, at which the group was at least present. That means
    that there are also times included in which more workers than the group were present.
    in calculateTimeOfAllGroups(groupToTime, groupToMachineErrorOfShift) the time of
    bigger groups is subtracted, so the times correspond to the real ones.
    
    
    The time is only calculated approximately. This is the reason why the results of 
    calculateTimeOfAllGroups(groupToTime, groupToMachineErrorOfShift) are also
    approximately and inaccurate.
    '''
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

     
def divideGroupToQualityControl(earlyTwelve, nightTwelve, early, late, night):
    '''
    Divides the groupToQualityControls by the 5 shifts. This is necessary because it´s not
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
    This algorithm does not need a sorting of the lists because the sorting is done
    here individually. For every group and every productType the % of an errorGroup
    is calculated.
    
    The result is saved in cp.percentageOfErrorGroup.
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
    Calculates the % of every Error Group of one product Type of one group. The quality controls of the
    productType and the group must already be provided through elementsOfProductType. Also a counter
    for all quality controls of the productType and group must already be provided through counterOfProductType.
    
    Some quality controls contain a negative number. In that case 0 is added.
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
    errorGroup fourth!!! Do not change the sorting in csv_parsing. For every group and productType is
    the % of an combination of errorCode and errorGroup calculated. Also for every group and productType
    and errorCode the % of an errorGroup is calculated.
    
    The results are saved in cp.percentageOfErrorCodeAndErrorGroup and cp.percentageOfErrorGroupInErrorCode.
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
    Does the grouping of errorCodes. This had to be done separately. elementsOfProductType provides all
    quality controls of one group and productType. Also a counter for all quality controls of the
    productType and group must already be provided through counterOfProductType.
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
    Calculates for a group and a productType and a errorCode the % of all errorGroups. Calculates also
    for a group and a productType the % of one combination of errorCode and errorGroup. The quality controls
    of the group and the productType and the errorCode must already be given through elementsOfErrorCode. Also
    the counter for the productType and the group must be provided through counterOfProductType.
    
    Some quality controls contain a negative number. In that case 0 is added.
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
    '''
    Starts the 2 analysis methods of the quality control dataset.
    '''
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
    
        
def calculateAnalyze(pathOfAnalysis):
    '''
    Calculates the whole analysis of both data sets.
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
    '''
    pathOfAnalysis is the folder in which the datasets (all 4 tables, 2 per Dataset) are saved.
    Do not change the names of the tables, so the program does recognize them.
    '''
    pathOfAnalysis = '/Users/alexanderfuchs/Desktop/SS_21/Bachelorarbeit Material/Dataset/'
    
    cp.loadAllData( \
    pathOfAnalysis + 'Personen_exp1.csv', pathOfAnalysis + 'Daten_exp1.csv', \
    pathOfAnalysis + 'Personen_exp2.csv', pathOfAnalysis + 'Daten_exp2.csv')
    joinAndGroup(pathOfAnalysis)
    calculateAnalyze(pathOfAnalysis)