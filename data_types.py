'''
Created on 08.05.2021

@author: Alexander Fuchs

This module contains the classes for the representation of the entries of the
.csv tables. So one entry is an object of one of the classes. The objects are
programmed to be immutable. Only create new objects with the factory methods!!!
'''

from datetime import datetime

class PersonShift(object):
    '''
    Saves all data of one entry of the person tables.
    personHash is the hashed name of the Person.
    group defines the int of the working group in that shift.
    since defines a datetime when the person started to work for the company.
    of defines a datetime when the person entered the working area.
    to defines a datetime when the person leaved the working area.
    '''


    def __init__(self, personHash, group, since, of, to):
        '''
        Saves all data from one shift. All param should already have the right type.
        '''
        self.__personHash = personHash
        self.__group = group
        self.__since = since
        self.__of = of
        self.__to = to

    def getHash(self):
        return self.__personHash
    
    def getGroup(self):
        return self.__group
    
    def getSince(self):
        return self.__since
    
    def getOf(self):
        return self.__of
    
    def getTo(self):
        return self.__to
            
    @classmethod
    def createPersonShift(cls, personHash, group, since, of, to):
        try:
            ofDateTime = _parseDate(of)
            toDateTime = _parseDate(to)
            sinceDateTime = datetime.strptime(since, '%Y-%m-%d')
            
            if ofDateTime.year >= 2019 and toDateTime.year >= 2019:
                return cls(int(personHash), int(group), sinceDateTime, ofDateTime, toDateTime)
            else:
                return None
            
        except ValueError:
            print('Parsing was not succesful for Person: ' + personHash + ' with shift '\
            + of + ' until ' + to)
            return None
  
     
class MachineErrorData(object):
    '''
    Saves all data of one entry of the machine error table.
    source defines the machine where the error occured, saved as int.
    errorCode defines the error type, saved as int.
    of defines a datetime when the error occured.
    to defines a datetime when the error was repaired.
    '''


    def __init__(self, source, errorCode, of, to):
        '''
        Saves all data of one machine error.
        All params should already have the right type.
        '''
        self.__source = source
        self.__errorCode = errorCode
        self.__of = of
        self.__to = to

    def getSource(self):
        return self.__source
    
    def getErrorCode(self):
        return self.__errorCode
    
    def getOf(self):
        return self.__of
    
    def getTo(self):
        return self.__to
            
    @classmethod
    def createMachineErrorData(cls, source, errorCode, of, to):
        try:
            ofDateTime = _parseDate(of)
            toDateTime = _parseDate(to)
            
            if _checkDate(ofDateTime) and _checkDate(toDateTime):
                return cls(int(source), int(errorCode), ofDateTime, toDateTime)
            else:
                return None
            
        except ValueError:
            print('Parsing was not succesful for Machine Error Data: ' + of + ' until ' + to\
            + ' with error Code: ' + errorCode)
            return None   
   
        
class QualityControlData(object):
    '''
    Saves all data of one entry of the quality control table.
    datum describes the datetime when the quality was controlled.
    productType defines the type that was controlled, saved as int.
    number defines the number of products that was controled.
    errorCode defines the error, saved as int.
    errorGroup defines what has to happen with the products
    '''


    def __init__(self, datum, productType, number, errorCode, errorGroup):
        '''
        Saves all data of one quality control with one specific error code, datum and productType.
        All params should already have the right type.
        '''
        self.__datum = datum
        self.__productType = productType
        self.__number = number
        self.__errorCode = errorCode
        self.__errorGroup = errorGroup


    def getDatum(self):
        return self.__datum
    
    def getProductType(self):
        return self.__productType
    
    def getNumber(self):
        return self.__number
    
    def getErrorCode(self):
        return self.__errorCode
    
    def getErrorGroup(self):
        return self.__errorGroup
            
    @classmethod
    def createQalityControleData(cls, datum, productType, number, errorCode, errorGroup):
        try:
            dateTime = _parseDate(datum)
            
            if _checkDate(dateTime):
                return cls(dateTime, int(productType), int(number), int(errorCode), int(errorGroup))
            else:
                return None
            
        except ValueError:
            print('Parsing was not succesful for Quality Control Data: '\
            + datum + ' product Type: ' + productType + ' errorCode: ' + errorCode)
            return None   
        
        
def _parseDate(text):
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):   #these 2 dateformats were used in the initial data
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError()


def _checkDate(date):
    if date.year < 2019:                           #date does not make sense
        return False
    elif date.hour == 5 or date.hour == 6:         #5-6 end of nightshift, 6-7 am begin of early stage
        return False
    elif date.hour == 13 and date.weekday() != 0:  #end of early stage, not monday (8 hours)
        return False
    elif date.hour == 22 and date.weekday() != 0:  #begin of night shift, not monday (8 hours)
        return False
    elif date.hour == 14 and date.weekday() != 0:  #begin of late layer
        return False
    elif date.hour == 21 and date.weekday() != 0:  #end of late layer
        return False
    elif date.hour == 17 and date.weekday() == 0:  #monday end of 12 early stage
        return False
    elif date.hour == 18 and date.weekday() == 0:  #monday begin of 12 hour nightshift
        return False
    else:
        return True
        
        