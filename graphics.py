'''
Created on 28.05.2021

@author: alexanderfuchs
'''

import csv_parsing

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def drawAverageMachineErrorTime():
    
    dataframe = pd.DataFrame(map(lambda x: (str(x[0]), x[2], x[3].total_seconds()),
    csv_parsing.averageMachineErrorTime),
    columns=['group', 'errorCode', 'timeInSeconds'])

    # plot violin chart
    sns.pairplot(dataframe)

    # show the graph
    plt.show()
    
def drawPercentageOfMachineErrorInTotalTime():
    
    dataframe = pd.DataFrame(map(lambda x: (str(x[0]), x[2], x[3] * 100),
    csv_parsing.percentageOfMachineErrorInTotalTime),
    columns=['group', 'errorCode', 'percentage'])

    # plot violin chart
    sns.pairplot(dataframe)

    # show the graph
    plt.show()

def drawPercentageOfErrorGroup():
    
    dataframe = pd.DataFrame(map(lambda x: (str(x[0]), x[3], x[4] * 100),
    csv_parsing.percentageOfErrorGroup),
    columns=['group', 'errorGroup', 'percentage'])

    # plot violin chart
    sns.pairplot(dataframe, corner=True)

    # show the graph
    plt.show()
    
def drawGroupLengthDict(dictionary):
    #dataframe = pd.DataFrame.from_dict(dictionary, orient='index', columns=['numberOfErrors'])
    
    dataframe = pd.DataFrame(dictionary.items(), columns=['groupLength', 'numberOfErrors'])

    # Plot the responses for different events and regions
    sns.lineplot(x="groupLength", y="numberOfErrors", data=dataframe)
    
    # show the graph
    plt.show()
    