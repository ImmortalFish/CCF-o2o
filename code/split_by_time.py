# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 16:09:08 2017

@author: Administrator
"""

'''
根据时间窗口法划分数据集
'''

def SplitDataset(dataset):
    '''划分特征区间'''
    dataset_1 = dataset[(dataset['Date_received'] >= '20160115') & (dataset['Date_received'] <= '20160315')]
    dataset_2 = dataset[(dataset['Date_received'] >= '20160301') & (dataset['Date_received'] <= '20160430')]
    dataset_3 = dataset[(dataset['Date_received'] >= '20160416') & (dataset['Date_received'] <= '20160615')]
    
    return dataset_1, dataset_2, dataset_3


def SplitLabel(dataset, dataset_test):
    '''划分标签区间'''
    label_1 = dataset[(dataset['Date_received'] >= '20160331') & (dataset['Date_received'] <= '20160430')].drop('Date', axis = 1)
    label_2 = dataset[(dataset['Date_received'] >= '20160516') & (dataset['Date_received'] <= '20160615')].drop('Date', axis = 1)
    label_3 = dataset_test
    
    return label_1, label_2, label_3

def SplitKong(dataset):
    '''划分空闲区间'''
    kong_1 = dataset[(dataset['Date'] >= '20160316') & (dataset['Date'] <= '20160330')]
    kong_2 = dataset[(dataset['Date'] >= '20160501') & (dataset['Date'] <= '20160515')]
    kong_3 = dataset[(dataset['Date'] >= '20160616') & (dataset['Date'] <= '20160630')]
    
    return kong_1, kong_2, kong_3