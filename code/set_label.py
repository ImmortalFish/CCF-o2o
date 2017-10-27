# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 15:11:49 2017

@author: Administrator
"""

'''
打标
'''

import pandas as pd
pd.options.mode.chained_assignment = None

def SetLabel(dataset):
    '''打标'''
    temp = dataset.copy()
    
    #领券未消费的标为0
    offline_train_part_1 = temp[(temp['Date_received'] != 'null') & (temp['Date'] == 'null')]
    offline_train_part_1['Label'] = 0
    
    #消费时间如果在15天之内，标记为1，否则标记为0
    offline_train_part_2 = temp[(temp['Date'] != 'null') & (temp['Date_received'] != 'null')]
    offline_train_part_2['Label'] = [1 if x <= 15 else 0 for x in offline_train_part_2['Day_gap']]
    
    #将两部分合并在一起，保存为csv文件
    offline_train_with_label = pd.concat([offline_train_part_1, offline_train_part_2], axis = 0)
    offline_train_with_label = offline_train_with_label.sample(frac = 1)
    
    return offline_train_with_label