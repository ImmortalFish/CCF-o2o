# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 13:18:44 2017

@author: Administrator
"""

'''
对数据集进行预处理
'''

import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

def DataHandle(dataset):
    '''对数据集进行预处理'''
    temp = dataset.copy()
    temp.rename(columns = {'Discount_rate': 'Discount'}, inplace = True)
    
    #新增两列，把两个时间都转为datetime
    if 'Date' in temp.columns:
        temp['Date_datetime'] = [pd.to_datetime(x, errors = 'coerce') for x in temp['Date']]
    temp['Date_received_datetime'] = [pd.to_datetime(x, errors = 'coerce') for x in temp['Date_received']]
    
    #计算时间间隔
    if 'Date_datetime' in temp.columns:
        temp['Day_gap'] = [x.days for x in temp['Date_datetime'] - temp['Date_received_datetime']]
        temp['Day_gap'].replace(np.nan, -1, inplace = True)
    
    #Date或者Date_received是否为周末
    if 'Date_datetime' in temp.columns:
        temp['Date_is_weekend'] = [1 if x.isoweekday() in [6, 7] else 0 if x.isoweekday() in [1, 2, 3, 4, 5] else -1 for x in temp['Date_datetime']]
    temp['Date_received_is_weekend'] = [1 if x.isoweekday() in [6, 7] else 0 if x.isoweekday() in [1, 2, 3, 4, 5] else -1 for x in temp['Date_received_datetime']]
    
    #券的类型(满减为0，折扣为1)
    temp['Discount_type'] = [0 if ':' in x else 1 if '.' in x else -1 for x in temp['Discount']]
    
    #券的折扣率
    temp['Discount_rate'] = [1 - int(x.split(':')[1]) / int(x.split(':')[0]) if ':' in x else float(x) if '.' in x else -1 for x in temp['Discount']]
    
    #最低消费
    temp['Min_cost'] = [int(x.split(':')[0]) if ':' in x else 0 if '.' in x else -1 for x in temp['Discount']]
    
    #距离中的null转为-1
    temp['Distance'].replace('null', -1, inplace = True)
    
    #把某些列转为要的类型
    if 'Date_datetime' in temp.columns:
        temp['Date'] = temp['Date'].astype('str')
    temp['Date_received'] = temp['Date_received'].astype('str')
    temp['Coupon_id'] = temp['Coupon_id'].astype('str')
    temp['Merchant_id'] = temp['Merchant_id'].astype('int')
    temp['User_id'] = temp['User_id'].astype('int')
    temp['Distance'] = temp['Distance'].astype('int')
    
    return temp

# =============================================================================
# if __name__ == '__main__':
#     
#     offline_train = pd.read_csv('../data_original/ccf_offline_stage1_train.csv')
#     print('开始训练集预处理!')
#     offline_train_handled = DataHandle(offline_train)
#     print('训练集预处理结束，保存为csv文件!')
#     offline_train_handled.to_csv('../data_handle/offline_train_handled.csv', index = False)
#     print('训练集保存结束!')
#     
#     offline_test = pd.read_csv('../data_original/ccf_offline_stage1_test_revised.csv')
#     offline_test['Date_received'] = offline_test['Date_received'].astype('str')
#     print('开始测试集预处理!')
#     offline_test_handled = DataHandle(offline_test)
#     print('测试集预处理结束，保存为csv文件!')
#     offline_test_handled.to_csv('../data_handle/offline_test_handled.csv', index = False)
#     print('测试集保存结束!')
# =============================================================================
