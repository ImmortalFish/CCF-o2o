# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 14:02:02 2017

@author: Administrator
"""

'''
生成训练集、验证集、测试集
'''

import numpy as np

def GenTrain(user_feature_2, merchant_feature_2, user_merchant_2, discount_feature_2, user_discount_2, label_feature_2, kong_user_feature_2, kong_merchant_feature_2, kong_user_merchant_feature_2):
    '''生成训练集'''
    train = label_feature_2.merge(user_feature_2, how = 'left', on = 'User_id')
    train = train.merge(merchant_feature_2, how = 'left', on = 'Merchant_id')
    train = train.merge(discount_feature_2, how = 'left', on = 'Discount')
    train = train.merge(user_merchant_2, how = 'left', on = ['User_id', 'Merchant_id'])
    train = train.merge(user_discount_2, how = 'left', on = ['User_id', 'Discount'])
    train = train.merge(kong_user_feature_2, how = 'left', on = 'User_id')
    train = train.merge(kong_merchant_feature_2, how = 'left', on = 'Merchant_id')
    train = train.merge(kong_user_merchant_feature_2, how = 'left', on = ['User_id', 'Merchant_id'])
    train.drop(['User_id', 'Merchant_id', 'Discount', 'Coupon_id',  'Date_datetime', 'Date_is_weekend', 'Day_gap', 'Date_received'], axis = 1, inplace = True)
    
    #缺失值填充
    for col in train.columns:
        if train[col].isnull().any():
            if 'count' in col:
                train[col].replace(np.nan, 0, inplace = True)
            elif 'per' in col:
                train[col].replace(np.nan, 0, inplace = True)
            else:
                train[col].replace(np.nan, -1, inplace = True)
                
    return train
    
def GenValidation(user_feature_1, merchant_feature_1, user_merchant_1, discount_feature_1, user_discount_1, label_feature_1, kong_user_feature_1, kong_merchant_feature_1, kong_user_merchant_feature_1):
    '''生成验证集'''
    validation = label_feature_1.merge(user_feature_1, how = 'left', on = 'User_id')
    validation = validation.merge(merchant_feature_1, how = 'left', on = 'Merchant_id')
    validation = validation.merge(discount_feature_1, how = 'left', on = 'Discount')
    validation = validation.merge(user_merchant_1, how = 'left', on = ['User_id', 'Merchant_id'])
    validation = validation.merge(user_discount_1, how = 'left', on = ['User_id', 'Discount'])
    validation = validation.merge(kong_user_feature_1, how = 'left', on = 'User_id')
    validation = validation.merge(kong_merchant_feature_1, how = 'left', on = 'Merchant_id')
    validation = validation.merge(kong_user_merchant_feature_1, how = 'left', on = ['User_id', 'Merchant_id'])
    validation.drop(['User_id', 'Merchant_id', 'Coupon_id', 'Discount', 'Date_datetime', 'Date_is_weekend', 'Day_gap', 'Date_received'], axis = 1, inplace = True)
    
    #缺失值填充
    for col in validation.columns:
        if validation[col].isnull().any():
            if 'count' in col:
                validation[col].replace(np.nan, 0, inplace = True)
            elif 'per' in col:
                validation[col].replace(np.nan, 0, inplace = True)
            else:
                validation[col].replace(np.nan, -1, inplace = True)
    return validation
                
def GenTest(user_feature_3, merchant_feature_3, user_merchant_3, discount_feature_3, user_discount_3, label_feature_3, kong_user_feature_3, kong_merchant_feature_3, kong_user_merchant_feature_3):
    '''生成测试集'''  
    test = label_feature_3.merge(user_feature_3, how = 'left', on = 'User_id')
    test = test.merge(merchant_feature_3, how = 'left', on = 'Merchant_id')
    test = test.merge(discount_feature_3, how = 'left', on = 'Discount')
    test = test.merge(user_merchant_3, how = 'left', on = ['User_id', 'Merchant_id'])
    test = test.merge(user_discount_3, how = 'left', on = ['User_id', 'Discount'])
    test = test.merge(kong_user_feature_3, how = 'left', on = 'User_id')
    test = test.merge(kong_merchant_feature_3, how = 'left', on = 'Merchant_id')
    test = test.merge(kong_user_merchant_feature_3, how = 'left', on = ['User_id', 'Merchant_id'])
    test.drop(['Merchant_id', 'Discount'], axis = 1, inplace = True)
    
    #缺失值填充
    for col in test.columns:
        if test[col].isnull().any():
            if 'count' in col:
                test[col].replace(np.nan, 0, inplace = True)
            elif 'per' in col:
                test[col].replace(np.nan, 0, inplace = True)
            else:
                test[col].replace(np.nan, -1, inplace = True)
                
    return test

