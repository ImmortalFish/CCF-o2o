# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 20:54:45 2017

@author: Administrator
"""

'''
提取折扣特征
'''

import numpy as np

def ExtraceDiscountFeature(dataset):
    '''提取折扣的特征'''
    discount = dataset[['Discount']].drop_duplicates()
    
    #折扣的类型（满减为0，折扣为1）
    discount['discount_type'] = dataset['Discount_type']
    
    #折扣率
    discount['discount_rate'] = dataset['Discount_rate']
    
    #该折扣率的券被领了多少次
    discount['discount_get_count'] = dataset.groupby('Discount', sort = False)['Date_received'].count().values
    
    #该折扣率的券被领取并消费了多少次
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_discount = temp[temp['Discount'] != 'null'][['Discount']].drop_duplicates()
    temp_discount['discount_get_cost_count'] = temp.groupby('Discount', sort = False)['Date'].count().values
    discount = discount.merge(temp_discount, how = 'left', on = 'Discount')
    discount['discount_get_cost_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被核销了多少次
    temp = dataset[dataset['Label'] == 1]
    temp_discount = temp[temp['Discount'] != 'null'][['Discount']].drop_duplicates()
    temp_discount['discount_hexiao_count'] = temp.groupby('Discount', sort = False)['Date'].count().values
    discount = discount.merge(temp_discount, how = 'left', on = 'Discount')
    discount['discount_hexiao_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被消费了多少次 / 该折扣率的券被领了多少次
    discount['discount_get_cost_get_per'] = [x for x in discount['discount_get_cost_count'] / discount['discount_get_count']]
    discount['discount_get_cost_get_per'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被核销了多少次 / 该折扣率的券被领了多少次
    discount['discount_hexiao_get_per'] = [x for x in discount['discount_hexiao_count'] / discount['discount_get_count']]
    discount['discount_hexiao_get_per'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被核销了多少次 / 该折扣率的券被消费了多少次
    discount['discount_hexiao_get_cost_per'] = [x for x in discount['discount_hexiao_count'] / discount['discount_get_cost_count']]
    discount['discount_hexiao_get_cost_per'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被多少个不同的用户所领取
    temp_discount = dataset[['Discount']].drop_duplicates()
    temp_discount_1 = dataset[['Discount', 'User_id']].drop_duplicates()
    temp_discount_1['temp'] = dataset.groupby(['Discount', 'User_id'], sort = False)['User_id'].count().values
    temp_discount['discount_get_kind_user_count'] = temp_discount_1.groupby('Discount', sort = False)['User_id'].count().values
    discount = discount.merge(temp_discount, how = 'left', on = 'Discount')
    discount['discount_get_kind_user_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被多少个不同的用户所领取并消费
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_discount = temp[['Discount']].drop_duplicates()
    temp_discount_1 = temp[['Discount', 'User_id']].drop_duplicates()
    temp_discount_1['temp'] = temp.groupby(['Discount', 'User_id'], sort = False)['User_id'].count().values
    temp_discount['discount_get_cost_kind_user_count'] = temp_discount_1.groupby('Discount', sort = False)['User_id'].count().values
    discount = discount.merge(temp_discount, how = 'left', on = 'Discount')
    discount['discount_get_cost_kind_user_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被多少个不同的商户发放
    temp_discount = dataset[['Discount']].drop_duplicates()
    temp_discount_1 = dataset[['Discount', 'Merchant_id']].drop_duplicates()
    temp_discount_1['temp'] = dataset.groupby(['Discount', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_discount['discount_get_kind_merchant_count'] = temp_discount_1.groupby('Discount', sort = False)['Merchant_id'].count().values
    discount = discount.merge(temp_discount, how = 'left', on = 'Discount')
    discount['discount_get_kind_merchant_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被多少个不同的商户发放并被领取消费
    temp = dataset[(dataset['Date_received'] != 'null') & (dataset['Date'] != 'null')]
    temp_discount = temp[['Discount']].drop_duplicates()
    temp_discount_1 = temp[['Discount', 'Merchant_id']].drop_duplicates()
    temp_discount_1['temp'] = temp.groupby(['Discount', 'Merchant_id'], sort = False)['Merchant_id'].count().values
    temp_discount['discount_get_cost_kind_merchant_count'] = temp_discount_1.groupby('Discount', sort = False)['Merchant_id'].count().values
    discount = discount.merge(temp_discount, how = 'left', on = 'Discount')
    discount['discount_get_cost_kind_merchant_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被领券的日期是周末的次数
    temp = dataset[dataset['Date_received_is_weekend'] == 1]
    temp_user = temp[['Discount']].drop_duplicates()
    temp_user['discount_get_is_weekend_count'] = temp.groupby('Discount', sort = False)['Date_received_is_weekend'].count().values
    discount = discount.merge(temp_user, how = 'left', on = 'Discount')
    discount['discount_get_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被领券并消费的日期是周末的次数
    temp = dataset[(dataset['Date_is_weekend'] == 1) & (dataset['Date_received'] != 'null')]
    temp_user = temp[['Discount']].drop_duplicates()
    temp_user['discount_get_cost_is_weekend_count'] = temp.groupby('Discount', sort = False)['Date_is_weekend'].count().values
    discount = discount.merge(temp_user, how = 'left', on = 'Discount')
    discount['discount_get_cost_is_weekend_count'].replace(np.nan, 0, inplace = True)
    
    #该折扣率的券被领券的日期是周末的次数 / 该折扣率的券被领了多少次
    discount['discount_get_is_weekend_get_per'] = [x for x in discount['discount_get_is_weekend_count'] / discount['discount_get_count']]
    
    #该折扣率的券被领券并消费的日期是周末的次数 / 该折扣率的券被领取并消费了多少次
    discount['discount_get_cost_is_weekend_cost_per'] = [x for x in discount['discount_get_cost_is_weekend_count'] / discount['discount_get_cost_count']]
    
    return discount